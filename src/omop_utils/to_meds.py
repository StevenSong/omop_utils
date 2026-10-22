"""Convert an omop_utils download to MEDS 0.3.3, one person ID range at a time.

This is a port of ``meds_etl_omop`` (``meds_etl.omop`` with the polars sort backend) that keeps its per-table event
mapping but changes how the work is split, so peak memory is bounded by the size of one output shard:

- Output shards are contiguous ``person_id`` ranges instead of hash buckets. Each shard reads only the source files
  (and, via Parquet statistics, row groups) whose person IDs overlap its range, maps them to events, sorts them and
  writes ``data/data_<shard>.parquet`` directly. There is no MEDS Unsorted intermediate and no second shuffle.
- The concept ID -> code lookup is a pair of polars Series loaded once per worker, rather than pickled Python dicts.
- Shards are written atomically and skipped if they already exist, so an interrupted run can be resumed.

The input is expected to be laid out as written by :mod:`omop_utils.download`: patient tables as directories of
Parquet chunks, non-patient tables (``concept``, ``concept_relationship``, ``cdm_source``) as single Parquet files.
"""

import argparse
import importlib.metadata
import json
import logging
import multiprocessing
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Any

import meds
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from .logs import LOG_FORMAT, log_to_file

log = logging.getLogger(__name__)

# OMOP constants, as in meds_etl.omop
CUSTOM_CONCEPT_ID_START = 2_000_000_000
DEFAULT_VISIT_CONCEPT_ID = 8
DEFAULT_NOTE_CONCEPT_ID = 46235038
DEFAULT_VISIT_DETAIL_CONCEPT_ID = 4203722

OMOP_TIME_FORMATS = ("%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d")

# MEDS columns that are not "properties"; everything else is sorted by name after these
CORE_COLUMNS = ("subject_id", "time", "code", "numeric_value")

_PLAN = "omop_utils_plan.json"


def omop_tables(omop_version: str = "5.4") -> dict[str, list[dict[str, Any]]]:
    """Per-table mapping details, as in ``meds_etl.omop.main``.

    Keys are the table names used for column prefixes (``visit`` -> ``visit_start_datetime``).
    ``file_suffix`` is appended to get the table's name on disk (``visit`` -> ``visit_occurrence``).
    """
    if omop_version not in ("5.3", "5.4"):
        raise ValueError(f"OMOP version {omop_version} not supported")
    return {
        "person": [
            {"force_concept_code": meds.birth_code},
            {"concept_id_field": "gender_concept_id"},
            {"concept_id_field": "race_concept_id"},
            {"concept_id_field": "ethnicity_concept_id"},
        ],
        "drug_exposure": [{"concept_id_field": "drug_concept_id"}],
        "visit": [
            {
                "fallback_concept_id": DEFAULT_VISIT_CONCEPT_ID,
                "file_suffix": "occurrence",
            },
            {
                "concept_id_field": "discharged_to_concept_id"
                if omop_version == "5.4"
                else "discharge_to_concept_id",
                "time_field_options": ["visit_end_datetime", "visit_end_date"],
                "file_suffix": "occurrence",
            },
        ],
        "condition": [{"file_suffix": "occurrence"}],
        "death": [{"force_concept_code": meds.death_code}],
        "procedure": [{"file_suffix": "occurrence"}],
        "device_exposure": [{"concept_id_field": "device_concept_id"}],
        "measurement": [
            {
                "string_value_field": "value_source_value",
                "numeric_value_field": "value_as_number",
                "concept_id_value_field": "value_as_concept_id",
            }
        ],
        "observation": [
            {
                "string_value_field": "value_as_string",
                "numeric_value_field": "value_as_number",
                "concept_id_value_field": "value_as_concept_id",
            }
        ],
        "note": [
            {
                "fallback_concept_id": DEFAULT_NOTE_CONCEPT_ID,
                "concept_id_field": "note_class_concept_id",
                "string_value_field": "note_text",
            }
        ],
        "visit_detail": [{"fallback_concept_id": DEFAULT_VISIT_DETAIL_CONCEPT_ID}],
    }


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Source files                                                                  #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass(frozen=True)
class SourceFile:
    path: Path
    # Inclusive person ID range from the Parquet statistics, or None if unknown (always read)
    person_ids: tuple[int, int] | None


@dataclass(frozen=True)
class SourceTable:
    name: str  # name used for column prefixes and the `table` property, e.g. "visit"
    details: list[dict[str, Any]]
    person_column: str  # actual (case-preserved) name of the person ID column
    files: list[SourceFile]

    def files_for(self, lo: int | None, hi: int | None) -> list[Path]:
        """Files that may contain person IDs in ``[lo, hi)`` (``None`` is unbounded)."""
        out = []
        for f in self.files:
            if f.person_ids is not None:
                first, last = f.person_ids
                if (hi is not None and first >= hi) or (lo is not None and last < lo):
                    continue
            out.append(f.path)
        return out


def table_files(src: PathLike, table: str) -> list[Path]:
    """Parquet files for ``table``: ``<src>/<table>/*.parquet`` or ``<src>/<table>.parquet``.

    In-progress downloads (``*.parquet.tmp``) are ignored.
    A table directory without the ``_COMPLETE`` marker is used, with a warning.
    """
    src = Path(src)
    directory = src / table
    if directory.is_dir():
        files = sorted(directory.glob("*.parquet"))
        if not files and any(directory.glob("*.csv*")):
            raise ValueError(
                f"{table} is stored as CSV; only Parquet input is supported"
            )
        if files and not (directory / "_COMPLETE").exists():
            log.warning(
                "%s has no _COMPLETE marker; the download may be incomplete", directory
            )
        return files
    single = src / f"{table}.parquet"
    if single.exists():
        return [single]
    if any(src.glob(f"{table}.csv*")):
        raise ValueError(f"{table} is stored as CSV; only Parquet input is supported")
    return []


def _person_column(path: Path) -> str:
    names = pq.read_schema(path).names
    matches = [n for n in names if n.lower() == "person_id"]
    if len(matches) != 1:
        raise ValueError(f"Expected one person_id column in {path}, found {names}")
    return matches[0]


def _person_id_range(path: Path, column: str) -> tuple[int, int] | None:
    """Inclusive (min, max) of ``column`` over the file, from row group statistics."""
    metadata = pq.read_metadata(path)
    if metadata.num_rows == 0:
        return None
    index = metadata.schema.to_arrow_schema().get_field_index(column)
    lo = hi = None
    for i in range(metadata.num_row_groups):
        stats = metadata.row_group(i).column(index).statistics
        if stats is None or not stats.has_min_max:
            return None
        if not isinstance(stats.min, int):
            return None
        lo = stats.min if lo is None else min(lo, stats.min)
        hi = stats.max if hi is None else max(hi, stats.max)
    return lo, hi


def find_tables(src: PathLike, omop_version: str = "5.4") -> list[SourceTable]:
    """Locate the event tables under ``src`` and index each file's person ID range."""
    tables = []
    for name, details in omop_tables(omop_version).items():
        on_disk = name + (
            "_" + details[0]["file_suffix"] if details[0].get("file_suffix") else ""
        )
        paths = table_files(src, on_disk)
        if not paths:
            log.info("%s: not found, skipping", on_disk)
            continue
        column = _person_column(paths[0])
        files = [SourceFile(p, _person_id_range(p, column)) for p in paths]
        unindexed = sum(f.person_ids is None for f in files)
        log.info(
            "%s: %d files, %s rows%s",
            on_disk,
            len(files),
            f"{sum(pq.read_metadata(p).num_rows for p in paths):,}",
            f" ({unindexed} without person_id statistics, read for every shard)"
            if unindexed
            else "",
        )
        tables.append(SourceTable(name, details, column, files))
    return tables


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Event mapping (ported from meds_etl.omop.write_event_data)                    #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


@dataclass(frozen=True)
class ConceptMap:
    """concept_id -> ``<vocabulary_id>/<concept_code>``, applied with ``replace_strict`` like meds_etl."""

    ids: pl.Series
    codes: pl.Series

    def replace(self, expr: pl.Expr) -> pl.Expr:
        return expr.replace_strict(
            self.ids, self.codes, return_dtype=pl.Utf8(), default=None
        )


def parse_time(time: pl.Expr, time_formats: Iterable[str]) -> pl.Expr:
    return pl.coalesce(
        [
            time.str.to_datetime(time_format, strict=False, time_unit="us")
            for time_format in time_formats
        ]
    )


def convert_generic_value_to_specific(
    generic_value: pl.Expr,
) -> tuple[pl.Expr, pl.Expr]:
    generic_value = generic_value.str.strip_chars()
    numeric_value = generic_value.cast(pl.Float32, strict=False)
    text_value = (
        pl.when(numeric_value.is_null())
        .then(generic_value)
        .otherwise(pl.lit(None, dtype=pl.Utf8()))
    )
    return numeric_value, text_value


def cast_to_datetime(
    schema: pl.Schema, column: str, move_to_end_of_day: bool = False
) -> pl.Expr:
    if column not in schema.names():
        return pl.lit(None, dtype=pl.Datetime(time_unit="us"))
    if schema[column] == pl.Utf8():
        if not move_to_end_of_day:
            return parse_time(pl.col(column), OMOP_TIME_FORMATS)
        # Try to cast time to a datetime but if only the date is available, then use
        # that date with a timestamp of 23:59:59
        time = pl.col(column)
        return pl.coalesce(
            time.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False, time_unit="us"),
            time.str.to_datetime("%Y-%m-%d", strict=False, time_unit="us")
            .dt.offset_by("1d")
            .dt.offset_by("-1s"),
        )
    elif schema[column] == pl.Date():
        time = pl.col(column).cast(pl.Datetime(time_unit="us"))
        if move_to_end_of_day:
            time = time.dt.offset_by("1d").dt.offset_by("-1s")
        return time
    elif isinstance(schema[column], pl.Datetime):
        return pl.col(column).cast(pl.Datetime(time_unit="us"))
    else:
        raise RuntimeError(
            f"Unknown how to handle date type {schema[column]} for {column}"
        )


def table_events(
    batch: pl.LazyFrame,
    *,  # enforce kwargs
    table_name: str,
    table_details: Mapping[str, Any],
    concepts: ConceptMap,
) -> pl.LazyFrame:
    """Map one OMOP table to MEDS events for one entry of its table details.

    The expressions are those of ``meds_etl.omop.write_event_data``; only the concept lookup and the output
    (a LazyFrame instead of a file) differ.
    """
    batch = batch.rename({c: c.lower() for c in batch.collect_schema().names()})
    schema = batch.collect_schema()

    subject_id = pl.col("person_id").cast(pl.Int64)

    # time
    if table_name == "person":
        # Take the `birth_datetime` if its available, otherwise
        # construct it from `year_of_birth`, `month_of_birth`, `day_of_birth`
        time = pl.coalesce(
            cast_to_datetime(schema, "birth_datetime"),
            pl.datetime(
                pl.col("year_of_birth"),
                pl.coalesce(pl.col("month_of_birth"), pl.lit(1)),
                pl.coalesce(pl.col("day_of_birth"), pl.lit(1)),
                time_unit="us",
            ),
        )
    else:
        # Prefer user defined time fields, then `_start_datetime`, `_start_date`, `_datetime`, `_date`
        options = table_details.get("time_field_options", []) + [
            table_name + "_start_datetime",
            table_name + "_start_date",
            table_name + "_datetime",
            table_name + "_date",
        ]
        options = [
            cast_to_datetime(schema, option, move_to_end_of_day=True)
            for option in options
            if option in schema.names()
        ]
        assert len(options) > 0, f"Could not find the time column {schema.names()}"
        time = pl.coalesce(options)

    # code
    if table_details.get("force_concept_code"):
        source_concept_id = pl.lit(0, dtype=pl.Int64)
        code = pl.lit(table_details.get("force_concept_code"), dtype=pl.Utf8)
    else:
        # Prefer the source concept ID, then the concept ID, then `fallback_concept_id`
        concept_id_field = table_details.get(
            "concept_id_field", table_name + "_concept_id"
        )
        concept_id = pl.col(concept_id_field).cast(pl.Int64)
        if (
            concept_id_field.replace("_concept_id", "_source_concept_id")
            in schema.names()
        ):
            source_concept_id = pl.col(
                concept_id_field.replace("_concept_id", "_source_concept_id")
            ).cast(pl.Int64)
        else:
            source_concept_id = pl.lit(0, dtype=pl.Int64)

        fallback_concept_id = pl.lit(
            table_details.get("fallback_concept_id", None), dtype=pl.Int64
        )

        concept_id = (
            pl.when(source_concept_id != 0)
            .then(source_concept_id)
            .when(concept_id != 0)
            .then(concept_id)
            .otherwise(fallback_concept_id)
        )
        code = concepts.replace(concept_id)

    # value
    value = pl.lit(None, dtype=str)
    if table_details.get("string_value_field"):
        value = pl.col(table_details["string_value_field"])
    if table_details.get("numeric_value_field"):
        value = pl.coalesce(pl.col(table_details["numeric_value_field"]), value)

    if table_details.get("concept_id_value_field"):
        concept_id_value = pl.col(table_details["concept_id_value_field"]).cast(
            pl.Int64
        )
        # A value_as_concept_id with no string or numeric value either comes from a "Maps to value" mapping
        # (the data is already in the source concept, so drop it) or is a non-standard code the ETL put in this
        # table (so keep the source code value).
        backup_value = (
            pl.when((source_concept_id == 0) & (concept_id_value != 0))
            .then(
                "SOURCE_CODE/"
                + pl.col(concept_id_field.replace("_concept_id", "_source_value"))
            )
            .when(concept_id_value != 0)
            .then("OMOP_CONCEPT_ID/" + concept_id_value.cast(pl.Utf8()))
            .otherwise(pl.lit(None, dtype=pl.Utf8()))
        )
        value = pl.coalesce(value, backup_value)

    # properties
    metadata = {"table": pl.lit(table_name, dtype=str)}
    if "visit_occurrence_id" in schema.names():
        metadata["visit_id"] = pl.col("visit_occurrence_id")

    unit_columns = []
    if "unit_source_value" in schema.names():
        unit_columns.append(pl.col("unit_source_value"))
    if "unit_concept_id" in schema.names():
        unit_columns.append(concepts.replace(pl.col("unit_concept_id")))
    if unit_columns:
        metadata["unit"] = pl.coalesce(unit_columns)

    if "load_table_id" in schema.names():
        metadata["clarity_table"] = pl.col("load_table_id")
    if "note_id" in schema.names():
        metadata["note_id"] = pl.col("note_id")
    if (table_name + "_end_datetime") in schema.names():
        metadata["end"] = cast_to_datetime(
            schema, table_name + "_end_datetime", move_to_end_of_day=True
        )

    batch = batch.filter(code.is_not_null())

    numeric_value, text_value = convert_generic_value_to_specific(value)
    columns = {
        "subject_id": subject_id,
        "time": time,
        "code": code,
        "numeric_value": numeric_value,
        "text_value": text_value,
    }
    for k, v in metadata.items():
        columns[k] = v.alias(k)

    return batch.select(**columns)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Concepts and metadata (ported from meds_etl.omop.extract_metadata)            #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def _scan_lower(paths: list[Path]) -> pl.LazyFrame:
    lf = pl.scan_parquet(paths)
    return lf.rename({c: c.lower() for c in lf.collect_schema().names()})


def concept_codes(src: PathLike) -> pl.DataFrame:
    """``concept_id`` (Int64) -> ``code`` (``<vocabulary_id>/<concept_code>``), last row wins on duplicate IDs."""
    paths = table_files(src, "concept")
    if not paths:
        log.warning(
            "No concept table found; every concept-based code will be null and dropped"
        )
        return pl.DataFrame(schema={"concept_id": pl.Int64, "code": pl.Utf8})
    return (
        _scan_lower(paths)
        .select(
            concept_id=pl.col("concept_id").cast(pl.Int64),
            code=pl.col("vocabulary_id") + "/" + pl.col("concept_code"),
        )
        .unique("concept_id", keep="last", maintain_order=True)
        .collect()
    )


def extract_metadata(
    src: PathLike, concepts: pl.DataFrame
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Dataset metadata and code metadata (custom concepts with their "Maps to" parents)."""
    code_metadata: dict[str, dict[str, Any]] = {}

    concept_paths = table_files(src, "concept")
    if concept_paths:
        custom = (
            _scan_lower(concept_paths)
            .filter(pl.col("concept_id").cast(pl.Int64) > CUSTOM_CONCEPT_ID_START)
            .select(
                code=pl.col("vocabulary_id") + "/" + pl.col("concept_code"),
                description=pl.col("concept_name"),
            )
            .collect()
        )
        for code, description in custom.iter_rows():
            code_metadata[code] = {
                "code": code,
                "description": description,
                "parent_codes": [],
            }

    relationship_paths = table_files(src, "concept_relationship")
    if relationship_paths:
        id_1, id_2 = (
            pl.col("concept_id_1").cast(pl.Int64),
            pl.col("concept_id_2").cast(pl.Int64),
        )
        relationships = (
            _scan_lower(relationship_paths)
            .filter(
                id_1 > CUSTOM_CONCEPT_ID_START,
                pl.col("relationship_id") == "Maps to",
                id_1 != id_2,
            )
            .select(concept_id_1=id_1, concept_id_2=id_2)
            .collect()
        )
        needed = pl.concat(
            [relationships["concept_id_1"], relationships["concept_id_2"]]
        ).unique()
        code_of = dict(
            concepts.filter(pl.col("concept_id").is_in(needed.implode())).iter_rows()
        )
        for concept_id_1, concept_id_2 in relationships.iter_rows():
            if concept_id_1 in code_of and concept_id_2 in code_of:
                code_metadata[code_of[concept_id_1]]["parent_codes"].append(
                    code_of[concept_id_2]
                )

    datasets: list[str] = []
    dataset_versions: list[str] = []
    cdm_source_paths = table_files(src, "cdm_source")
    if cdm_source_paths:
        cdm_source = (
            _scan_lower(cdm_source_paths)
            .select("cdm_source_name", "cdm_release_date")
            .collect()
        )
        datasets.extend(cdm_source["cdm_source_name"].to_list())
        dataset_versions.extend(cdm_source["cdm_release_date"].to_list())

    metadata = {
        "dataset_name": "|".join(datasets),
        "dataset_version": "|".join(str(a) for a in dataset_versions),
        "etl_name": "omop_utils.to_meds",
        "etl_version": importlib.metadata.version("omop-utils"),
        "meds_version": meds.__version__,
    }
    return metadata, list(code_metadata.values())


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Sharding                                                                      #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def shard_boundaries(tables: list[SourceTable], num_shards: int) -> list[int]:
    """Person IDs at which shards 1..N-1 start, splitting the person table's IDs into ``num_shards`` even runs.

    Shard ``i`` covers ``[boundaries[i - 1], boundaries[i])``; the first and last shards are unbounded below and
    above, so events of persons missing from the person table still land in exactly one shard.
    """
    person = next((t for t in tables if t.name == "person"), None)
    if person is None:
        raise ValueError("The person table is required to plan shards")
    ids = (
        pl.scan_parquet([f.path for f in person.files])
        .select(
            pl.col(person.person_column).cast(pl.Int64).drop_nulls().unique().sort()
        )
        .collect()
        .to_series()
    )
    num_shards = max(1, min(num_shards, len(ids)))
    return [ids[len(ids) * i // num_shards] for i in range(1, num_shards)]


def output_schema(tables: list[SourceTable], concepts: ConceptMap) -> pl.Schema:
    """Union of every table's event columns: core MEDS columns, then properties sorted by name."""
    properties: dict[str, pl.DataType] = {}
    for table in tables:
        lf = pl.scan_parquet([f.path for f in table.files])
        for details in table.details:
            schema = table_events(
                lf, table_name=table.name, table_details=details, concepts=concepts
            ).collect_schema()
            for name, dtype in schema.items():
                if name in CORE_COLUMNS:
                    continue
                if properties.setdefault(name, dtype) != dtype:
                    raise ValueError(
                        f"Column {name} has type {dtype} in {table.name} but {properties[name]} elsewhere"
                    )
    return pl.Schema(
        {
            "subject_id": pl.Int64(),
            "time": pl.Datetime("us"),
            "code": pl.Utf8(),
            "numeric_value": pl.Float32(),
            **dict(sorted(properties.items())),
        }
    )


def shard_events(
    tables: list[SourceTable],
    *,  # enforce kwargs
    lo: int | None,
    hi: int | None,
    schema: pl.Schema,
    concepts: ConceptMap,
) -> pl.LazyFrame | None:
    """Sorted MEDS events for persons in ``[lo, hi)``, or None if no source file overlaps the range."""
    frames = []
    for table in tables:
        paths = table.files_for(lo, hi)
        if not paths:
            continue
        person_id = pl.col(table.person_column)
        lf = pl.scan_parquet(paths)
        if lo is not None:
            lf = lf.filter(person_id >= lo)
        if hi is not None:
            lf = lf.filter(person_id < hi)
        for details in table.details:
            events = table_events(
                lf, table_name=table.name, table_details=details, concepts=concepts
            )
            names = events.collect_schema().names()
            frames.append(
                events.select(
                    pl.col(c) if c in names else pl.lit(None, dtype=dtype).alias(c)
                    for c, dtype in schema.items()
                )
            )
    if not frames:
        return None
    return pl.concat(frames).sort(by=["subject_id", "time", "code"], nulls_last=False)


def write_shard(events: pl.DataFrame, path: Path) -> None:
    """Write ``events`` to ``path`` atomically, with the core columns cast to the MEDS schema.

    Property columns keep the Arrow types polars exports, as in meds_etl.
    """
    for column in ("subject_id", "code"):
        if events[column].null_count():
            raise ValueError(
                f"{path.name}: {events[column].null_count()} rows with null {column}"
            )
    table = events.to_arrow()
    properties = [(f.name, f.type) for f in table.schema if f.name not in CORE_COLUMNS]
    table = table.cast(meds.data_schema(properties))
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Driver                                                                        #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# Per-process state for shard workers, set by _init_worker
_STATE: dict[str, Any] = {}


def _init_worker(
    tables: list[SourceTable], concepts_path: Path, schema: pl.Schema, data_dir: Path
) -> None:
    concepts = pl.read_parquet(concepts_path)
    _STATE.update(
        tables=tables,
        concepts=ConceptMap(concepts["concept_id"], concepts["code"]),
        schema=schema,
        data_dir=data_dir,
    )


def _run_shard(task: tuple[int, int | None, int | None]) -> tuple[int, int]:
    """Build and write one shard; returns (shard index, rows written)."""
    index, lo, hi = task
    path = _STATE["data_dir"] / f"data_{index}.parquet"
    if path.exists():
        return index, -1
    events = shard_events(
        _STATE["tables"],
        lo=lo,
        hi=hi,
        schema=_STATE["schema"],
        concepts=_STATE["concepts"],
    )
    if events is None:
        return index, 0
    events = events.collect()
    if len(events):
        write_shard(events, path)
    return index, len(events)


def _check_plan(path: Path, plan: dict[str, Any]) -> None:
    """Record the shard plan in ``path``, or check it matches the plan of a previous run."""
    if path.exists():
        previous = json.loads(path.read_text())
        if previous != plan:
            raise RuntimeError(
                f"{path} records a different shard plan (num_shards / omop_version / source changed?); "
                "delete the output directory to start over"
            )
        log.info("Resuming; existing shards will be skipped")
    else:
        path.write_text(json.dumps(plan, indent=2))


def convert(
    src: PathLike,
    dest: PathLike,
    *,  # enforce kwargs
    num_shards: int = 100,
    num_proc: int = 1,
    threads_per_proc: int | None = None,
    omop_version: str = "5.4",
) -> Path:
    """Convert the OMOP download in ``src`` to a MEDS dataset in ``dest``.

    Writes ``<dest>/metadata/{dataset.json,codes.parquet}`` and ``<dest>/data/data_<shard>.parquet``.
    Peak memory is roughly ``num_proc`` x (one shard's events, sorted) + the concept lookup per process, so raise
    ``num_shards`` or lower ``num_proc`` if a run runs out of memory. Re-running with the same arguments resumes,
    skipping shards that were already written.
    """
    src, dest = Path(src), Path(dest)
    if not src.is_dir():
        raise ValueError(f"The source OMOP folder ({src}) does not exist")
    dest.mkdir(parents=True, exist_ok=True)
    data_dir = dest / "data"
    metadata_dir = dest / "metadata"
    data_dir.mkdir(exist_ok=True)
    metadata_dir.mkdir(exist_ok=True)

    tables = find_tables(src, omop_version)
    concepts = concept_codes(src)
    concepts_path = dest / "concepts.tmp.parquet"
    concepts.write_parquet(concepts_path)
    try:
        concept_map = ConceptMap(concepts["concept_id"], concepts["code"])

        log.info("Writing metadata")
        metadata, code_metadata = extract_metadata(src, concepts)
        (metadata_dir / "dataset.json").write_text(json.dumps(metadata))
        pq.write_table(
            pa.Table.from_pylist(code_metadata, meds.code_metadata_schema()),
            metadata_dir / "codes.parquet",
        )

        schema = output_schema(tables, concept_map)
        del concepts, concept_map

        boundaries = shard_boundaries(tables, num_shards)
        _check_plan(
            dest / _PLAN,
            {
                "src": str(src.resolve()),
                "omop_version": omop_version,
                "boundaries": boundaries,
            },
        )
        bounds: list[int | None] = [None, *boundaries, None]
        tasks = [(i, bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]
        log.info("Writing %d shards with %d process(es)", len(tasks), num_proc)

        init_args = (tables, concepts_path, schema, data_dir)
        total = 0
        if num_proc == 1:
            _init_worker(*init_args)
            results = map(_run_shard, tasks)
            total = _log_shards(results, len(tasks))
        else:
            threads = threads_per_proc or max(1, (os.cpu_count() or 1) // num_proc)
            # Spawned workers read POLARS_MAX_THREADS when they import polars
            previous = os.environ.get("POLARS_MAX_THREADS")
            os.environ["POLARS_MAX_THREADS"] = str(threads)
            try:
                ctx = multiprocessing.get_context("spawn")
                with ctx.Pool(
                    num_proc,
                    initializer=_init_worker,
                    initargs=init_args,
                    maxtasksperchild=8,
                ) as pool:
                    total = _log_shards(
                        pool.imap_unordered(_run_shard, tasks), len(tasks)
                    )
            finally:
                if previous is None:
                    os.environ.pop("POLARS_MAX_THREADS", None)
                else:
                    os.environ["POLARS_MAX_THREADS"] = previous
    finally:
        concepts_path.unlink(missing_ok=True)

    log.info("Finished: %s events written to %s", f"{total:,}", data_dir)
    return dest


def _log_shards(results: Iterable[tuple[int, int]], n: int) -> int:
    total = 0
    for done, (index, rows) in enumerate(results, start=1):
        if rows < 0:
            log.info("[%d/%d] shard %d already written, skipped", done, n, index)
        else:
            total += rows
            log.info("[%d/%d] shard %d: %s events", done, n, index, f"{rows:,}")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="omop-to-meds",
        description="Convert an omop_utils OMOP download to MEDS 0.3.3, one person ID range per shard.",
    )
    parser.add_argument(
        "src",
        type=Path,
        help="OMOP download directory (the --output-dir of omop_utils)",
    )
    parser.add_argument("dest", type=Path, help="Output MEDS directory")
    parser.add_argument(
        "--num-shards",
        type=int,
        default=100,
        help="Number of output shards (contiguous person ID ranges); more shards means less memory per shard",
    )
    parser.add_argument(
        "--num-proc",
        type=int,
        default=1,
        help="Shards built in parallel; memory scales with this",
    )
    parser.add_argument(
        "--threads-per-proc",
        type=int,
        default=None,
        help="Polars threads per process (default: CPUs / num-proc)",
    )
    parser.add_argument("--omop-version", choices=["5.3", "5.4"], default="5.4")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format=LOG_FORMAT)
    with log_to_file(args.dest, name="omop_to_meds", level=level):
        convert(
            args.src,
            args.dest,
            num_shards=args.num_shards,
            num_proc=args.num_proc,
            threads_per_proc=args.threads_per_proc,
            omop_version=args.omop_version,
        )


if __name__ == "__main__":
    main()
