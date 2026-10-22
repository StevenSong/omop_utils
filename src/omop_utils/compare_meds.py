"""Check that two MEDS datasets contain the same events, regardless of how subjects are sharded.

Two datasets are equivalent when:

- their ``metadata/dataset.json`` match, apart from ``etl_name`` / ``etl_version``;
- their ``metadata/codes.parquet`` hold the same rows (in any order, with ``parent_codes`` in any order);
- their data files have the same columns and types;
- every subject has exactly the same multiset of event rows in both. Row order within a timestamp is not compared:
  MEDS only requires events sorted by time, and the meds_etl backends break ties differently (polars by ``code``,
  cpp not at all).

Each dataset is also checked to be valid MEDS: no null ``subject_id`` / ``code``, each file sorted by
``(subject_id, time)``, and each subject in exactly one file.

The meds_etl cpp backend writes empty strings as null; ``empty_strings_as_null`` normalizes both sides to compare
against its output.

Events are compared in buckets of contiguous subject IDs so memory stays bounded; both pipelines write files sorted
by ``subject_id``, so Parquet statistics let each bucket skip most row groups.
"""

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

SORT_KEYS = ["subject_id", "time", "code"]
MEDS_ORDER = [
    "subject_id",
    "time",
]  # required by MEDS; meds_etl (polars) also sorts by code
_IGNORED_DATASET_KEYS = {"etl_name", "etl_version"}


@dataclass
class Report:
    problems: list[str] = field(default_factory=list)

    @property
    def equivalent(self) -> bool:
        return not self.problems

    def fail(self, message: str) -> None:
        log.error(message)
        self.problems.append(message)


def data_files(root: PathLike) -> list[Path]:
    files = sorted((Path(root) / "data").rglob("*.parquet"))
    if not files:
        raise ValueError(f"No data files under {Path(root) / 'data'}")
    return files


def _examples(df: pl.DataFrame, n: int) -> str:
    return "\n".join(f"    {row}" for row in df.head(n).iter_rows(named=True))


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Metadata                                                                      #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def compare_metadata(
    old: Path, new: Path, report: Report, *, max_examples: int = 5
) -> None:
    old_dataset = json.loads((old / "metadata" / "dataset.json").read_text())
    new_dataset = json.loads((new / "metadata" / "dataset.json").read_text())
    for key in sorted(old_dataset.keys() | new_dataset.keys()):
        a, b = old_dataset.get(key), new_dataset.get(key)
        if a == b:
            continue
        if key in _IGNORED_DATASET_KEYS:
            log.info("dataset.json %s differs (expected): %r vs %r", key, a, b)
        else:
            report.fail(f"dataset.json {key} differs: {a!r} vs {b!r}")

    def codes(root: Path) -> pl.DataFrame:
        df = pl.read_parquet(root / "metadata" / "codes.parquet")
        if "parent_codes" in df.columns:
            df = df.with_columns(pl.col("parent_codes").list.sort())
        # List columns can't be sort or join keys, so key each row by a string rendering of it
        key = pl.concat_str(
            [
                (
                    pl.col(c).list.join("\x1e")
                    if df.schema[c] == pl.List(pl.Utf8)
                    else pl.col(c).cast(pl.Utf8)
                ).fill_null("\x00")
                for c in df.columns
            ],
            separator="\x1f",
        )
        return df.with_columns(_row=key).sort("_row")

    old_codes, new_codes = codes(old), codes(new)
    if old_codes.schema != new_codes.schema:
        report.fail(
            f"codes.parquet schemas differ: {old_codes.schema} vs {new_codes.schema}"
        )
    elif not old_codes.equals(new_codes):
        only_old = old_codes.join(new_codes.select("_row"), on="_row", how="anti").drop(
            "_row"
        )
        only_new = new_codes.join(old_codes.select("_row"), on="_row", how="anti").drop(
            "_row"
        )
        report.fail(
            f"codes.parquet differs: {len(old_codes)} vs {len(new_codes)} rows; "
            f"{len(only_old)} only in old, {len(only_new)} only in new\n"
            f"  only in old:\n{_examples(only_old, max_examples)}\n"
            f"  only in new:\n{_examples(only_new, max_examples)}"
        )
    else:
        log.info("codes.parquet: %d rows match", len(old_codes))


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Schema and MEDS layout                                                        #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def dataset_schema(files: Sequence[Path], label: str, report: Report) -> pl.Schema:
    """The polars schema shared by all ``files`` (reported if they disagree)."""
    schemas: dict[tuple, list[Path]] = {}
    for path in files:
        schemas.setdefault(tuple(pl.read_parquet_schema(path).items()), []).append(path)
    if len(schemas) > 1:
        detail = "\n".join(
            f"    {dict(s)} ({len(p)} files, e.g. {p[0].name})"
            for s, p in schemas.items()
        )
        report.fail(
            f"{label}: data files have {len(schemas)} different schemas:\n{detail}"
        )
    return pl.Schema(next(iter(schemas)))


def compare_schemas(
    old_files: Sequence[Path],
    new_files: Sequence[Path],
    report: Report,
    *,
    ignore_columns: Sequence[str] = (),
) -> list[str]:
    """Report column/type differences, except in ``ignore_columns``; returns the columns to compare events on."""

    def schema(files, label):
        full = dataset_schema(files, label, report)
        return pl.Schema({c: t for c, t in full.items() if c not in ignore_columns})

    old_schema = schema(old_files, "old")
    new_schema = schema(new_files, "new")
    columns = [c for c in old_schema.names() if c in new_schema]
    if missing := [c for c in old_schema.names() if c not in new_schema]:
        report.fail(f"columns only in old: {missing}")
    if extra := [c for c in new_schema.names() if c not in old_schema]:
        report.fail(f"columns only in new: {extra}")
    for c in columns:
        if old_schema[c] != new_schema[c]:
            report.fail(
                f"column {c} has type {old_schema[c]} in old but {new_schema[c]} in new"
            )
    if [c for c in old_schema.names() if c in columns] != [
        c for c in new_schema.names() if c in columns
    ]:
        log.warning(
            "column order differs: %s vs %s", old_schema.names(), new_schema.names()
        )

    # Physical Arrow types (e.g. string vs large_string) don't change the data, but are worth knowing about
    old_arrow, new_arrow = pq.read_schema(old_files[0]), pq.read_schema(new_files[0])
    for c in columns:
        a, b = old_arrow.field(c).type, new_arrow.field(c).type
        if a != b:
            log.warning(
                "column %s is stored as %s in old but %s in new (same polars type)",
                c,
                a,
                b,
            )
    log.info("schema: %d shared columns %s", len(columns), columns)
    return [c for c in columns if old_schema[c] == new_schema[c]]


def check_layout(
    files: Sequence[Path], label: str, report: Report, *, max_examples: int = 5
) -> None:
    """Check ``files`` form valid MEDS: non-null keys, sorted files, and each subject in one file."""
    subjects = []
    for i, path in enumerate(files):
        df = pl.read_parquet(path, columns=SORT_KEYS)
        for column in ("subject_id", "code"):
            if nulls := df[column].null_count():
                report.fail(f"{label}: {path.name} has {nulls} null {column}")
        if not df.equals(df.sort(MEDS_ORDER, nulls_last=False, maintain_order=True)):
            report.fail(
                f"{label}: {path.name} is not sorted by {MEDS_ORDER} (nulls first)"
            )
        subjects.append(df.select(pl.col("subject_id").unique(), file=pl.lit(i)))
    split = (
        pl.concat(subjects)
        .group_by("subject_id")
        .agg(pl.col("file").sort())
        .filter(pl.col("file").list.len() > 1)
        .sort("subject_id")
    )
    if len(split):
        examples = ", ".join(
            f"{sid} in {[files[i].name for i in f]}"
            for sid, f in split.head(max_examples).iter_rows()
        )
        report.fail(
            f"{label}: {len(split)} subjects appear in more than one file, e.g. {examples}"
        )
    log.info("%s: %d files, layout checked", label, len(files))


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Events                                                                        #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def _subject_counts(files: Sequence[Path]) -> pl.DataFrame:
    return pl.scan_parquet(files).group_by("subject_id").len().collect()


def _buckets(counts: pl.DataFrame, rows_per_bucket: int) -> list[tuple[int, int]]:
    """Inclusive subject ID ranges holding about ``rows_per_bucket`` rows each."""
    counts = counts.sort("subject_id").with_columns(
        bucket=(pl.col("rows").cum_sum() - 1) // rows_per_bucket
    )
    ranges = counts.group_by("bucket", maintain_order=True).agg(
        lo=pl.col("subject_id").first(), hi=pl.col("subject_id").last()
    )
    return list(ranges.select("lo", "hi").iter_rows())


def _empty_to_null(df: pl.DataFrame) -> pl.DataFrame:
    strings = [c for c, dtype in df.schema.items() if dtype == pl.Utf8]
    return df.with_columns(
        pl.when(pl.col(c) != "").then(pl.col(c)).alias(c) for c in strings
    )


def _bucket(
    files: Sequence[Path],
    columns: list[str],
    lo: int,
    hi: int,
    *,
    empty_strings_as_null: bool,
) -> pl.DataFrame:
    df = (
        pl.scan_parquet(files)
        .filter(pl.col("subject_id").is_between(lo, hi))
        .select(columns)
        .collect()
    )
    if empty_strings_as_null:
        df = _empty_to_null(df)
    return df.sort(columns, nulls_last=False)


def _row_diff(
    old: pl.DataFrame, new: pl.DataFrame, columns: list[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Rows (with multiplicity) only in ``old`` and only in ``new``."""
    a = old.group_by(columns).len("n_old")
    b = new.group_by(columns).len("n_new")
    joined = a.join(
        b, on=columns, how="full", coalesce=True, nulls_equal=True
    ).with_columns(pl.col("n_old", "n_new").fill_null(0))
    only_old = joined.filter(pl.col("n_old") > pl.col("n_new"))
    only_new = joined.filter(pl.col("n_new") > pl.col("n_old"))
    return (
        only_old.with_columns(count=pl.col("n_old") - pl.col("n_new"))
        .drop("n_old", "n_new")
        .sort(SORT_KEYS),
        only_new.with_columns(count=pl.col("n_new") - pl.col("n_old"))
        .drop("n_old", "n_new")
        .sort(SORT_KEYS),
    )


def compare_events(
    old_files: Sequence[Path],
    new_files: Sequence[Path],
    columns: list[str],
    report: Report,
    *,  # enforce kwargs
    rows_per_bucket: int = 2_000_000,
    max_examples: int = 5,
    empty_strings_as_null: bool = False,
) -> None:
    old_counts = _subject_counts(old_files)
    new_counts = _subject_counts(new_files)
    log.info(
        "events: old has %s rows / %s subjects, new has %s rows / %s subjects",
        f"{old_counts['len'].sum():,}",
        f"{len(old_counts):,}",
        f"{new_counts['len'].sum():,}",
        f"{len(new_counts):,}",
    )
    counts = old_counts.join(
        new_counts, on="subject_id", how="full", coalesce=True, suffix="_new"
    ).rename({"len": "old", "len_new": "new"})
    for name, other in (("old", "new"), ("new", "old")):
        only = counts.filter(pl.col(other).is_null()).sort("subject_id")
        if len(only):
            report.fail(
                f"{len(only)} subjects only in {name} ({only[name].sum():,} rows), "
                f"e.g. {only['subject_id'].head(max_examples).to_list()}"
            )
    differ = counts.filter(pl.col("old") != pl.col("new")).sort("subject_id")
    if len(differ):
        report.fail(
            f"{len(differ)} subjects have different row counts, e.g. (subject_id, old, new): "
            f"{differ.head(max_examples).rows()}"
        )

    counts = counts.select(
        "subject_id",
        rows=pl.max_horizontal(pl.col("old").fill_null(0), pl.col("new").fill_null(0)),
    )
    buckets = _buckets(counts, rows_per_bucket)
    mismatched = 0
    for i, (lo, hi) in enumerate(buckets, start=1):
        old = _bucket(
            old_files, columns, lo, hi, empty_strings_as_null=empty_strings_as_null
        )
        new = _bucket(
            new_files, columns, lo, hi, empty_strings_as_null=empty_strings_as_null
        )
        if old.equals(new):
            log.info(
                "[%d/%d] subjects %d..%d: %s rows match",
                i,
                len(buckets),
                lo,
                hi,
                f"{len(old):,}",
            )
            continue
        mismatched += 1
        only_old, only_new = _row_diff(old, new, columns)
        hint = ""
        if not empty_strings_as_null:
            a, b = _empty_to_null(old), _empty_to_null(new)
            if a.sort(columns, nulls_last=False).equals(
                b.sort(columns, nulls_last=False)
            ):
                hint = " (all differences are empty strings vs nulls, as the meds_etl cpp backend writes them; "
                hint += "use empty_strings_as_null / --empty-strings-as-null to ignore)"
        report.fail(
            f"[{i}/{len(buckets)}] subjects {lo}..{hi}: {only_old['count'].sum():,} rows only in old, "
            f"{only_new['count'].sum():,} only in new{hint}\n"
            f"  only in old:\n{_examples(only_old, max_examples)}\n"
            f"  only in new:\n{_examples(only_new, max_examples)}"
        )
    if mismatched:
        log.error("%d of %d buckets differ", mismatched, len(buckets))


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Driver                                                                        #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def compare(
    old: PathLike,
    new: PathLike,
    *,  # enforce kwargs
    ignore_columns: Sequence[str] = (),
    rows_per_bucket: int = 2_000_000,
    max_examples: int = 5,
    empty_strings_as_null: bool = False,
) -> Report:
    """Compare the MEDS datasets ``old`` and ``new``; see the module docstring for what is checked."""
    old, new = Path(old), Path(new)
    report = Report()
    old_files, new_files = data_files(old), data_files(new)

    compare_metadata(old, new, report, max_examples=max_examples)
    check_layout(old_files, "old", report, max_examples=max_examples)
    check_layout(new_files, "new", report, max_examples=max_examples)
    columns = compare_schemas(
        old_files, new_files, report, ignore_columns=ignore_columns
    )
    for c in SORT_KEYS:
        if c not in columns:
            raise ValueError(f"Can't compare events without the {c} column")
    compare_events(
        old_files,
        new_files,
        columns,
        report,
        rows_per_bucket=rows_per_bucket,
        max_examples=max_examples,
        empty_strings_as_null=empty_strings_as_null,
    )

    if report.equivalent:
        log.info("EQUIVALENT: %s and %s contain the same events and metadata", old, new)
    else:
        log.error("NOT EQUIVALENT: %d problem(s) found", len(report.problems))
    return report
