"""Convert a CSV download (``.csv.gz`` or ``.csv``) to Parquet in place.

Each table's Arrow schema is read from the database, so converted files have
exactly the types a fresh download writes: every chunk of a table agrees, and a
partial download can be converted and then resumed. Values are parsed with
that schema, not inferred.

Patient-table chunks ``<dir>/<table>/<first_id>-<last_id>.csv[.gz]`` become
``<first_id>-<last_id>.parquet`` and non-patient tables ``<dir>/<table>.csv[.gz]``
become ``<table>.parquet``. Originals are deleted unless ``--keep`` is given.
Files are written to ``.parquet.tmp`` and renamed into place, so an interrupted
run never leaves a truncated ``.parquet``; re-running picks up where it left off.

    uv run python utils/csv_to_parquet.py out/ --credentials creds.json --database my_omop_db
"""

import argparse
import logging
from pathlib import Path

import mssql_python
import pyarrow as pa
import pyarrow.csv as pacsv

from omop_utils.connection import connect
from omop_utils.download import _q, write_parquet
from omop_utils.logs import LOG_FORMAT

log = logging.getLogger("csv_to_parquet")

# CSV bytes per batch; each batch is a row group, so keep them from being tiny
_BLOCK_SIZE = 64 << 20


def table_schema(
    conn: mssql_python.Connection,
    *,  # enforce kwargs
    schema: str,
    table: str,
) -> pa.Schema:
    """Arrow schema of ``schema.table``, exactly as the downloads write it."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT TOP 0 * FROM {_q(schema)}.{_q(table)}")
        with cur.arrow_reader() as reader:
            return reader.schema


def _stem(csv: Path) -> str:
    return csv.name.removesuffix(".gz").removesuffix(".csv")


def csv_to_parquet(
    src: Path,
    *,  # enforce kwargs
    schema: pa.Schema,
    keep: bool = False,
) -> Path:
    """Convert the CSV ``src`` (gzipped if it ends in ``.gz``) to Parquet with ``schema``."""
    final = src.with_name(_stem(src) + ".parquet")
    # if final exists, an interrupted run converted src but didn't delete it
    if not final.exists():
        with pacsv.open_csv(
            src,
            read_options=pacsv.ReadOptions(block_size=_BLOCK_SIZE),
            convert_options=pacsv.ConvertOptions(
                column_types=schema,
                # the CSV writer left nulls unquoted and quoted empty strings
                strings_can_be_null=True,
                quoted_strings_can_be_null=False,
            ),
        ) as reader:
            if reader.schema.names != schema.names:
                raise ValueError(
                    f"{src} has columns {reader.schema.names}, but the database has {schema.names}"
                )
            # CSV doesn't carry nullability, so match the database's NOT NULLs
            write_parquet((b.cast(schema) for b in reader), schema=schema, final=final)
    if not keep:
        src.unlink()
    return final


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directory", type=Path, help="Output directory of a download")
    parser.add_argument(
        "--credentials", type=Path, required=True, help="JSON credential file"
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", default="cdm")
    parser.add_argument("--keep", action="store_true", help="Keep the original CSVs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    # stale output from interrupted runs, of this script and of CSV downloads
    for pattern in ("*.parquet.tmp", "*.csv.tmp", "*.csv.gz.tmp"):
        for tmp in [
            *args.directory.glob(pattern),
            *args.directory.glob(f"*/{pattern}"),
        ]:
            tmp.unlink()

    # non-patient tables are <dir>/<table>.csv[.gz], patient tables <dir>/<table>/*.csv[.gz]
    tables: dict[str, list[Path]] = {}
    for pattern in ("*.csv", "*.csv.gz"):
        for p in args.directory.glob(pattern):
            tables.setdefault(_stem(p), []).append(p)
        for p in args.directory.glob(f"*/{pattern}"):
            tables.setdefault(p.parent.name, []).append(p)

    with connect(credential_file=args.credentials, database=args.database) as conn:
        schemas = {t: table_schema(conn, schema=args.schema, table=t) for t in tables}

    n = sum(map(len, tables.values()))
    i = 0
    for table, srcs in sorted(tables.items()):
        for src in sorted(srcs):
            i += 1
            final = csv_to_parquet(src, schema=schemas[table], keep=args.keep)
            log.info("[%d/%d] %s", i, n, final)
    log.info("Converted %d files", n)


if __name__ == "__main__":
    main()
