import logging
import os
import re
from collections.abc import Callable, Iterable
from os import PathLike
from pathlib import Path

import mssql_python
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

_CHUNK_NAME = re.compile(r"^(-?\d+)-(-?\d+)\.parquet$")
_COMPLETE = "_COMPLETE"
_COMPRESSION = "zstd"


def _q(identifier: str) -> str:
    # SQL Server specific quoting
    return "[" + identifier.replace("]", "]]") + "]"


def download_patient_table(
    conn: mssql_python.Connection,
    *,  # enforce kwargs
    table: str,
    output_dir: PathLike,
    schema: str = "cdm",
    person_column: str = "person_id",
    person_table: str = "person",
    patients_per_chunk: int = 1_000,
    fetch_size: int = 50_000,
    on_progress: Callable[[int], None] | None = None,
    check_clustered: bool = True,
) -> Path:
    """Download ``schema.table`` as a directory of Parquet chunks of whole patients.

    Chunks of whole patients are written to ``<output_dir>/<table>/<first_id>-<last_id>.parquet``.
    Each file holds every row for the patients in its inclusive person ID range.

    In-progress chunks are suffixed with ``.tmp`` and renamed into place when complete, so every ``.parquet`` in the directory is a whole chunk.
    If interrupted, ``.tmp`` files are stale and deleted on resume.
    On resume, downloading continues after the highest person ID among the finished chunks.
    This assumes that the table does not change between runs.
    When the table download is done, an empty ``_COMPLETE`` marker is written.
    """
    if check_clustered:
        assert_clustered_on(conn, schema=schema, table=table, column=person_column)

    out = Path(output_dir) / table
    out.mkdir(parents=True, exist_ok=True)
    marker = out / _COMPLETE

    # removed stale interrupted chunks
    for tmp in out.glob("*.parquet.tmp"):
        tmp.unlink()

    chunks = list(out.glob("*.parquet"))
    if marker.exists():
        log.info(
            "%s is already complete; skipping (delete %s to re-download)", table, out
        )
        return out

    # cannot pass as query parameters
    qualified = f"{_q(schema)}.{_q(table)}"
    pid = _q(person_column)

    # chunk names are <first_id>-<last_id>.parquet
    last_read_pid = max(
        (int(_CHUNK_NAME.match(p.name).group(2)) for p in chunks), default=None
    )
    if last_read_pid is not None:
        log.info(
            "Resuming %s after person %s (delete %s to start over)",
            table,
            last_read_pid,
            out,
        )

    _log_remaining_estimate(
        conn,
        schema=schema,
        table=table,
        person_table=person_table,
        person_column=person_column,
        last_read_pid=last_read_pid,
    )

    rows = 0
    with conn.cursor() as cur:
        while True:
            # get chunk PID range
            where_clause = ""
            if last_read_pid is not None:
                # NOTE: last_read_pid also gets set at the end of the loop
                where_clause = f"WHERE {pid} > %(last_read_pid)s"
            cur.execute(
                (
                    f"SELECT MIN({pid}), MAX({pid}) FROM ("
                    f"    SELECT DISTINCT TOP (%(patients_per_chunk)s) {pid}"
                    f"    FROM {qualified} {where_clause}"
                    f"    ORDER BY {pid}"
                    f") AS chunk"
                ),
                {
                    "patients_per_chunk": patients_per_chunk,
                    "last_read_pid": last_read_pid,  # ignored if absent in query
                },
            )
            chunk_lo, chunk_hi = cur.fetchone()
            if chunk_hi is None:
                # None when there are no pids > last_read_pid
                break

            # read the chunk and write to disk
            final = out / f"{chunk_lo:020d}-{chunk_hi:020d}.parquet"
            cur.execute(
                f"SELECT * FROM {qualified} WHERE {pid} BETWEEN %(chunk_lo)s AND %(chunk_hi)s ORDER BY {pid}",
                {"chunk_lo": chunk_lo, "chunk_hi": chunk_hi},
            )
            with cur.arrow_reader(batch_size=fetch_size) as reader:
                rows += write_parquet(reader, schema=reader.schema, final=final)

            last_read_pid = chunk_hi
            if on_progress:
                on_progress(rows)

    marker.touch()
    log.info("Finished %s (%d rows this run), written to %s", table, rows, out)
    return out


def download_table(
    conn: mssql_python.Connection,
    *,  # enforce kwargs
    table: str,
    output_dir: PathLike,
    schema: str = "cdm",
    fetch_size: int = 50_000,
) -> Path:
    """Download the non-patient table ``schema.table`` as a single Parquet file.

    The table is written to ``<output_dir>/<table>.parquet``.
    It is read in one pass into ``<table>.parquet.tmp`` and renamed into place when complete, so the output is always whole.
    A stale ``.tmp`` from an interrupted run is overwritten.
    If the ``.parquet`` already exists, the download is skipped.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / f"{table}.parquet"

    if final.exists():
        log.info(
            "%s is already downloaded; skipping (delete %s to re-download)",
            table,
            final,
        )
        return final

    qualified = f"{_q(schema)}.{_q(table)}"
    log.info(
        "%s has %d rows to download",
        table,
        _metadata_row_count(conn, schema=schema, table=table),
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {qualified}")
        with cur.arrow_reader(batch_size=fetch_size) as reader:
            rows = write_parquet(reader, schema=reader.schema, final=final)

    log.info("Finished %s (%d rows), written to %s", table, rows, final)
    return final


def write_parquet(
    batches: Iterable[pa.RecordBatch],
    *,  # enforce kwargs
    schema: pa.Schema,
    final: Path,
) -> int:
    """Write ``batches`` to ``final`` as zstd-compressed Parquet, returning the row count.

    Each batch becomes (at least) one row group.
    Rows are written to ``final`` + ``.tmp`` and renamed into place when complete.
    """
    tmp = final.with_name(final.name + ".tmp")
    rows = 0
    with open(tmp, "wb") as f:
        # ParquetWriter doesn't close f, so it can still be fsynced below
        with pq.ParquetWriter(f, schema, compression=_COMPRESSION) as writer:
            for batch in batches:
                writer.write_batch(batch)
                rows += batch.num_rows
        # Without this, a machine crash after the rename could leave a
        # truncated file under the final name.
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, final)  # atomic: the file appears whole or not at all
    return rows


def _metadata_row_count(
    conn: mssql_python.Connection,
    *,  # enforce kwargs
    schema: str,
    table: str,
) -> int:
    """Row count of ``schema.table`` from catalog metadata (no table scan)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT SUM(rows)
            FROM sys.partitions
            WHERE object_id = OBJECT_ID(?)
              AND index_id IN (0, 1)  -- heap or clustered index
            """,
            [f"{_q(schema)}.{_q(table)}"],
        )
        return int(cur.fetchone()[0] or 0)


def _log_remaining_estimate(
    conn: mssql_python.Connection,
    *,  # enforce kwargs
    schema: str,
    table: str,
    person_table: str,
    person_column: str,
    last_read_pid: int | None,
) -> None:
    """Log the approximate rows left to download for a patient table.

    Scales the table's metadata row count by the fraction of patients with ID
    above ``last_read_pid``. This is approximate since rows per patient vary.
    """
    total = _metadata_row_count(conn, schema=schema, table=table)
    if last_read_pid is None:
        log.info("%s has %d rows to download", table, total)
        return

    people = _q(schema) + "." + _q(person_table)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT_BIG(*), COUNT_BIG(CASE WHEN {_q(person_column)} > ? THEN 1 END) FROM {people}",
            [last_read_pid],
        )
        n_people, n_remaining = cur.fetchone()
    if not n_people:
        return
    log.info(
        "%s has ~%d of %d rows remaining (%d of %d patients left)",
        table,
        round(total * n_remaining / n_people),
        total,
        n_remaining,
        n_people,
    )


def assert_clustered_on(
    conn: mssql_python.Connection,
    *,  # enforce kwargs
    schema: str,
    table: str,
    column: str,
) -> None:
    """Raise unless ``column`` is the leading key of the table's clustered index.

    The leading column is what matters: it's what lets the per-chunk range
    queries seek and read in order instead of scanning/sorting the table.
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic
              ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c
              ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(?)
              AND i.type = 1          -- rowstore clustered index
              AND ic.key_ordinal = 1  -- leading key column
            """,
            [f"{_q(schema)}.{_q(table)}"],
        )
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError(
            f"{schema}.{table} has no clustered index (or doesn't exist / isn't visible); refusing to chunk by {column!r}."
        )
    if row[0].lower() != column.lower():
        raise RuntimeError(
            f"{schema}.{table} is clustered on {row[0]!r}, not {column!r}; refusing to chunk by it."
        )
