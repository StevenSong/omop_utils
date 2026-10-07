import logging
import os
import re
from collections.abc import Callable
from os import PathLike
from pathlib import Path

import mssql_python
import pyarrow.csv as pacsv

log = logging.getLogger(__name__)

_CHUNK_NAME = re.compile(r"^(-?\d+)-(-?\d+)\.csv$")
_COMPLETE = "_COMPLETE"


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
    patients_per_chunk: int = 1_000,
    fetch_size: int = 50_000,
    on_progress: Callable[[int], None] | None = None,
    check_clustered: bool = True,
) -> Path:
    """Download ``schema.table`` as a directory of CSV chunks of whole patients.

    Chunks of whole patients are written to ``<output_dir>/<table>/<first_id>-<last_id>.csv``.
    Each file has a header and holds every row for the patients in its inclusive person ID range.

    In-progress chunks are suffixed with ``.tmp`` and renamed into place when complete, so every ``.csv`` in the directory is a whole chunk.
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
    for tmp in out.glob("*.csv.tmp"):
        tmp.unlink()

    chunks = sorted(out.glob("*.csv"))
    if marker.exists():
        log.info(
            "%s is already complete; skipping (delete %s to re-download)", table, out
        )
        return out

    # cannot pass as query parameters
    qualified = f"{_q(schema)}.{_q(table)}"
    pid = _q(person_column)

    # chunk names are <first_id>-<last_id>.csv
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
            final = out / f"{chunk_lo:020d}-{chunk_hi:020d}.csv"
            tmp = final.with_name(final.name + ".tmp")
            cur.execute(
                f"SELECT * FROM {qualified} WHERE {pid} BETWEEN %(chunk_lo)s AND %(chunk_hi)s ORDER BY {pid}",
                {"chunk_lo": chunk_lo, "chunk_hi": chunk_hi},
            )
            with open(tmp, "wb") as f:
                with (
                    cur.arrow_reader(batch_size=fetch_size) as reader,
                    pacsv.CSVWriter(f, reader.schema) as writer,
                ):
                    for batch in reader:
                        writer.write_batch(batch)
                        rows += batch.num_rows
                # Without this, a machine crash after the rename could leave a
                # truncated file under the final name.
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, final)  # atomic: the chunk appears whole or not at all

            last_read_pid = chunk_hi
            if on_progress:
                on_progress(rows)

    marker.touch()
    log.info("Finished %s (%d rows this run), written to %s", table, rows, out)
    return out


def assert_clustered_on(conn, *, schema: str, table: str, column: str) -> None:
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
