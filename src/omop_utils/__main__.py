import argparse
import logging
from functools import partial
from pathlib import Path

from .logs import LOG_FORMAT, log_to_file

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-v", "--verbose", action="store_true", help="Debug logging")

    parser = argparse.ArgumentParser(
        prog="omop_utils",
        description="Download OMOP tables from SQL Server and convert them to MEDS.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    download = commands.add_parser(
        "download",
        parents=[common],
        help="Download OMOP tables from SQL Server as Parquet files",
        description="Download OMOP tables from SQL Server as Parquet files.",
    )
    download.add_argument(
        "--patient-tables",
        nargs="+",
        default=[],
        help="Patient tables to download as per-patient Parquet chunks, e.g. --patient-tables person measurement",
    )
    download.add_argument(
        "--non-patient-tables",
        nargs="+",
        default=[],
        help="Non-patient tables to download as a single Parquet file, e.g. --non-patient-tables concept cdm_source",
    )
    download.add_argument(
        "--credentials", type=Path, required=True, help="JSON credential file"
    )
    download.add_argument("--database", required=True)
    download.add_argument("--schema", default="cdm")
    download.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Root output directory; patient tables are written to <output-dir>/<table>/, non-patient tables to <output-dir>/<table>.parquet",
    )
    download.add_argument("--patients-per-chunk", type=int, default=1_000)
    download.add_argument("--fetch-size", type=int, default=50_000)
    download.set_defaults(handler=_download)

    to_meds = commands.add_parser(
        "to-meds",
        parents=[common],
        help="Convert a download to MEDS 0.3.3",
        description="Convert an OMOP download to MEDS 0.3.3, one person ID range per shard.",
    )
    to_meds.add_argument(
        "src", type=Path, help="OMOP download directory (the --output-dir of download)"
    )
    to_meds.add_argument("dest", type=Path, help="Output MEDS directory")
    to_meds.add_argument(
        "--num-shards",
        type=int,
        default=100,
        help="Number of output shards (contiguous person ID ranges); more shards means less memory per shard",
    )
    to_meds.add_argument(
        "--num-proc",
        type=int,
        default=1,
        help="Shards built in parallel; memory scales with this",
    )
    to_meds.add_argument(
        "--threads-per-proc",
        type=int,
        default=None,
        help="Polars threads per process (default: CPUs / num-proc)",
    )
    to_meds.add_argument("--omop-version", choices=["5.3", "5.4"], default="5.4")
    to_meds.set_defaults(handler=_to_meds)

    compare = commands.add_parser(
        "compare",
        parents=[common],
        help="Check two MEDS datasets contain the same events",
        description="Check two MEDS datasets contain the same events, ignoring sharding. Exits 1 if they differ.",
    )
    compare.add_argument(
        "old", type=Path, help="Reference MEDS directory (e.g. from meds_etl_omop)"
    )
    compare.add_argument(
        "new", type=Path, help="MEDS directory to check (e.g. from to-meds)"
    )
    compare.add_argument(
        "--ignore-columns",
        nargs="+",
        default=[],
        help="Columns to leave out of the comparison",
    )
    compare.add_argument(
        "--rows-per-bucket",
        type=int,
        default=2_000_000,
        help="Rows per side compared at once; lower to save memory",
    )
    compare.add_argument(
        "--max-examples",
        type=int,
        default=5,
        help="Example rows to show per difference",
    )
    compare.add_argument(
        "--empty-strings-as-null",
        action="store_true",
        help="Treat empty strings as null on both sides (the meds_etl cpp backend writes them as null)",
    )
    compare.set_defaults(handler=_compare)

    args = parser.parse_args()
    if args.command == "download" and not (
        args.patient_tables or args.non_patient_tables
    ):
        download.error("specify --patient-tables and/or --non-patient-tables")
    return args


def main() -> int:
    args = parse_args()
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format=LOG_FORMAT)
    return args.handler(args, level=level)


def _log_progress(
    rows: int,
    *,  # enforce kwargs
    table: str,
) -> None:
    log.info("%s: %s rows", table, f"{rows:,}")


def _download(args: argparse.Namespace, *, level: int) -> int:
    from .connection import connect
    from .download import download_patient_table, download_table

    with (
        log_to_file(args.output_dir, name="omop_utils", level=level),
        connect(credential_file=args.credentials, database=args.database) as conn,
    ):
        for table in args.patient_tables:
            download_patient_table(
                conn=conn,
                table=table,
                output_dir=args.output_dir,
                schema=args.schema,
                patients_per_chunk=args.patients_per_chunk,
                fetch_size=args.fetch_size,
                on_progress=partial(_log_progress, table=table),
            )
        for table in args.non_patient_tables:
            download_table(
                conn=conn,
                table=table,
                output_dir=args.output_dir,
                schema=args.schema,
                fetch_size=args.fetch_size,
            )
    return 0


def _to_meds(args: argparse.Namespace, *, level: int) -> int:
    from .to_meds import convert

    with log_to_file(args.dest, name="omop_to_meds", level=level):
        convert(
            args.src,
            args.dest,
            num_shards=args.num_shards,
            num_proc=args.num_proc,
            threads_per_proc=args.threads_per_proc,
            omop_version=args.omop_version,
        )
    return 0


def _compare(args: argparse.Namespace, *, level: int) -> int:
    from .compare_meds import compare

    report = compare(
        args.old,
        args.new,
        ignore_columns=args.ignore_columns,
        rows_per_bucket=args.rows_per_bucket,
        max_examples=args.max_examples,
        empty_strings_as_null=args.empty_strings_as_null,
    )
    return 0 if report.equivalent else 1


if __name__ == "__main__":
    raise SystemExit(main())
