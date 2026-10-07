import argparse
import logging
from pathlib import Path

from .connection import connect
from .download import download_patient_table

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download OMOP tables from SQL Server as per-patient CSV chunks."
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        required=True,
        help="Tables to download, e.g. --tables person measurement",
    )
    parser.add_argument(
        "--credentials", type=Path, required=True, help="JSON credential file"
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--schema", default="cdm")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Root output directory, each table is written to <output-dir>/<table>/",
    )
    parser.add_argument("--patients-per-chunk", type=int, default=1_000)
    parser.add_argument("--fetch-size", type=int, default=50_000)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    with connect(credential_file=args.credentials, database=args.database) as conn:
        for table in args.tables:
            download_patient_table(
                conn=conn,
                table=table,
                output_dir=args.output_dir,
                schema=args.schema,
                patients_per_chunk=args.patients_per_chunk,
                fetch_size=args.fetch_size,
                on_progress=lambda rows, t=table: log.info(
                    "%s: %s rows", t, f"{rows:,}"
                ),
            )


if __name__ == "__main__":
    main()
