import argparse
import logging
from pathlib import Path

from .connection import connect
from .download import download_patient_table, download_table

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download OMOP tables from SQL Server as CSVs."
    )
    parser.add_argument(
        "--patient-tables",
        nargs="+",
        default=[],
        help="Patient tables to download as per-patient CSV chunks, e.g. --patient-tables person measurement",
    )
    parser.add_argument(
        "--non-patient-tables",
        nargs="+",
        default=[],
        help="Non-patient tables to download as a single CSV, e.g. --non-patient-tables concept cdm_source",
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
        help="Root output directory; patient tables are written to <output-dir>/<table>/, non-patient tables to <output-dir>/<table>.csv",
    )
    parser.add_argument("--patients-per-chunk", type=int, default=1_000)
    parser.add_argument("--fetch-size", type=int, default=50_000)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    if not (args.patient_tables or args.non_patient_tables):
        parser.error("specify --patient-tables and/or --non-patient-tables")
    return args


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    with connect(credential_file=args.credentials, database=args.database) as conn:
        for table in args.patient_tables:
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
        for table in args.non_patient_tables:
            download_table(
                conn=conn,
                table=table,
                output_dir=args.output_dir,
                schema=args.schema,
                fetch_size=args.fetch_size,
            )


if __name__ == "__main__":
    main()
