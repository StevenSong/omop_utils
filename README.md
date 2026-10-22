# omop_utils

Utilities for downloading [OMOP CDM](https://ohdsi.github.io/CommonDataModel/) tables from a SQL Server database (e.g. AWS RDS) as zstd-compressed Parquet files.

- **Patient tables** (e.g. `person`, `measurement`) are written as chunks of whole patients, `<output-dir>/<table>/<first_id>-<last_id>.parquet`. Downloads are resumable.
- **Non-patient tables** (e.g. `concept`, `cdm_source`) are written as a single `<output-dir>/<table>.parquet`.
- Column types come from the database (e.g. dates, timestamps, decimals), so every file of a table has the same schema.
- Connections are refused if the account has write permissions on the database (read-only check).

## Setup

Requires [`uv`](https://docs.astral.sh/uv/) and Python 3.14 (pinned in `.python-version`; `uv` will fetch it if needed).

```bash
uv sync
```

This creates `.venv` and installs the locked dependencies (`mssql-python`, `pyarrow`, plus the `dev` group: `pre-commit` and `jupyterlab`). Optionally install the git hooks:

```bash
uv run pre-commit install
```

## CA certificate bundle (AWS RDS)

The connection requires a verified TLS certificate (`trustservercertificate="no"`). AWS RDS uses its own CA, so it has to be trusted. In ephemeral workspaces where installing certs system-wide is awkward, bundle the RDS certs with your existing CA certs and point `SSL_CERT_FILE` at the result. [`utils/prepare-cert.ipynb`](utils/prepare-cert.ipynb) does this.

1. Download the certificate bundle for your region (e.g. `us-east-1-bundle.pem`) from the [AWS docs](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html#UsingWithRDS.SSL.CertificatesDownload) and place it in the directory you run the notebook from. Edit the filename in the notebook if yours differs.
2. Run the notebook:

   ```bash
   uv run jupyter lab utils/prepare-cert.ipynb
   ```

   The notebook concatenates `certifi`'s CA bundle with the RDS bundle and writes `ca-bundle.pem`.
3. Export the bundle before downloading:

   ```bash
   export SSL_CERT_FILE="$PWD/ca-bundle.pem"
   ```

If you see a `certificate verify failed` error, `omop_utils` raises a hint to check this step.

## Credentials

Create a JSON file with the connection details (keep it out of version control):

```json
{
  "hostname": "my-db.xxxxxxxx.us-east-1.rds.amazonaws.com",
  "port": 1433,
  "username": "readonly_user",
  "password": "..."
}
```

## Usage

Run the CLI as a module:

```bash
uv run python -m omop_utils \
  --credentials creds.json \
  --database my_omop_db \
  --output-dir out \
  --patient-tables person measurement \
  --non-patient-tables concept cdm_source
```

| Option | Default | Description |
| --- | --- | --- |
| `--credentials` | required | JSON credential file |
| `--database` | required | Database name |
| `--output-dir` | required | Root output directory |
| `--patient-tables` | | Tables to download as per-patient Parquet chunks |
| `--non-patient-tables` | | Tables to download as a single Parquet file |
| `--schema` | `cdm` | Schema containing the tables |
| `--patients-per-chunk` | `1000` | Patients per chunk file |
| `--fetch-size` | `50000` | Rows per Arrow batch |
| `-v`, `--verbose` | | Debug logging |

At least one of `--patient-tables` / `--non-patient-tables` is required.

### Resuming and re-running

- Chunks are written to `*.parquet.tmp` and atomically renamed, so every `.parquet` is complete. Interrupted runs resume after the highest finished person ID (this assumes the table doesn't change between runs).
- A finished patient table gets an empty `_COMPLETE` marker and is skipped on later runs; delete the table directory to re-download. A non-patient table is skipped if its `.parquet` exists.
- Patient tables must be clustered on `person_id` (checked before download) so chunk queries can seek in order.

### Python API

```python
from omop_utils.connection import connect
from omop_utils.download import download_patient_table, download_table

with connect(credential_file="creds.json", database="my_omop_db") as conn:
    download_patient_table(
        conn, table="measurement", output_dir="out", patients_per_chunk=5_000
    )
    download_table(conn, table="concept", output_dir="out")
```

## Converting to MEDS

`omop-to-meds` converts a download to [MEDS](https://github.com/Medical-Event-Data-Standard/meds) 0.3.3. It is a port of `meds_etl_omop` from [meds_etl](https://github.com/Medical-Event-Data-Standard/meds_etl) and maps the same tables to the same events. The difference is in how the work is split, which keeps memory bounded:

- Each output shard is a contiguous `person_id` range. A shard reads only the chunks (and, via Parquet statistics, row groups) that overlap its range, then maps, sorts and writes them as `data/data_<shard>.parquet`. There is no MEDS Unsorted intermediate and no second shuffle.
- Concept IDs are mapped with a polars lookup that is loaded once per worker, not with pickled Python dicts copied into every task.
- Shards are written atomically. Re-running with the same arguments resumes and skips shards that already exist; changing `--num-shards` requires a fresh output directory.

```bash
uv run omop-to-meds out meds --num-shards 100 --num-proc 4
```

| Option | Default | Description |
| --- | --- | --- |
| `--num-shards` | `100` | Number of output shards (person ID ranges, split evenly by person count) |
| `--num-proc` | `1` | Shards built in parallel |
| `--threads-per-proc` | CPUs / `--num-proc` | Polars threads per process |
| `--omop-version` | `5.4` | `5.3` or `5.4` (`discharge_to_concept_id` vs `discharged_to_concept_id`) |

Peak memory is roughly `--num-proc` × (one shard's events, while sorting) plus the concept lookup per process. If a run runs out of memory, raise `--num-shards` or lower `--num-proc`. The `person` table and the `concept` table are required. Missing event tables are skipped.

Shard membership differs from `meds_etl_omop`, which assigns subjects to shards by hash, so check the output with `meds-compare` instead of comparing files directly.

## Checking equivalence

`meds-compare` checks that two MEDS datasets contain the same events, however their subjects are sharded:

```bash
uv run meds-compare meds_etl_output meds
```

It checks that:

- `metadata/dataset.json` matches, apart from `etl_name` / `etl_version`;
- `metadata/codes.parquet` holds the same rows, in any order;
- the data files have the same columns and types;
- each dataset is valid MEDS: no null `subject_id` / `code`, files sorted by `(subject_id, time)`, and each subject in exactly one file;
- every subject has the same multiset of event rows in both datasets.

Row order within a timestamp is not compared. The meds_etl polars backend breaks ties by `code`, and the cpp backend doesn't break them at all.

Events are compared in buckets of contiguous subject IDs (`--rows-per-bucket`, default 2M rows per side), so memory stays bounded. When rows differ, it reports which subjects and example rows. The exit code is 0 only if the datasets are equivalent.

The meds_etl cpp backend writes empty strings as null. To compare against its output, pass `--empty-strings-as-null`; the report suggests this when it is the only difference. `--ignore-columns` leaves columns out of the comparison entirely.

## Tests

```bash
uv run pytest
```

The tests cover the per-table event mapping and shard file selection. To check a conversion end to end, run `meds-compare` against a `meds_etl_omop` output of the same data.
