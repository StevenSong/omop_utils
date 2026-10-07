# omop_utils

Utilities for downloading [OMOP CDM](https://ohdsi.github.io/CommonDataModel/) tables from a SQL Server database (e.g. AWS RDS) as CSV files.

- **Patient tables** (e.g. `person`, `measurement`) are written as chunks of whole patients, `<output-dir>/<table>/<first_id>-<last_id>.csv`. Downloads are resumable.
- **Non-patient tables** (e.g. `concept`, `cdm_source`) are written as a single `<output-dir>/<table>.csv`.
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
| `--patient-tables` | | Tables to download as per-patient CSV chunks |
| `--non-patient-tables` | | Tables to download as a single CSV |
| `--schema` | `cdm` | Schema containing the tables |
| `--patients-per-chunk` | `1000` | Patients per chunk file |
| `--fetch-size` | `50000` | Rows per Arrow batch |
| `-v`, `--verbose` | | Debug logging |

At least one of `--patient-tables` / `--non-patient-tables` is required.

### Resuming and re-running

- Chunks are written to `*.csv.tmp` and atomically renamed, so every `.csv` is complete. Interrupted runs resume after the highest finished person ID (this assumes the table doesn't change between runs).
- A finished patient table gets an empty `_COMPLETE` marker and is skipped on later runs; delete the table directory to re-download. A non-patient table is skipped if its `.csv` exists.
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
