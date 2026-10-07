import json
from os import PathLike

import mssql_python


def connect(
    *,  # enforce kwargs
    credential_file: PathLike,
    database: str,
    check_read_only: bool = True,
) -> mssql_python.Connection:
    with open(credential_file, "r") as f:
        config = json.load(f)

    try:
        conn = mssql_python.connect(
            server=config["hostname"] + "," + str(config["port"]),
            database=database,
            uid=config["username"],
            pwd=config["password"],
            encrypt="yes",
            trustservercertificate="no",  # require cert
        )
    except mssql_python.OperationalError as e:
        if "certificate verify failed" in str(e):
            raise RuntimeError(
                "Did you load the AWS RDS cert? If you have a merged cert file, set it using the SSL_CERT_FILE env var"
            ) from e
        raise

    if check_read_only:
        try:
            assert_read_only(conn)
        except:
            conn.close()
            raise

    return conn


def assert_read_only(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT
                HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'INSERT'),
                HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'UPDATE'),
                HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'DELETE'),
                HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'ALTER'),
                HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE')
        """)
        row = cursor.fetchone()
    if any(row):
        raise RuntimeError(
            "Connection has write permissions; refusing to run as read-only."
        )
