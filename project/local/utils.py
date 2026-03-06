# Utility file for managing aws credentials and resource connections

import os
import json
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# AWS Secrets Manager ARN for the RDS password (fallback when PGPASSWORD is not in .env)
_SECRET_ARN = "arn:aws:secretsmanager:us-east-1:549787090008:secret:rds!db-6323faa7-77d3-4952-af08-bcd6d623f642-g3XgW6"
_AWS_REGION = "us-east-1"


def load_db_credentials(env_path: Path | None = None) -> dict[str, str]:
    """
    Load Postgres credentials from a .env file (and optionally AWS Secrets Manager).

    Looks for the .env at *env_path*; defaults to ``<this-file's-parent-dir>/../.env``
    (i.e. the project root).

    Returns a dict with keys: host, port, dbname, user, password, sslrootcert.
    """
    if env_path is None:
        env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path, override=True)

    password = os.getenv("PGPASSWORD")
    if not password:
        import boto3
        sm = boto3.client("secretsmanager", region_name=_AWS_REGION)
        secret_str = sm.get_secret_value(SecretId=_SECRET_ARN)["SecretString"]
        password = json.loads(secret_str)["password"]

    return {
        "host": os.getenv("PGHOST") or "127.0.0.1",
        "port": int(os.getenv("PGPORT")) or 5433,
        "dbname": os.getenv("PGDATABASE") or "postgres",
        "user": os.getenv("PGUSER") or "postgres",
        "password": password,
        "sslrootcert": str(Path.home() / "rds-global-bundle.pem"),
    }


def connect_postgres(creds: dict[str, str] | None = None) -> psycopg2.extensions.connection:
    """
    Return a psycopg2 connection to the RDS Postgres instance (via SSH tunnel).

    If *creds* is not supplied, ``load_db_credentials()`` is called automatically.

    Make sure you have connected to the ec2 instance via an SSH tunnel first:
    ```
    ssh -i "<RELATIVE PATH TO .PEM KEY>" \\
        -L 5433:database-stall-munezero.cluster-chm317to06o1.us-east-1.rds.amazonaws.com:5432 \\
        ec2-user@98.89.230.64
    ```
    """
    if creds is None:
        creds = load_db_credentials()

    return psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["user"],
        password=creds["password"],
        sslmode="verify-ca",
        sslrootcert=creds["sslrootcert"],
    )

def _test_db_connection():
    conn = None
    try:
        conn = connect_postgres()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            print(cur.fetchone()[0])
    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    _test_db_connection()
