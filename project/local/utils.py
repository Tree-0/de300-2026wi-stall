# Utility file for managing aws credentials and resource connections

import os
import json
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# AWS Secrets Manager ARN for the RDS password (fallback when PGPASSWORD is not in .env)
_SECRET_ARN = "arn:aws:secretsmanager:us-east-1:549787090008:secret:rds!db-6323faa7-77d3-4952-af08-bcd6d623f642-g3XgW6"
_AWS_REGION = "us-east-1"

# ---------------------------------------------------------------------------
# Credentials / Connections
# ---------------------------------------------------------------------------

def load_db_credentials(env_path: Path | None = None) -> dict[str, str | int]:
    """
    Load Postgres credentials from a .env file (and optionally AWS Secrets Manager).

    Looks for the .env at *env_path*; defaults to ``<this-file's-parent-dir>/../.env``
    (i.e. the project root).

    Returns a dict with keys: host, port, dbname, user, password, sslrootcert.
    """
    if env_path is None:
        env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path, override=True)

    env_host = os.getenv("PGHOST")
    env_port = os.getenv("PGPORT")
    env_dbname = os.getenv("PGDATABASE")
    env_user = os.getenv("PGUSER")
    env_password = os.getenv("PGPASSWORD")

    secret: dict = {}
    if not env_password or not env_user:
        try:
            import boto3

            sm = boto3.client("secretsmanager", region_name=_AWS_REGION)
            secret_str = sm.get_secret_value(SecretId=_SECRET_ARN)["SecretString"]
            secret = json.loads(secret_str)
        except Exception:
            # Fall back to env/default values when secrets access is unavailable.
            secret = {}

    host = env_host or secret.get("host") or "127.0.0.1"

    port_raw = env_port or secret.get("port") or "5433"
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        port = 5433

    dbname = env_dbname or secret.get("dbname") or secret.get("database") or "postgres"
    user = env_user or secret.get("username") or secret.get("user") or "postgres"
    password = env_password or secret.get("password")

    if not password:
        raise RuntimeError(
            "Database password not found. Set PGPASSWORD in .env or allow Secrets Manager access."
        )

    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
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

    # Use appropriate SSL mode based on connection type
    host = str(creds["host"]).strip('"')
    is_localhost = host in {"127.0.0.1", "localhost", "::1"}
    
    if is_localhost:
        # SSH tunnel is already encrypted, so SSL not needed
        return psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["dbname"],
            user=creds["user"],
            password=creds["password"],
            sslmode="prefer",  # Try SSL but fall back to non-SSL
        )
    else:
        # Direct RDS connection - check if certificate bundle exists
        cert_path = Path(creds["sslrootcert"])
        if cert_path.exists():
            # Use certificate verification (most secure)
            return psycopg2.connect(
                host=creds["host"],
                port=creds["port"],
                dbname=creds["dbname"],
                user=creds["user"],
                password=creds["password"],
                sslmode="verify-ca",
                sslrootcert=str(cert_path),
            )
        else:
            # Certificate not found - use SSL without verification
            # (secure enough for AWS internal connections)
            return psycopg2.connect(
                host=creds["host"],
                port=creds["port"],
                dbname=creds["dbname"],
                user=creds["user"],
                password=creds["password"],
                sslmode="require",  # Enforce SSL but don't verify cert
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

# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def get_s3_client(region: str | None = None):
    """Return a boto3 S3 client using the default credential chain."""
    import boto3
    return boto3.client("s3", region_name=region or _AWS_REGION)


def list_s3_artifacts(bucket: str, prefix: str, s3_client=None) -> list[str]:
    """
    List all object keys under *prefix* in *bucket*.

    Uses the paginator so it works for >1 000 objects.
    Returns a list of full S3 keys (strings).
    """
    if s3_client is None:
        s3_client = get_s3_client()

    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_s3_dump(bucket: str, key: str, local_dir: Path | str | None = None,
                     s3_client=None) -> Path:
    """
    Download an S3 object to a local directory and return the local Path.

    *local_dir* defaults to ``./tmp_listenbrainz``.  The directory is created
    if it doesn't exist.  Cleanup is the caller's responsibility.
    """
    if s3_client is None:
        s3_client = get_s3_client()

    if local_dir is None:
        local_dir = Path("./tmp_listenbrainz")
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    filename = key.rsplit("/", 1)[-1]
    local_path = local_dir / filename

    print(f"Downloading s3://{bucket}/{key} → {local_path} ...")
    s3_client.download_file(bucket, key, str(local_path))
    print(f"Done ({local_path.stat().st_size / 1e6:.1f} MB)")
    return local_path

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _test_db_connection()
