"""
Standalone script to delete daily listen rows before a given date.

Use this to remove existing rows with day < cutoff from artist_daily_listens
and track_daily_listens (e.g. after deciding to only keep data from 2026-01-01
for short-term popularity modeling). Ingestion filtering is controlled by
run_pipeline.py --min-date-to-ingest; this script only prunes already-stored data.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_LOCAL_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_LOCAL_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_LOCAL_DIR))

from utils import connect_postgres, load_db_credentials


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete rows with day < BEFORE_DATE from artist_daily_listens and track_daily_listens."
    )
    parser.add_argument(
        "--before-date",
        type=str,
        required=True,
        metavar="YYYY-MM-DD",
        help="Remove rows with day strictly before this date (ISO).",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional path to .env. Defaults to project/.env when present.",
    )
    parser.add_argument(
        "--skip-dotenv",
        action="store_true",
        help="Skip loading environment variables from a .env file.",
    )
    parser.add_argument("--host", default=None, help="Postgres host override.")
    parser.add_argument("--port", default=None, help="Postgres port override.")
    parser.add_argument("--dbname", default=None, help="Postgres database override.")
    parser.add_argument("--user", default=None, help="Postgres user override.")
    parser.add_argument("--password", default=None, help="Postgres password override.")
    return parser.parse_args()


def maybe_load_dotenv(args: argparse.Namespace) -> Path | None:
    if args.skip_dotenv:
        return args.env_file
    env_path = args.env_file
    if env_path is None:
        env_path = PROJECT_LOCAL_DIR.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"Loaded env from: {env_path}")
        return env_path
    print(f"No .env found at: {env_path} (continuing with existing environment)")
    return None


def load_conn_params(args: argparse.Namespace, env_path: Path | None) -> dict[str, str | int]:
    conn_params = load_db_credentials(env_path=env_path)
    if args.host:
        conn_params["host"] = args.host
    if args.port:
        conn_params["port"] = int(args.port)
    if args.dbname:
        conn_params["dbname"] = args.dbname
    if args.user:
        conn_params["user"] = args.user
    if args.password is not None:
        conn_params["password"] = args.password
    return conn_params


def main() -> None:
    args = parse_args()
    env_path = maybe_load_dotenv(args)
    conn_params = load_conn_params(args, env_path)

    print(
        f"Target DB: {conn_params['host']}:{conn_params['port']}"
        f"/{conn_params['dbname']} as {conn_params['user']}"
    )
    print(f"Removing rows with day < {args.before_date} from artist_daily_listens and track_daily_listens.")

    conn = connect_postgres(conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM artist_daily_listens WHERE day < %s", (args.before_date,))
            #artist_deleted = cur.rowcount
            artist_deleted = cur.fetchone()[0]
            cur.execute("SELECT count(*)FROM track_daily_listens WHERE day < %s", (args.before_date,))
            #track_deleted = cur.rowcount
            track_deleted = cur.fetchone()[0]
        conn.commit()
        print(f"Deleted {artist_deleted:,} rows from artist_daily_listens.")
        print(f"Deleted {track_deleted:,} rows from track_daily_listens.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
