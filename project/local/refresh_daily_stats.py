"""Recompute and populate ListenBrainz daily stats tables.

This script fills:
- artist_daily_stats
- track_daily_stats

It reuses pipeline.compute_daily_stats(), so the math is identical to the
pipeline/notebook flow.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from pipeline import compute_daily_stats
from utils import load_db_credentials


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute artist/track daily stats in Postgres"
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional path to a .env file (default: project/.env if present).",
    )
    parser.add_argument(
        "--skip-dotenv",
        action="store_true",
        help="Skip loading environment variables from .env.",
    )
    parser.add_argument("--host", default=None, help="Postgres host override.")
    parser.add_argument("--port", default=None, help="Postgres port override.")
    parser.add_argument("--dbname", default=None, help="Postgres database override.")
    parser.add_argument("--user", default=None, help="Postgres user override.")
    parser.add_argument("--password", default=None, help="Postgres password override.")
    return parser.parse_args()


def maybe_load_dotenv(args: argparse.Namespace) -> None:
    if args.skip_dotenv:
        return

    env_path = args.env_file
    if env_path is None:
        # project/local/refresh_daily_stats.py -> project/.env
        env_path = Path(__file__).resolve().parents[1] / ".env"

    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"Loaded env from: {env_path}")
    else:
        print(f"No .env found at: {env_path} (continuing with existing env)")


def build_conn_params(args: argparse.Namespace) -> dict:
    conn_params = load_db_credentials()

    if args.host:
        conn_params["host"] = args.host
    if args.port:
        conn_params["port"] = args.port
    if args.dbname:
        conn_params["dbname"] = args.dbname
    if args.user:
        conn_params["user"] = args.user
    if args.password is not None:
        conn_params["password"] = args.password

    return conn_params


def main() -> None:
    args = parse_args()
    maybe_load_dotenv(args)

    conn_params = build_conn_params(args)
    print(
        "Target DB: "
        f"{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']} "
        f"as {conn_params['user']}"
    )

    compute_daily_stats(conn_params)
    print("Daily stats refresh complete.")


if __name__ == "__main__":
    main()
