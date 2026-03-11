"""Recompute and populate ListenBrainz v2 daily stats tables.

This script fills:
- artist_daily_stats_v2  (reads from artist_daily_listens_v2, key: artist_id)
- track_daily_stats_v2   (reads from track_daily_listens_v2,  key: recording_id)

It reuses pipeline._compute_entity_stats(), so the rolling-window math is
identical to the v1 stats job.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

from pipeline import _compute_entity_stats
from utils import load_db_credentials


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute v2 artist/track daily stats in Postgres"
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
    parser.add_argument("--host",     default=None, help="Postgres host override.")
    parser.add_argument("--port",     default=None, help="Postgres port override.")
    parser.add_argument("--dbname",   default=None, help="Postgres database override.")
    parser.add_argument("--user",     default=None, help="Postgres user override.")
    parser.add_argument("--password", default=None, help="Postgres password override.")
    parser.add_argument(
        "--entity",
        choices=["artist", "track", "both"],
        default="both",
        help="Which entity to compute stats for (default: both).",
    )
    return parser.parse_args()


def maybe_load_dotenv(args: argparse.Namespace) -> None:
    if args.skip_dotenv:
        return
    env_path = args.env_file
    if env_path is None:
        env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"Loaded env from: {env_path}")
    else:
        print(f"No .env found at: {env_path} (continuing with existing env)")


def main() -> None:
    args = parse_args()
    maybe_load_dotenv(args)

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

    print(
        f"Target DB: {conn_params['host']}:{conn_params['port']}"
        f"/{conn_params['dbname']} as {conn_params['user']}"
    )

    # Lazy Spark import — same setup as pipeline.compute_daily_stats
    if os.name == "nt":
        hadoop_home = Path.home() / ".hadoop"
        (hadoop_home / "bin").mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("HADOOP_HOME", str(hadoop_home))
        os.environ.setdefault("hadoop.home.dir", str(hadoop_home))

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("ListenBrainz Stats v2")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    jdbc_url = (
        f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}"
        f"/{conn_params['dbname']}"
    )
    jdbc_props = {
        "user":     conn_params["user"],
        "password": conn_params["password"],
        "driver":   "org.postgresql.Driver",
    }

    try:
        if args.entity in ("artist", "both"):
            _compute_entity_stats(
                spark, jdbc_url, jdbc_props,
                "artist_daily_listens_v2",  # source
                "artist_daily_stats_v2",    # destination
                "artist_id",                # key col (v2 uses artist_id, not artist_mbid)
            )

        if args.entity in ("track", "both"):
            _compute_entity_stats(
                spark, jdbc_url, jdbc_props,
                "track_daily_listens_v2",   # source
                "track_daily_stats_v2",     # destination
                "recording_id",
            )
    finally:
        spark.stop()
        print("Spark session stopped.")

    print("v2 daily stats refresh complete.")


if __name__ == "__main__":
    main()
