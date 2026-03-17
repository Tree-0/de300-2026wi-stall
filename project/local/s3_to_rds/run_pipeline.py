from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

PROJECT_LOCAL_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_LOCAL_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_LOCAL_DIR))

from pipeline import _compute_entity_stats
from pipeline_recording_id import (
    _load_alias_map_from_db,
    ensure_tables,
    parse_dump,
    print_parser_details,
    upsert,
)
from utils import (
    connect_postgres,
    download_s3_dump,
    get_s3_client,
    list_s3_artifacts,
    load_db_credentials,
)


DUMP_ID_RE = re.compile(r"dump-(\d+)-")
DEFAULT_BUCKET = "stall-munezero-final-project"
DEFAULT_PREFIX = "listenbrainz/incremental/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Batch-ingest ListenBrainz dumps from S3 into the v2 canonical tables, "
            "update ingestion_state, and optionally refresh daily stats."
        )
    )
    parser.add_argument("--n-dumps", type=int, required=True, help="Number of new dumps to ingest.")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="S3 bucket containing ListenBrainz dumps.")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="S3 prefix that contains dump files.")
    parser.add_argument(
        "--tmp-dir",
        type=Path,
        default=Path("./tmp_s3_to_rds_v2"),
        help="Local directory used for downloaded dump files.",
    )
    parser.add_argument(
        "--start-after-dump-id",
        type=int,
        default=None,
        help="Override ingestion_state and start after this dump id.",
    )
    parser.add_argument(
        "--max-lines",
        type=int,
        default=0,
        help="Optional parse cap for debugging; 0 means full dump.",
    )
    parser.add_argument(
        "--min-date-to-ingest",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Only ingest daily listens with day >= this date (ISO). Omit to ingest all.",
    )
    parser.add_argument(
        "--skip-stats",
        action="store_true",
        help="Skip the artist/track daily stats refresh after ingestion.",
    )
    parser.add_argument(
        "--stats-entity",
        choices=["artist", "track", "both"],
        default="both",
        help="Which stats tables to refresh after ingest.",
    )
    parser.add_argument(
        "--spark-driver-memory",
        default="4g",
        help="Spark driver memory for the stats refresh job.",
    )
    parser.add_argument(
        "--keep-temp-files",
        action="store_true",
        help="Keep downloaded dump files after each ingest for debugging.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional path to a .env file. Defaults to project/.env when present.",
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


def extract_dump_id(value: str | Path) -> int | None:
    match = DUMP_ID_RE.search(str(value))
    if not match:
        return None
    return int(match.group(1))


def ensure_ingestion_state(conn) -> None:
    prev_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_state (
                    id INT PRIMARY KEY DEFAULT 1,
                    last_dump_id BIGINT,
                    last_dump_key TEXT,
                    last_dump_path TEXT,
                    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    CONSTRAINT singleton_row CHECK (id = 1)
                )
                """
            )
            cur.execute("ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS last_dump_id BIGINT")
            cur.execute("ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS last_dump_key TEXT")
            cur.execute("ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS last_dump_path TEXT")
            cur.execute(
                "ALTER TABLE ingestion_state ADD COLUMN IF NOT EXISTS loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()"
            )
            cur.execute(
                "INSERT INTO ingestion_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING"
            )
    finally:
        conn.autocommit = prev_autocommit


def get_last_dump_id(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(NULLIF(last_dump_id, '')::bigint, 0) "
            "FROM ingestion_state WHERE id = 1"
        )
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def update_ingestion_state(conn, dump_key: str, dump_path: str) -> None:
    dump_id = extract_dump_id(dump_key)
    if dump_id is None:
        raise ValueError(f"Could not extract dump id from key: {dump_key}")

    with conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion_state
               SET last_dump_id = %s,
                   last_dump_key = %s,
                   last_dump_path = %s,
                   loaded_at = now()
             WHERE id = 1
            """,
            (dump_id, dump_key, dump_path),
        )


def select_next_dump_keys(all_keys: list[str], start_after_dump_id: int, n_dumps: int) -> list[str]:
    keyed = []
    for key in all_keys:
        dump_id = extract_dump_id(key)
        if dump_id is None:
            continue
        if dump_id > start_after_dump_id:
            keyed.append((dump_id, key))
    keyed.sort(key=lambda item: item[0])
    return [key for _, key in keyed[:n_dumps]]


def cleanup_local_file(path: Path, keep_temp_files: bool) -> None:
    if keep_temp_files:
        return
    if path.exists():
        path.unlink()
        print(f"Deleted temp file: {path}")
    parent = path.parent
    if parent.exists() and not any(parent.iterdir()):
        parent.rmdir()
        print(f"Removed empty dir: {parent}")


def refresh_daily_stats(conn_params: dict[str, str | int], entity: str, spark_driver_memory: str) -> None:
    if os.name == "nt":
        hadoop_home = Path.home() / ".hadoop"
        (hadoop_home / "bin").mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("HADOOP_HOME", str(hadoop_home))
        os.environ.setdefault("hadoop.home.dir", str(hadoop_home))

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("ListenBrainz S3->RDS v2 stats refresh")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .config("spark.driver.memory", spark_driver_memory)
        .getOrCreate()
    )

    jdbc_url = (
        f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}"
        f"/{conn_params['dbname']}"
    )
    jdbc_props = {
        "user": conn_params["user"],
        "password": conn_params["password"],
        "driver": "org.postgresql.Driver",
    }

    try:
        if entity in ("artist", "both"):
            _compute_entity_stats(
                spark,
                jdbc_url,
                jdbc_props,
                "artist_daily_listens",
                "artist_daily_stats",
                "artist_id",
            )
        if entity in ("track", "both"):
            _compute_entity_stats(
                spark,
                jdbc_url,
                jdbc_props,
                "track_daily_listens",
                "track_daily_stats",
                "recording_id",
            )
    finally:
        spark.stop()
        print("Spark session stopped.")


def ingest_next_n_dumps(args: argparse.Namespace, conn_params: dict[str, str | int]) -> dict[str, object]:
    bootstrap_conn = connect_postgres(conn_params)
    try:
        ensure_tables(bootstrap_conn)
        ensure_ingestion_state(bootstrap_conn)
        stored_last_dump_id = get_last_dump_id(bootstrap_conn)
    finally:
        bootstrap_conn.close()

    start_after_dump_id = args.start_after_dump_id
    if start_after_dump_id is None:
        start_after_dump_id = stored_last_dump_id

    s3_client = get_s3_client()
    all_keys = list_s3_artifacts(args.bucket, args.prefix, s3_client=s3_client)
    selected_keys = select_next_dump_keys(all_keys, start_after_dump_id, args.n_dumps)

    print(f"Stored ingestion_state last_dump_id: {stored_last_dump_id}")
    print(f"Using start-after dump id: {start_after_dump_id}")
    print(f"Found {len(all_keys)} S3 keys under s3://{args.bucket}/{args.prefix}")

    if not selected_keys:
        print("No new dumps to ingest.")
        return {
            "start_after_dump_id": start_after_dump_id,
            "n_ingested": 0,
            "dump_ids": [],
            "total_rows_parsed": 0,
        }

    print(f"Will ingest {len(selected_keys)} dump(s):")
    for key in selected_keys:
        print(f"  {key}")

    args.tmp_dir.mkdir(parents=True, exist_ok=True)

    total_rows_parsed = 0
    dump_ids: list[int] = []
    alias_conn = connect_postgres(conn_params)
    try:
        for idx, dump_key in enumerate(selected_keys, start=1):
            dump_id = extract_dump_id(dump_key)
            print(f"\n{'=' * 72}")
            print(f"[{idx}/{len(selected_keys)}] dump_id={dump_id} -> {dump_key}")
            print(f"{'=' * 72}")

            start = time.perf_counter()
            local_path = download_s3_dump(
                args.bucket,
                dump_key,
                local_dir=args.tmp_dir,
                s3_client=s3_client,
            )

            try:
                alias_to_mbid = _load_alias_map_from_db(alias_conn)
                track_daily, artist_daily, track_info, artist_info, summary = parse_dump(
                    local_path,
                    max_lines=args.max_lines,
                    alias_to_mbid=alias_to_mbid,
                    min_date_to_ingest=args.min_date_to_ingest,
                )
                print_parser_details(summary)
                total_rows_parsed += int(summary.get("lines_parsed", 0))

                upsert_conn = connect_postgres(conn_params)
                try:
                    upsert(
                        upsert_conn,
                        track_daily,
                        artist_daily,
                        track_info,
                        artist_info,
                        dump_path=dump_key,
                    )
                    update_ingestion_state(upsert_conn, dump_key=dump_key, dump_path=dump_key)
                finally:
                    upsert_conn.close()

                elapsed = time.perf_counter() - start
                dump_ids.append(int(dump_id))
                print(f"Finished dump_id={dump_id} in {elapsed:.1f}s")
            finally:
                cleanup_local_file(local_path, keep_temp_files=args.keep_temp_files)
    finally:
        alias_conn.close()

    if dump_ids and not args.skip_stats:
        print(f"\nRefreshing v2 stats tables for entity='{args.stats_entity}'...")
        refresh_daily_stats(conn_params, entity=args.stats_entity, spark_driver_memory=args.spark_driver_memory)

    return {
        "start_after_dump_id": start_after_dump_id,
        "n_ingested": len(dump_ids),
        "dump_ids": dump_ids,
        "total_rows_parsed": total_rows_parsed,
    }


def main() -> None:
    args = parse_args()
    env_path = maybe_load_dotenv(args)
    conn_params = load_conn_params(args, env_path)

    print(
        f"Target DB: {conn_params['host']}:{conn_params['port']}"
        f"/{conn_params['dbname']} as {conn_params['user']}"
    )

    result = ingest_next_n_dumps(args, conn_params)
    print("\n" + "=" * 72)
    print("Batch ingest summary")
    print("=" * 72)
    print(f"Start-after dump id: {result['start_after_dump_id']}")
    print(f"Dumps ingested:      {result['n_ingested']}")
    print(f"Dump ids:            {result['dump_ids']}")
    print(f"Rows parsed total:   {result['total_rows_parsed']:,}")


if __name__ == "__main__":
    main()