"""AWS Lambda S3 trigger for ListenBrainz v2 ingestion.

Flow per uploaded *.tar.zst object:
1. Download object from S3 to /tmp
2. Parse listens using pipeline_recording_id_v2.parse_dump()
3. Upsert into *_v2 info + daily tables
4. Recompute *_daily_stats_v2 in Postgres (SQL window functions)

Environment variables (recommended):
- SOURCE_BUCKET=stall-munezero-final-project
- KEY_SUFFIX=.tar.zst
- MAX_LINES=0                        (0 = no cap)
- MAX_OBJECT_MB=8192                 (guardrail before /tmp download)
- REFRESH_STATS_AFTER_INGEST=true

DB credentials are resolved via utils.load_db_credentials(), which supports:
- PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
- AWS Secrets Manager fallback (same behavior as local pipeline)
"""

from __future__ import annotations

import logging
import os
import traceback
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import unquote_plus

import boto3

from pipeline_recording_id_v2 import ensure_v2_tables, parse_dump, upsert_v2
from utils import connect_postgres, load_db_credentials

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _refresh_entity_stats_sql(cur, listens_table: str, stats_table: str, id_col: str) -> int:
    """Recompute one v2 stats table from its corresponding listens table."""
    # Full refresh is deterministic and idempotent, at the cost of runtime.
    cur.execute(f"TRUNCATE TABLE {stats_table}")

    cur.execute(
        f"""
        WITH daily AS (
            SELECT
                day::date AS day,
                {id_col} AS id_value,
                SUM(listen_count)::bigint AS listen_count
            FROM {listens_table}
            GROUP BY day::date, {id_col}
        ),
        base AS (
            SELECT
                day,
                id_value,
                listen_count,
                SUM(listen_count) OVER (
                    PARTITION BY id_value
                    ORDER BY day
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_listen_count,
                SUM(listen_count) OVER (
                    PARTITION BY id_value
                    ORDER BY day
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS listen_count_past_7_days,
                SUM(listen_count) OVER (
                    PARTITION BY id_value
                    ORDER BY day
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS listen_count_past_30_days
            FROM daily
        ),
        enriched AS (
            SELECT
                day,
                id_value,
                cumulative_listen_count,
                listen_count_past_7_days,
                listen_count_past_30_days,
                CASE
                    WHEN LAG(cumulative_listen_count) OVER (
                        PARTITION BY id_value
                        ORDER BY day
                    ) > 0
                    THEN listen_count::double precision
                         / LAG(cumulative_listen_count) OVER (
                             PARTITION BY id_value
                             ORDER BY day
                         )
                    ELSE NULL
                END AS growth_rate
            FROM base
        ),
        ranked AS (
            SELECT
                day,
                id_value,
                PERCENT_RANK() OVER (
                    PARTITION BY day
                    ORDER BY growth_rate ASC NULLS FIRST
                ) AS growth_percentile,
                cumulative_listen_count,
                listen_count_past_7_days,
                PERCENT_RANK() OVER (
                    PARTITION BY day
                    ORDER BY listen_count_past_7_days
                ) AS listen_pctl_past_7_days,
                listen_count_past_30_days,
                PERCENT_RANK() OVER (
                    PARTITION BY day
                    ORDER BY listen_count_past_30_days
                ) AS listen_pctl_past_30_days
            FROM enriched
        )
        INSERT INTO {stats_table} (
            day,
            {id_col},
            growth_percentile,
            cumulative_listen_count,
            listen_count_past_7_days,
            listen_pctl_past_7_days,
            listen_count_past_30_days,
            listen_pctl_past_30_days
        )
        SELECT
            day,
            id_value,
            growth_percentile,
            cumulative_listen_count,
            listen_count_past_7_days,
            listen_pctl_past_7_days,
            listen_count_past_30_days,
            listen_pctl_past_30_days
        FROM ranked
        """
    )

    cur.execute(f"SELECT COUNT(*) FROM {stats_table}")
    return int(cur.fetchone()[0])


def refresh_v2_stats_sql(conn) -> dict[str, int]:
    """Recompute artist_daily_stats_v2 and track_daily_stats_v2 via SQL."""
    conn.autocommit = False
    with conn, conn.cursor() as cur:
        artist_rows = _refresh_entity_stats_sql(
            cur,
            listens_table="artist_daily_listens_v2",
            stats_table="artist_daily_stats_v2",
            id_col="artist_id",
        )
        track_rows = _refresh_entity_stats_sql(
            cur,
            listens_table="track_daily_listens_v2",
            stats_table="track_daily_stats_v2",
            id_col="recording_id",
        )

    return {
        "artist_daily_stats_v2": artist_rows,
        "track_daily_stats_v2": track_rows,
    }


def _build_db_creds() -> dict:
    """Load credentials and apply optional Lambda env overrides."""
    creds = load_db_credentials()

    overrides = {
        "PGHOST": "host",
        "PGPORT": "port",
        "PGDATABASE": "dbname",
        "PGUSER": "user",
        "PGPASSWORD": "password",
        "DB_SSLROOTCERT": "sslrootcert",
    }
    for env_name, cred_key in overrides.items():
        value = os.getenv(env_name)
        if value:
            creds[cred_key] = int(value) if cred_key == "port" else value

    return creds


def _download_s3_object(bucket: str, key: str) -> Path:
    """Download one S3 object to /tmp and return local path."""
    suffix = ".tar.zst" if key.endswith(".tar.zst") else ".bin"
    with NamedTemporaryFile(delete=False, suffix=suffix, dir="/tmp") as fh:
        local_path = Path(fh.name)

    LOGGER.info("Downloading s3://%s/%s -> %s", bucket, key, local_path)
    S3_CLIENT.download_file(bucket, key, str(local_path))
    return local_path


def _process_object(bucket: str, key: str) -> dict:
    """Process one S3 object: parse, upsert v2, and optionally refresh stats."""
    max_object_mb = _env_float("MAX_OBJECT_MB", 8192.0)
    max_bytes = int(max_object_mb * 1024 * 1024)

    head = S3_CLIENT.head_object(Bucket=bucket, Key=key)
    object_bytes = int(head.get("ContentLength", 0))
    if object_bytes > max_bytes:
        raise RuntimeError(
            f"Object too large for configured limit: {object_bytes} bytes > {max_bytes} bytes"
        )

    local_path = _download_s3_object(bucket, key)
    try:
        max_lines = _env_int("MAX_LINES", 0)
        track_daily, artist_daily, track_info, artist_info, summary = parse_dump(
            local_path,
            max_lines=max_lines,
        )

        db_creds = _build_db_creds()
        conn = connect_postgres(db_creds)
        try:
            ensure_v2_tables(conn)
            upsert_v2(
                conn,
                track_daily=track_daily,
                artist_daily=artist_daily,
                track_info=track_info,
                artist_info=artist_info,
                dump_path=f"s3://{bucket}/{key}",
            )

            stats_counts = {}
            if _env_bool("REFRESH_STATS_AFTER_INGEST", True):
                LOGGER.info("Refreshing v2 stats tables using SQL window functions...")
                stats_counts = refresh_v2_stats_sql(conn)
        finally:
            conn.close()

        result = {
            "bucket": bucket,
            "key": key,
            "bytes": object_bytes,
            "lines_parsed": int(summary.get("lines_parsed", 0)),
            "usable_rows": int(summary.get("usable_rows", 0)),
            "unique_tracks_v2": int(summary.get("unique_tracks_v2", 0)),
            "unique_artists_v2": int(summary.get("unique_artists_v2", 0)),
            "track_day_rows": len(track_daily),
            "artist_day_rows": len(artist_daily),
            "stats_rows": stats_counts,
        }
        LOGGER.info("Processing complete: %s", result)
        return result
    finally:
        try:
            local_path.unlink(missing_ok=True)
        except Exception:
            LOGGER.warning("Failed to cleanup temp file: %s", local_path)


def lambda_handler(event, context):
    """Lambda entrypoint for S3 ObjectCreated notifications."""
    expected_bucket = os.getenv("SOURCE_BUCKET", "stall-munezero-final-project")
    expected_suffix = os.getenv("KEY_SUFFIX", ".tar.zst")

    records = event.get("Records", []) if isinstance(event, dict) else []
    if not records:
        return {
            "statusCode": 400,
            "message": "No S3 records in event",
            "results": [],
        }

    results = []
    errors = []

    for record in records:
        try:
            if record.get("eventSource") != "aws:s3":
                continue

            event_name = record.get("eventName", "")
            if not event_name.startswith("ObjectCreated:"):
                continue

            bucket = record["s3"]["bucket"]["name"]
            key = unquote_plus(record["s3"]["object"]["key"])

            if expected_bucket and bucket != expected_bucket:
                LOGGER.info("Skipping object in non-target bucket: %s/%s", bucket, key)
                continue
            if expected_suffix and not key.endswith(expected_suffix):
                LOGGER.info("Skipping object with non-target suffix: %s", key)
                continue

            result = _process_object(bucket, key)
            results.append({"status": "ok", **result})
        except Exception as exc:
            err = {
                "status": "error",
                "error": str(exc),
                "traceback": traceback.format_exc(limit=8),
                "record": record,
            }
            LOGGER.exception("Failed processing S3 record")
            results.append(err)
            errors.append(err)

    response = {
        "statusCode": 200 if not errors else 500,
        "processed": len([r for r in results if r.get("status") == "ok"]),
        "failed": len(errors),
        "results": results,
    }

    # Raise to let Lambda async retry failed S3 events.
    if errors:
        raise RuntimeError(f"{len(errors)} record(s) failed. See CloudWatch logs.")

    return response
