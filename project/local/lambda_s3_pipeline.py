"""AWS Lambda S3 trigger for ListenBrainz ingestion (ingest-only).

Flow per uploaded *.tar.zst object:
1. Download object from S3 to /tmp
2. Parse listens using pipeline_recording_id.parse_dump()
3. Upsert into canonical tables:
   - artist_info
   - track_info
   - artist_daily_listens
   - track_daily_listens

This Lambda intentionally does NOT recompute daily stats tables. Stats refresh is
meant to run later, to cover multiple ingestions at once (e.g. on EC2) via project/local/refresh_daily_stats.py.

Environment variables:
- SOURCE_BUCKET=stall-munezero-final-project
- KEY_SUFFIX=.tar.zst
- MAX_LINES=0                        (0 = no cap)
- MAX_OBJECT_MB=8192                 (guardrail before /tmp download)
- MIN_DATE_TO_INGEST=YYYY-MM-DD      (optional; only ingest daily listens on/after this day)

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

from pipeline_recording_id import (
    _load_alias_map_from_db,
    ensure_tables,
    parse_dump,
    print_parser_details,
    upsert,
)
from utils import connect_postgres, load_db_credentials

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")


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
    """Process one S3 object: parse and upsert canonical tables (no stats refresh)."""
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
        min_date_to_ingest = os.getenv("MIN_DATE_TO_INGEST") or None

        alias_conn = connect_postgres(_build_db_creds())
        try:
            alias_to_mbid = _load_alias_map_from_db(alias_conn)
        finally:
            alias_conn.close()

        track_daily, artist_daily, track_info, artist_info, summary = parse_dump(
            local_path,
            max_lines=max_lines,
            alias_to_mbid=alias_to_mbid,
            min_date_to_ingest=min_date_to_ingest,
        )

        # Lambda logs are the best place to see parse health; this mirrors local output.
        try:
            print_parser_details(summary)
        except Exception:
            LOGGER.info("Parse summary: %s", summary)

        db_creds = _build_db_creds()
        conn = connect_postgres(db_creds)
        try:
            ensure_tables(conn)
            upsert(
                conn,
                track_daily=track_daily,
                artist_daily=artist_daily,
                track_info=track_info,
                artist_info=artist_info,
                dump_path=f"s3://{bucket}/{key}",
            )
        finally:
            conn.close()

        result = {
            "bucket": bucket,
            "key": key,
            "bytes": object_bytes,
            "min_date_to_ingest": min_date_to_ingest,
            "lines_parsed": int(summary.get("lines_parsed", 0)),
            "usable_rows": int(summary.get("usable_rows", 0)),
            "track_day_rows": len(track_daily),
            "artist_day_rows": len(artist_daily),
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
                "bucket": record.get("s3", {}).get("bucket", {}).get("name"),
                "key": record.get("s3", {}).get("object", {}).get("key"),
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
            LOGGER.error("Failed to process record: %s", err)
            errors.append(err)

    status_code = 200 if not errors else 500
    return {"statusCode": status_code, "results": results, "errors": errors}

