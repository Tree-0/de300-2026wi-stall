"""
ListenBrainz Ingestion Pipeline (Local runner -> AWS)
- Downloads tar.zst dumps from S3
- Processes into daily aggregates
- Writes to RDS PostgreSQL
- Computes stats using PySpark

Run on your local machine to avoid EC2 costs. Requires AWS creds.
"""

import os
import sys
import boto3
import psycopg2
import tarfile
import zstandard as zstd
import orjson
from collections import defaultdict
from datetime import datetime, timezone
from tqdm import tqdm
from pathlib import Path
import logging

from artist_identity import canonicalize_artist_entities, register_artist_aliases

# Setup logging
LOG_PATH = os.getenv("LOG_PATH", "./listenbrainz_ingest.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIG - EDIT THESE OR USE ENV VARS
# ============================================================================

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "stall-munezero-final-project")
S3_DUMP_KEY = os.getenv(
    "S3_DUMP_KEY",
    "listenbrainz/incremental/listenbrainz-listens-dump-2400-20260118-000003-incremental.tar.zst",
)
TEMP_DIR = os.getenv("TEMP_DIR", "./tmp_listenbrainz")

# RDS PostgreSQL Configuration
RDS_HOST = os.getenv("RDS_HOST", "http://database-1.chm317to06o1.us-east-1.rds.amazonaws.com/")
RDS_PORT = int(os.getenv("RDS_PORT", "5432"))
RDS_DATABASE = os.getenv("RDS_DATABASE", "postgres")
RDS_USER = os.getenv("RDS_USER", "postgres")
RDS_PASSWORD = os.getenv("RDS_PASSWORD", "$(aws secretsmanager get-secret-value --secret-id 'arn:aws:secretsmanager:us-east-1:549787090008:secret:rds!db-6323faa7-77d3-4952-af08-bcd6d623f642-g3XgW6' --query SecretString --output text | jq -r '.password')")

# AWS Region + optional profile
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
AWS_PROFILE = os.getenv("AWS_PROFILE")

# Processing config
MAX_LINES = int(os.getenv("MAX_LINES", "0"))  # 0 = no limit, set for testing
CHUNK_SIZE = 50_000  # Batch size for DB inserts

# ============================================================================
# UTILITIES
# ============================================================================


def day_from_unix(ts: int) -> str:
    """Convert unix timestamp to ISO date string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()


def get_any(d, *keys):
    """Try multiple keys in dict, return first found."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    return None


def chunked(iterable, n=50_000):
    """Yield successive chunks from iterable."""
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

# ============================================================================
# S3 OPERATIONS
# ============================================================================


from botocore.exceptions import ClientError
import boto3

def download_s3_dump(bucket, key, local_path):
    # Use boto3's default credential chain (same idea as AWS CLI)
    session = boto3.Session(region_name=AWS_REGION)
    s3 = session.client("s3")

    # Debug: confirm identity used by boto3
    ident = session.client("sts").get_caller_identity()
    logger.info("boto3 identity: %s", ident)

    logger.info("Downloading s3://%s/%s", bucket, key)

    try:
        s3.head_object(Bucket=bucket, Key=key)  # explicit + better error
        s3.download_file(bucket, key, local_path)
        logger.info("Downloaded to %s", local_path)
        return True
    except ClientError as e:
        err = e.response.get("Error", {})
        logger.error("S3 ClientError %s: %s", err.get("Code"), err.get("Message"))
        return False

# ============================================================================
# POSTGRES OPERATIONS
# ============================================================================


def create_tables(conn):
    """Create schema if not exists."""
    ddl = """
    CREATE TABLE IF NOT EXISTS ingestion_state (
      id                INT PRIMARY KEY DEFAULT 1,
      last_dump_key     TEXT,
      last_dump_path    TEXT,
      loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT singleton_row CHECK (id = 1)
    );
    INSERT INTO ingestion_state (id) VALUES (1)
    ON CONFLICT (id) DO NOTHING;

    CREATE TABLE IF NOT EXISTS artist_info (
      artist_mbid TEXT PRIMARY KEY,
      artist_name TEXT
    );

    CREATE TABLE IF NOT EXISTS track_info (
      recording_id TEXT PRIMARY KEY,
      track_name TEXT,
      artist_mbids TEXT[],
      release_name TEXT
    );

    CREATE TABLE IF NOT EXISTS artist_daily_listens (
      day DATE NOT NULL,
      artist_mbid TEXT NOT NULL,
      listen_count BIGINT NOT NULL,
      PRIMARY KEY (day, artist_mbid),
      FOREIGN KEY (artist_mbid) REFERENCES artist_info(artist_mbid)
    );

    CREATE TABLE IF NOT EXISTS track_daily_listens (
      day DATE NOT NULL,
      recording_id TEXT NOT NULL,
      listen_count BIGINT NOT NULL,
      PRIMARY KEY (day, recording_id),
      FOREIGN KEY (recording_id) REFERENCES track_info(recording_id)
    );

    CREATE TABLE IF NOT EXISTS artist_daily_stats (
      day DATE NOT NULL,
      artist_mbid TEXT NOT NULL,
      growth_percentile FLOAT,
      cumulative_listen_count BIGINT,
      listen_count_past_7_days BIGINT,
      listen_pctl_past_7_days FLOAT,
      listen_count_past_30_days BIGINT,
      listen_pctl_past_30_days FLOAT,
      PRIMARY KEY (day, artist_mbid),
      FOREIGN KEY (artist_mbid) REFERENCES artist_info(artist_mbid)
    );

    CREATE TABLE IF NOT EXISTS track_daily_stats (
      day DATE NOT NULL,
      recording_id TEXT NOT NULL,
      growth_percentile FLOAT,
      cumulative_listen_count BIGINT,
      listen_count_past_7_days BIGINT,
      listen_pctl_past_7_days FLOAT,
      listen_count_past_30_days BIGINT,
      listen_pctl_past_30_days FLOAT,
      PRIMARY KEY (day, recording_id),
      FOREIGN KEY (recording_id) REFERENCES track_info(recording_id)
    );

    CREATE INDEX IF NOT EXISTS idx_track_daily_day ON track_daily_listens(day);
    CREATE INDEX IF NOT EXISTS idx_artist_daily_day ON artist_daily_listens(day);
    CREATE INDEX IF NOT EXISTS idx_track_stats_day ON track_daily_stats(day);
    CREATE INDEX IF NOT EXISTS idx_artist_stats_day ON artist_daily_stats(day);
    """

    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(ddl)
    logger.info("Tables created/verified")


def iter_listens_from_tar_zst(path: Path):
    """Stream decompress and extract .listens files from tar.zst."""
    with path.open("rb") as fh:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            with tarfile.open(fileobj=reader, mode="r|*") as tf:
                for member in tf:
                    if not member.isfile():
                        continue
                    name = member.name
                    if "/listens/" not in name or not name.endswith(".listens"):
                        continue
                    f = tf.extractfile(member)
                    if f is None:
                        continue
                    for line in f:
                        line = line.strip()
                        if line:
                            yield name, line


def parse_dump(dump_path):
    """Parse tar.zst dump and aggregate daily listens."""
    track_daily = defaultdict(int)
    artist_daily = defaultdict(int)
    track_info = {}
    artist_info = {}
    alias_to_mbid = {}

    lines = 0
    bad_json = 0
    missing_ts = 0
    missing_artist = 0
    synthetic_artist_mbids = 0

    logger.info("Parsing %s", dump_path)

    for _, line in tqdm(iter_listens_from_tar_zst(Path(dump_path)), desc="Parsing listens"):
        lines += 1
        if MAX_LINES and lines > MAX_LINES:
            break

        try:
            rec = orjson.loads(line)
        except Exception:
            bad_json += 1
            continue

        ts = rec.get("timestamp")
        if ts is None:
            missing_ts += 1
            continue

        day = day_from_unix(int(ts))
        tm = rec.get("track_metadata") or {}
        add = tm.get("additional_info") or {}
        artist_mbids = get_any(add, "artist_mbids") or []
        track_name = tm.get("track_name")
        release_name = tm.get("release_name")
        artist_name = tm.get("artist_name")

        artist_entities, synthetic_count = canonicalize_artist_entities(
            artist_name=artist_name,
            artist_mbids=artist_mbids,
            alias_to_mbid=alias_to_mbid,
        )
        synthetic_artist_mbids += synthetic_count
        normalized_artist_mbids = [ambid for ambid, _, _ in artist_entities]

        recording_id = f"{'_'.join(normalized_artist_mbids)}_{track_name}"

        track_daily[(day, recording_id)] += 1
        if recording_id not in track_info:
            track_info[recording_id] = (track_name, normalized_artist_mbids, release_name)

        if artist_entities:
            for ambid, normalized_name, _ in artist_entities:
                artist_daily[(day, ambid)] += 1
                if ambid not in artist_info and normalized_name:
                    artist_info[ambid] = normalized_name
                register_artist_aliases(
                    alias_to_mbid,
                    artist_info.get(ambid) or normalized_name,
                    ambid,
                )
        else:
            missing_artist += 1

    summary = {
        "lines_parsed": lines,
        "bad_json": bad_json,
        "missing_timestamp": missing_ts,
        "missing_artist_mbid": missing_artist,
        "synthetic_artist_mbids": synthetic_artist_mbids,
        "unique_track_day_keys": len(track_daily),
        "unique_artist_day_keys": len(artist_daily),
        "unique_tracks": len({k[1] for k in track_daily.keys()}),
        "unique_artists": len({k[1] for k in artist_daily.keys()}),
    }

    logger.info("Parse summary: %s", summary)
    return track_daily, artist_daily, track_info, artist_info


def upsert_data(conn, track_daily, artist_daily, track_info, artist_info, dump_key):
    """Upsert parsed data into RDS."""
    from psycopg2.extras import execute_values

    conn.autocommit = False

    with conn, conn.cursor() as cur:
        # Upsert artist_info
        logger.info("Upserting artist_info...")
        artist_rows = [(k, v) for k, v in artist_info.items()]
        for batch in chunked(artist_rows, n=CHUNK_SIZE):
            execute_values(
                cur,
                """INSERT INTO artist_info (artist_mbid, artist_name) VALUES %s
                   ON CONFLICT (artist_mbid) DO UPDATE
                         SET artist_name = COALESCE(artist_info.artist_name, EXCLUDED.artist_name)""",
                batch,
            )
        logger.info("Upserted %d artist records", len(artist_rows))

        # Upsert track_info
        logger.info("Upserting track_info...")
        track_rows = [(k, v[0], v[1], v[2]) for k, v in track_info.items()]
        for batch in chunked(track_rows, n=CHUNK_SIZE):
            execute_values(
                cur,
                """INSERT INTO track_info (recording_id, track_name, artist_mbids, release_name) VALUES %s
                   ON CONFLICT (recording_id) DO UPDATE
                   SET track_name = COALESCE(EXCLUDED.track_name, track_info.track_name),
                       artist_mbids = COALESCE(EXCLUDED.artist_mbids, track_info.artist_mbids),
                       release_name = COALESCE(EXCLUDED.release_name, track_info.release_name)""",
                batch,
            )
        logger.info("Upserted %d track records", len(track_rows))

        # Upsert track_daily_listens
        logger.info("Upserting track_daily_listens...")
        track_daily_rows = [(day, rid, cnt) for (day, rid), cnt in track_daily.items()]
        for batch in chunked(track_daily_rows, n=CHUNK_SIZE):
            execute_values(
                cur,
                """INSERT INTO track_daily_listens (day, recording_id, listen_count) VALUES %s
                   ON CONFLICT (day, recording_id) DO UPDATE
                   SET listen_count = track_daily_listens.listen_count + EXCLUDED.listen_count""",
                batch,
            )
        logger.info("Upserted %d track_daily records", len(track_daily_rows))

        # Upsert artist_daily_listens
        logger.info("Upserting artist_daily_listens...")
        artist_daily_rows = [(day, mbid, cnt) for (day, mbid), cnt in artist_daily.items()]
        for batch in chunked(artist_daily_rows, n=CHUNK_SIZE):
            execute_values(
                cur,
                """INSERT INTO artist_daily_listens (day, artist_mbid, listen_count) VALUES %s
                   ON CONFLICT (day, artist_mbid) DO UPDATE
                   SET listen_count = artist_daily_listens.listen_count + EXCLUDED.listen_count""",
                batch,
            )
        logger.info("Upserted %d artist_daily records", len(artist_daily_rows))

        # Update ingestion state
        cur.execute(
            """UPDATE ingestion_state
               SET last_dump_key = %s, loaded_at = now() WHERE id = 1""",
            (dump_key,),
        )
        conn.commit()

# ============================================================================
# SPARK STATS COMPUTATION
# ============================================================================


def compute_stats_with_spark(rds_host, rds_port, rds_db, rds_user, rds_password):
    """Use PySpark to compute daily stats from aggregated data."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    logger.info("Starting Spark session for stats computation...")

    spark = (
        SparkSession.builder.appName("ListenBrainz Stats")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    jdbc_url = f"jdbc:postgresql://{rds_host}:{rds_port}/{rds_db}"
    jdbc_props = {
        "user": rds_user,
        "password": rds_password,
        "driver": "org.postgresql.Driver",
    }

    try:
        # ========== ARTIST STATS ==========
        logger.info("Computing artist stats...")
        artist_df = spark.read.jdbc(url=jdbc_url, table="artist_daily_listens", properties=jdbc_props)
        artist_df = artist_df.sort("artist_mbid", "day")

        artist_window_unbounded = Window.partitionBy("artist_mbid").orderBy("day").rowsBetween(
            Window.unboundedPreceding, 0
        )
        artist_window_7d = Window.partitionBy("artist_mbid").orderBy("day").rowsBetween(-6, 0)
        artist_window_30d = Window.partitionBy("artist_mbid").orderBy("day").rowsBetween(-29, 0)

        artist_df = artist_df.withColumn(
            "cumulative_listen_count", F.sum("listen_count").over(artist_window_unbounded)
        )
        artist_df = artist_df.withColumn(
            "listen_count_past_7_days", F.sum("listen_count").over(artist_window_7d)
        )
        artist_df = artist_df.withColumn(
            "listen_count_past_30_days", F.sum("listen_count").over(artist_window_30d)
        )

        artist_window_lag = Window.partitionBy("artist_mbid").orderBy("day")
        artist_df = artist_df.withColumn(
            "cumulative_yesterday", F.lag("cumulative_listen_count", 1).over(artist_window_lag)
        )
        artist_df = artist_df.withColumn(
            "growth_rate",
            F.when(
                F.col("cumulative_yesterday").isNotNull() & (F.col("cumulative_yesterday") > 0),
                F.col("listen_count") / F.col("cumulative_yesterday"),
            ).otherwise(None),
        )

        artist_df = artist_df.withColumn(
            "growth_percentile",
            F.percent_rank().over(Window.partitionBy("day").orderBy(F.col("growth_rate").asc_nulls_first())),
        )
        artist_df = artist_df.withColumn(
            "listen_pctl_past_7_days",
            F.percent_rank().over(Window.partitionBy("day").orderBy(F.col("listen_count_past_7_days"))),
        )
        artist_df = artist_df.withColumn(
            "listen_pctl_past_30_days",
            F.percent_rank().over(Window.partitionBy("day").orderBy(F.col("listen_count_past_30_days"))),
        )

        artist_stats = artist_df.select(
            "day",
            "artist_mbid",
            "growth_percentile",
            "cumulative_listen_count",
            "listen_count_past_7_days",
            "listen_pctl_past_7_days",
            "listen_count_past_30_days",
            "listen_pctl_past_30_days",
        )

        artist_stats.write.jdbc(url=jdbc_url, table="artist_daily_stats", mode="overwrite", properties=jdbc_props)
        logger.info("Wrote %s artist stats", f"{artist_stats.count():,}")

        # ========== TRACK STATS ==========
        logger.info("Computing track stats...")
        track_df = spark.read.jdbc(url=jdbc_url, table="track_daily_listens", properties=jdbc_props)
        track_df = track_df.sort("recording_id", "day")

        track_window_unbounded = Window.partitionBy("recording_id").orderBy("day").rowsBetween(
            Window.unboundedPreceding, 0
        )
        track_window_7d = Window.partitionBy("recording_id").orderBy("day").rowsBetween(-6, 0)
        track_window_30d = Window.partitionBy("recording_id").orderBy("day").rowsBetween(-29, 0)

        track_df = track_df.withColumn(
            "cumulative_listen_count", F.sum("listen_count").over(track_window_unbounded)
        )
        track_df = track_df.withColumn(
            "listen_count_past_7_days", F.sum("listen_count").over(track_window_7d)
        )
        track_df = track_df.withColumn(
            "listen_count_past_30_days", F.sum("listen_count").over(track_window_30d)
        )

        track_window_lag = Window.partitionBy("recording_id").orderBy("day")
        track_df = track_df.withColumn(
            "cumulative_yesterday", F.lag("cumulative_listen_count", 1).over(track_window_lag)
        )
        track_df = track_df.withColumn(
            "growth_rate",
            F.when(
                F.col("cumulative_yesterday").isNotNull() & (F.col("cumulative_yesterday") > 0),
                F.col("listen_count") / F.col("cumulative_yesterday"),
            ).otherwise(None),
        )

        track_df = track_df.withColumn(
            "growth_percentile",
            F.percent_rank().over(Window.partitionBy("day").orderBy(F.col("growth_rate").asc_nulls_first())),
        )
        track_df = track_df.withColumn(
            "listen_pctl_past_7_days",
            F.percent_rank().over(Window.partitionBy("day").orderBy(F.col("listen_count_past_7_days"))),
        )
        track_df = track_df.withColumn(
            "listen_pctl_past_30_days",
            F.percent_rank().over(Window.partitionBy("day").orderBy(F.col("listen_count_past_30_days"))),
        )

        track_stats = track_df.select(
            "day",
            "recording_id",
            "growth_percentile",
            "cumulative_listen_count",
            "listen_count_past_7_days",
            "listen_pctl_past_7_days",
            "listen_count_past_30_days",
            "listen_pctl_past_30_days",
        )

        track_stats.write.jdbc(url=jdbc_url, table="track_daily_stats", mode="overwrite", properties=jdbc_props)
        logger.info("Wrote %s track stats", f"{track_stats.count():,}")

    finally:
        spark.stop()
        logger.info("Spark session closed")

# ============================================================================
# MAIN PIPELINE
# ============================================================================


def main():
    """Execute full ingestion pipeline."""
    logger.info("=" * 80)
    logger.info("Starting ListenBrainz Local -> AWS Ingestion Pipeline")
    logger.info("=" * 80)

    if not RDS_PASSWORD:
        logger.error("RDS_PASSWORD is not set. Export it in your shell.")
        return False

    # Create temp directory
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Download from S3
    local_dump = f"{TEMP_DIR}/{S3_DUMP_KEY.split('/')[-1]}"
    logger.info("S3_BUCKET=%s", S3_BUCKET)
    logger.info("S3_DUMP_KEY=%s", S3_DUMP_KEY)
    if not download_s3_dump(S3_BUCKET, S3_DUMP_KEY, local_dump):
        logger.error("Failed to download dump from S3")
        return False

    # Connect to RDS
    try:
        conn = psycopg2.connect(
            host=RDS_HOST,
            port=RDS_PORT,
            database=RDS_DATABASE,
            user=RDS_USER,
            password=RDS_PASSWORD,
        )
        logger.info("Connected to RDS: %s@%s:%s/%s", RDS_USER, RDS_HOST, RDS_PORT, RDS_DATABASE)
    except Exception as e:
        logger.error("Failed to connect to RDS: %s", e)
        return False

    try:
        # Create tables
        create_tables(conn)

        # Parse dump
        track_daily, artist_daily, track_info, artist_info = parse_dump(local_dump)

        # Upsert to RDS
        upsert_data(conn, track_daily, artist_daily, track_info, artist_info, S3_DUMP_KEY)

        # Compute stats with Spark
        compute_stats_with_spark(RDS_HOST, RDS_PORT, RDS_DATABASE, RDS_USER, RDS_PASSWORD)

        logger.info("=" * 80)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 80)
        return True

    except Exception as e:
        logger.error("Pipeline failed: %s", e, exc_info=True)
        return False
    finally:
        conn.close()
        # Cleanup
        if os.path.exists(local_dump):
            os.remove(local_dump)
            logger.info("Cleaned up %s", local_dump)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
