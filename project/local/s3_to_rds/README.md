# S3 to RDS v2 Pipeline

This folder contains a server-run batch ingester that combines:

- S3 dump discovery and progress tracking from the notebook / older pipeline flow
- v2 canonical/fallback cleaning and upsert logic from `pipeline_recording_id_v2.py`
- `dump_id` provenance in daily listen tables
- optional refresh of `artist_daily_stats` and `track_daily_stats`

## What it does

When you run the pipeline, it will:

1. Read `ingestion_state` to find the last processed dump id
2. List dump files in S3
3. Select the next `N` dumps after the saved dump id
4. Download each dump locally
5. Parse and clean it with the v2 identity logic
6. Upsert into:
   - `artist_info`
   - `track_info`
   - `artist_daily_listens`
   - `track_daily_listens`
7. Update `ingestion_state`
8. (Optional) recompute:
   - `artist_daily_stats`
   - `track_daily_stats`

## Files

- `run_pipeline.py`: main CLI entrypoint

## Remote server setup

### 1. System prerequisites

Install these on the remote server:

- Python 3.10+
- Java 11 or newer
- Network access to S3
- Network access to Postgres, either:
  - direct RDS access from the server, or
  - an SSH tunnel to the EC2 host

### 2. Python packages

In your virtual environment, install at least:

```bash
pip install boto3 psycopg2-binary python-dotenv orjson zstandard pyspark
```

If your existing project environment already has these, you can reuse it.

### 3. Environment configuration

By default the script looks for a `.env` file at:

`project/.env`

Set the following if they are not already available through IAM / Secrets Manager:

```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_DEFAULT_REGION=us-east-1

PGHOST=127.0.0.1
PGPORT=5433
PGDATABASE=postgres
PGUSER=postgres
PGPASSWORD=...
```

If the server can reach RDS directly, use the real RDS hostname and port instead of `127.0.0.1:5433`.

### 4. If using an SSH tunnel

Run this first and keep it open while the batch job runs:

```bash
ssh -i "/path/to/key.pem" \
  -L 5433:database-stall-munezero.cluster-chm317to06o1.us-east-1.rds.amazonaws.com:5432 \
  ec2-user@98.89.230.64
```

## How to run

From `project/local/s3_to_rds_v2`:

```bash
python run_pipeline.py --n-dumps 5
```

This ingests the next 5 dumps after the `last_dump_id` saved in `ingestion_state`.

### Common commands

Ingest the next 1 dump:

```bash
python run_pipeline.py --n-dumps 1
```

Ingest the next 5 dumps but skip stats refresh:

```bash
python run_pipeline.py --n-dumps 5 --skip-stats
```

## Stats refresh (SQL-only, windowed)

`project/local/refresh_daily_stats.py` now performs a SQL-only, *windowed* refresh
of the stats tables (no Spark). By default it recomputes only the most recent
14 days (based on `MAX(day)` in each listens table) and replaces only that date
range in `*_daily_stats`.

Run (recommended after ingest, or on a schedule):

```bash
python project/local/refresh_daily_stats.py --entity both --window-days 14
```

Notes:
- This keeps historical stats rows older than the window intact.
- The refresh reads a 30-day lookback of listens to make rolling 30-day sums
  correct inside the output window.

Start after a specific dump id instead of using `ingestion_state`:

```bash
python run_pipeline.py --n-dumps 5 --start-after-dump-id 2428
```

Keep downloaded dump files for debugging:

```bash
python run_pipeline.py --n-dumps 1 --keep-temp-files
```

Use a custom `.env` file:

```bash
python run_pipeline.py --n-dumps 5 --env-file /path/to/.env
```

## Notes

- The v2 cleaning logic is reused from `pipeline_recording_id_v2.py`.
- `dump_id` stays in the daily listens tables for provenance.
- The stats tables are recomputed from the aggregated daily listens tables, so stats remain one row per `(day, entity)` while raw lineage remains per dump.
- Re-running the same dump is idempotent because daily listen upserts use `(day, entity_id, dump_id)` as the conflict key.
