# DE 300 ListenBrainz Final Project Pipeline

**Nathaniel Stall, Didier Munezero**

---

This README describes our ListenBrainz pipeline: environment setup, ingesting new data dumps from S3 into Postgres, computing statistics, and training models / generating popularity predictions.

---

## 1. Environment & infrastructure

### 1.1 Components

- **S3**: Stores incremental ListenBrainz dumps
  - **Bucket**: `stall-munezero-final-project`
  - **Prefix**: `listenbrainz/incremental/`
- **Postgres (RDS or local)**
  - **RDS instance name**: `database-stall-munezero`
  - **Tables**:
    - `artist_info`, `track_info`
    - `artist_daily_listens`, `track_daily_listens`
    - `artist_daily_stats`, `track_daily_stats`
    - `ingestion_state`
- **Compute**
  - Local machine **or** a remote server (e.g., EC2)
  - When running locally, we typically connect to RDS through an **SSH tunnel**

### 1.2 `.env` configuration

The pipeline expects database and AWS credentials in `project/.env` (or a custom file via `--env-file`).

```env
# AWS (only needed if you’re not using an EC2 IAM role)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_DEFAULT_REGION=us-east-1

# Postgres connection (local or tunneled to RDS)
PGHOST=127.0.0.1
PGPORT=5433            # use 5432 if you connect directly to RDS
PGDATABASE=postgres
PGUSER=postgres
PGPASSWORD=...
```

By default, `project/local/s3_to_rds/run_pipeline.py` tries to load `project/.env`. You can override with `--env-file /path/to/other.env` or skip loading with `--skip-dotenv`.

### 1.3 SSH tunnel to RDS (when running locally)

**This is how we did the majority of our development.**

If your code runs on your laptop and RDS is not directly reachable, create an SSH tunnel via an EC2 instance that can reach RDS:

```bash
ssh -i "path/to/your-key.pem" \
  -L 5433:database-stall-munezero.cluster-chm317to06o1.us-east-1.rds.amazonaws.com:5432 \
  ec2-user@98.89.230.64
```

- Keep this SSH session open while you run the pipeline.
- In this configuration, set `PGHOST=127.0.0.1` and `PGPORT=5433` in your `.env`.

### 1.4 Running on EC2 (no SSH tunnel)

If you run the pipeline directly on EC2 and that instance can reach RDS:

- Set `PGHOST` to the **RDS endpoint** and `PGPORT=5432`.
- Use either:
  - an **IAM role** for S3 access (preferred), or
  - AWS access keys in `.env`.

Install dependencies (on EC2 or locally) in a virtualenv:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

pip install --upgrade pip
pip install -r requirements.txt
```

---

## Quick “minimum viable” flow (Details below)

```bash
# 1) Ensure .env is set and DB is reachable (via SSH tunnel or direct).

# 2) Ingest N new dumps from S3, skipping stats:
python project/local/s3_to_rds/run_pipeline.py \
  --n-dumps 5 \
  --min-date-to-ingest 2026-01-01 \
  --skip-stats

# 3) Refresh stats for the last 14 days:
python project/local/refresh_daily_stats.py \
  --entity both \
  --window-days 14

# 4) Open modeling notebook and run predictions:
# project/local/tests/popularity.ipynb
```

## 2. Ingesting new data dumps (S3 → RDS)

### 2.1 Entry point

The main ingestion entry point is:

```bash
python project/local/s3_to_rds/run_pipeline.py ...
```

This script:

1. Reads or creates `ingestion_state` in Postgres to track the last processed dump.
2. Lists available dumps in S3 under `s3://BUCKET/PREFIX` (defaults to `stall-munezero-final-project` and `listenbrainz/incremental/`).
3. Selects the next N dumps (by dump id), downloads them, parses and cleans them.
4. Upserts into:
  - `artist_info`
  - `track_info`
  - `artist_daily_listens`
  - `track_daily_listens`
5. Updates `ingestion_state` with the most recent dump.
6. Optionally refreshes stats tables for artists/tracks (Spark-based).

### 2.2 Common arguments

Key CLI options (see `--help` for the full list):

- `--n-dumps INT` (required): how many new dumps to ingest.
- `--bucket BUCKET`: S3 bucket (default `stall-munezero-final-project`).
- `--prefix PREFIX`: S3 prefix (default `listenbrainz/incremental/`).
- `--min-date-to-ingest YYYY-MM-DD`: ignore listens before this date.
- `--skip-stats`: skip the stats refresh step after ingestion.
- `--stats-entity {artist,track,both}`: which stats tables to refresh (default `both`).
- `--spark-driver-memory`: Spark driver memory (default `4g`).
- `--start-after-dump-id ID`: override `ingestion_state` and start after a specific dump id.
- `--tmp-dir PATH`: local temp dir for downloaded dumps (default `./tmp_s3_to_rds_v2`).
- `--keep-temp-files`: keep downloaded dumps on disk after ingestion.
- `--env-file PATH`: explicit `.env` path.
- `--skip-dotenv`: skip loading any `.env`.

### 2.3 Typical ingest commands

Ingest the next N dumps, skip stats (fast ingest-only run):

```bash
python project/local/s3_to_rds/run_pipeline.py \
  --n-dumps 5 \
  --min-date-to-ingest 2026-01-01 \
  --skip-stats
```

Ingest exactly the next 1 dump (using `ingestion_state`):

```bash
python project/local/s3_to_rds/run_pipeline.py --n-dumps 1
```

Start after a specific dump id (ignore `ingestion_state`):

```bash
python project/local/s3_to_rds/run_pipeline.py \
  --n-dumps 5 \
  --start-after-dump-id 2428
```

After ingestion, you can query tables like `artist_daily_listens` and `track_daily_listens` in Postgres to verify row counts.

---

## 3. Computing statistics from ingested data

There are two mechanisms in this repository:

1. **Spark-based stats** (optional, driven by `run_pipeline.py`)
2. **Preferred: SQL-only, windowed stats refresher** (`refresh_daily_stats.py`)

### 3.1 Recommended: SQL-only windowed stats refresh

To (re)compute `artist_daily_stats` and `track_daily_stats` for a moving window:

```bash
python project/local/refresh_daily_stats.py --entity both --window-days 14
```

- `--entity {artist,track,both}`: which stats tables to refresh.
- `--window-days N`: recompute only the last N days based on `MAX(day)` in each listens table.
- **Behavior**:
  - Reads a 30-day lookback of listens so rolling 30-day aggregates are correct.
  - Replaces only the target window in `*_daily_stats`, leaving older history untouched.

Recommended flow:

1. Ingest dumps with `--skip-stats` (fast).
2. Run `refresh_daily_stats.py` after ingestion (or on a schedule).

### 3.2 Spark-based stats (invoked from ingester)

If you do not pass `--skip-stats` to `run_pipeline.py`, it will:

- Start a `pyspark.sql.SparkSession` with `--spark-driver-memory`.
- Connect to Postgres via JDBC.
- Recompute stats via `_compute_entity_stats` for artists and/or tracks.

Use this path only if you specifically want the Spark-based refresh; otherwise, prefer the SQL-only refresher.

---

## 4. Modeling & popularity predictions

The modeling and prediction logic currently lives in:

- `project/local/tests/popularity.ipynb`

This notebook:

- Reads from the `*_daily_stats` (and/or `*_daily_listens`) tables in Postgres.
- Builds time-series features for artists and tracks.
- Trains models (e.g., XGBoost / LSTM) across different windows (e.g., 7-day, 30-day).
- Produces predicted “top N” artists and tracks for a future window.

### 4.1 Typical workflow

1. Ensure stats are up-to-date (Section 3).
2. Open the notebook in:
  - VS Code / Cursor, or
  - Jupyter Lab / Notebook
3. Configure the DB connection (typically via the same `.env`).
4. Run all cells:
  - Data loading
  - Feature engineering
  - Model training
  - Evaluation & prediction output

---

## 5. Important files & directories

### 5.1 Core pipeline

- `project/local/s3_to_rds/run_pipeline.py`: main CLI for ingesting dumps from S3 into Postgres (and optional Spark stats refresh).
- `project/local/refresh_daily_stats.py`: SQL-only, windowed stats refresher for `artist_daily_stats` and `track_daily_stats`.
- `project/local/utils.py`: helpers for AWS (S3 client, downloads), DB connections, and configuration.
- `project/local/pipeline.py`: processing and aggregation utilities used by the pipeline.
- `project/local/pipeline_recording_id.py`: canonical identity resolution, cleaning and upsert logic for recording IDs.

### 5.2 Notebooks

- `project/local/tests/popularity.ipynb`: modeling and prediction notebook.

---

