# README: DE 300 Final Project

# Running code locally:
- get the aws credentials from the access portal and store them under the corresponding values in the .env
```
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
AWS_DEFAULT_REGION=us-east-1

PGPORT=5433
PGHOST="127.0.0.1"
PGDATABASE=postgres
PGPASSWORD=""
```

Connecting to remote rds database via. SSH tunnel to EC2 instance: <br>
```ssh -i "labs/lab3/de300-ec2-lab3-stall.pem" -L 5433:database-stall-munezero.cluster-chm317to06o1.us-east-1.rds.amazonaws.com:5432 ec2-user@98.89.230.64```

- Replace `labs/lab3/de300-ec2-lab3-stall.pem` with wherever your own pem key is.
- I use port 5433 for my tunnel because I have a local pgsql instance on port 5432 already.
- `98.89.230.64` is the elastic IP I allocated for the instance id which hosts our RDS.

- NOTE: make sure the instance has been started on ec2 (in our case, it is `de300_stall_munezero`).

## File layout
- `utils.py`
    - aws credentials, connections, resource managers
- `pipeline.py`
    - pipeline specific processing functions for listenbrainz data and analytics. Used for the flow in `tests/pipeline.ipynb`
- `tests/`
    - Probably need to rename this directory, because actual ingestion and analysis has been happening here.
    - `pipeline.ipynb`
        - test flow for loading raw dumps from our s3 bucket, parsing them, and uploading them to rds database. I RAN THIS TO IMPORT THE FIRST 3 DATA DUMPS INTO RDS.

    - `popularity.ipynb`
        - the analytics/modeling portion of the project. Trains a variety of models on the time series data stored in rds in an effort to predict song and artist popularity. I HAVE NOT YET RUN THIS FILE, MAYBE START HERE?
        - supposedly, I created separate models to predict top artists and top tracks, using both xgboost and lstm. Also have different windows (7 days / 30 days) used for predictions.

## Lambda + stats refresh (current)

- `local/lambda_s3_pipeline.py`
  - Ingest-only Lambda S3 trigger.
  - Parses dumps and upserts into the canonical tables:
    - `artist_info`, `track_info`, `artist_daily_listens`, `track_daily_listens`
  - Supports `MIN_DATE_TO_INGEST=YYYY-MM-DD` to ignore older daily listens.
  - Does **not** recompute `*_daily_stats` (intentionally).

- `local/refresh_daily_stats.py`
  - SQL-only (no Spark) windowed recomputation of `artist_daily_stats` and `track_daily_stats`.
  - Default: recompute last 14 days (per listens table) and replace only that date range.

## Minimum Viable Flow

```
# skip stats, as we will recompute separately for multiple dumps later
python project/local/s3_to_rds/run_pipeline.py --n-dumps n --min-date-to-ingest 2026-01-01 --skip-stats

# --entity both --> compute for both songs and artists
# --window-days 14 --> only recompute statistics for the last 14 days
python project/local/refresh_daily_stats.py --entity both --window-days 14
```