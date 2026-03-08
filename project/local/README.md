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

Connecting to remote rds database via. EC2 instance: <br>
```ssh -i "labs/lab3/de300-ec2-lab3-stall.pem" -L 5433:database-stall-munezero.cluster-chm317to06o1.us-east-1.rds.amazonaws.com:5432 ec2-user@98.89.230.64```

- Replace `labs/lab3/de300-ec2-lab3-stall.pem` with wherever your own pem key is.
- I use port 5433 for my tunnel because I have a local instance on port 5432 already.
- `98.89.230.64` is an elastic IP I allocated for the instance id which hosts our RDS.

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



- **TODO**
    - as is, the `upsert_aggregates` function in `pipeline.py` is very slow, because it is single-threaded and uploading millions of rows to the database in batches. Want to speed this up.
        - [ ] one suggestion was to parallelize the 4 separate table upserts in the function with `threadPoolExecutor`, since the table data is independent once it has been parsed from the raw dump. 
