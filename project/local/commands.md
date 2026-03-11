# Project Commands Reference

All commands assume:

- You are in the `project/local/` directory
- Your SSH tunnel is open (see **0. SSH Tunnel** below)
- The virtual environment is activated

---

## 0. Activate the virtual environment

```bash
source "/Users/didiermunezero/Documents/NU/Junior/DE 300/.venv/bin/activate"
```

---

## 1. Open the SSH Tunnel (required for all DB commands)

Forwards local port **5433** → RDS **5432** through the EC2 bastion.
Must be running in a separate terminal before any DB work.

```bash
ssh -i "/Users/didiermunezero/Documents/NU/Junior/DE 300/de300-2026wi-stall/project/de300-ec2-lab3-stall.pem" \
    -L 5433:database-stall-munezero.cluster-chm317to06o1.us-east-1.rds.amazonaws.com:5432 \
    ec2-user@98.89.230.64
```

---

## 2. Ingest a dump into the v2 tables (`pipeline_recording_id_v2.py`)

Parses a `.tar.zst` ListenBrainz incremental dump and writes rows into:
`artist_info_v2`, `track_info_v2`, `artist_daily_listens_v2`, `track_daily_listens_v2`

### Full ingest (production)

```bash
python pipeline_recording_id_v2.py \
  "/Users/didiermunezero/Documents/NU/Junior/DE 300/de300-2026wi-munezero/Final Project/datadumps/musicbrainz-2/listenbrainz-listens-dump-2435-20260219-000003-incremental.tar.zst" \
  --upsert
```

### Quick test (first 2000 lines only, no DB write)

```bash
python pipeline_recording_id_v2.py \
  "/path/to/dump.tar.zst" \
  --max-lines 2000
```

### Quick test with DB write

```bash
python pipeline_recording_id_v2.py \
  "/path/to/dump.tar.zst" \
  --max-lines 2000 --upsert
```

**Flags:**
| Flag | Description |
|------|-------------|
| `dump_path` | Path to the `.tar.zst` dump file (required) |
| `--upsert` | Create v2 tables and write to Postgres |
| `--max-lines N` | Cap lines parsed (for fast smoke tests) |

---

## 3. Compute rolling-window stats for v2 tables (`refresh_daily_stats_v2.py`)

Reads `artist_daily_listens_v2` and `track_daily_listens_v2`, computes
7-day / 30-day windows, `growth_percentile`, cumulative counts, etc., and
**overwrites** `artist_daily_stats_v2` and `track_daily_stats_v2`.

**Must be run after every new ingest before the notebook will return non-empty rows.**

### Compute stats for both artists and tracks

```bash
python refresh_daily_stats_v2.py
```

### Compute stats for artists only

```bash
python refresh_daily_stats_v2.py --entity artist
```

### Compute stats for tracks only

```bash
python refresh_daily_stats_v2.py --entity track
```

**Flags:**
| Flag | Description |
|------|-------------|
| `--entity` | `artist` \| `track` \| `both` (default: `both`) |
| `--host` | Postgres host override |
| `--port` | Postgres port override |
| `--dbname` | Postgres database override |
| `--user` | Postgres user override |
| `--password` | Postgres password override |
| `--env-file` | Path to a custom `.env` file |
| `--skip-dotenv` | Skip loading `.env` entirely |

---

## 4. Compute rolling-window stats for v1 tables (`refresh_daily_stats.py`)

Same as above but writes to the original v1 tables:
`artist_daily_stats`, `track_daily_stats` (key: `artist_mbid`)

```bash
python refresh_daily_stats.py
```

---

## 5. Audit a dump for missing artist IDs (`audit_dump_missing_artists.py`)

Scans a dump and reports what percentage of listen records are missing
artist MBIDs and prints example records for inspection.

### Full audit

```bash
python audit_dump_missing_artists.py \
  "/path/to/dump.tar.zst"
```

### With line cap and more examples

```bash
python audit_dump_missing_artists.py \
  "/path/to/dump.tar.zst" \
  --max-lines 100000 --examples 20
```

**Flags:**
| Flag | Description |
|------|-------------|
| `dump_path` | Path to the `.tar.zst` dump file (required) |
| `--max-lines N` | Cap lines scanned |
| `--examples N` | Number of missing-artist records to print (default: 10) |

---

## 6. Verify DB connection and row counts (one-liner)

Quick sanity check — prints which RDS you are connected to and the current
row counts for all 6 v2 tables.

```bash
python -c "
from utils import connect_postgres, load_db_credentials
creds = load_db_credentials()
print(f'Connecting to: {creds[\"host\"]}:{creds[\"port\"]}/{creds[\"dbname\"]} as {creds[\"user\"]}')
conn = connect_postgres(creds)
cur = conn.cursor()
cur.execute('SELECT current_user, current_database(), inet_server_addr(), inet_server_port()')
print('Server info:', cur.fetchone())
for table in ['artist_info_v2','track_info_v2','artist_daily_listens_v2','track_daily_listens_v2','artist_daily_stats_v2','track_daily_stats_v2']:
    cur.execute(f'SELECT COUNT(*) FROM {table}')
    print(f'  {table}: {cur.fetchone()[0]:,} rows')
cur.close(); conn.close()
"
```

---

## Typical end-to-end workflow

```
1.  Open SSH tunnel (step 1) in a separate terminal — keep it open.
2.  Ingest a new dump:   python pipeline_recording_id_v2.py <dump> --upsert
3.  Refresh stats:       python refresh_daily_stats_v2.py
4.  Open popularity_v2.ipynb and run all cells.
```

---

## 7. Deploy `pipeline_v2` as an S3-triggered AWS Lambda

This deploys the new handler in `lambda_s3_pipeline_v2.py` as a **container-based Lambda**
and wires it to S3 `ObjectCreated` events on bucket `stall-munezero-final-project`.

### 7.1 Set deployment variables

```bash
export AWS_REGION="us-east-1"
export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export ECR_REPO="listenbrainz-v2-lambda"
export IMAGE_TAG="latest"
export IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}"
export LAMBDA_NAME="listenbrainz-v2-s3-ingest"
export BUCKET_NAME="stall-munezero-final-project"

# You must provide an execution role ARN with permissions listed below.
export LAMBDA_ROLE_ARN="arn:aws:iam::<account-id>:role/<lambda-execution-role>"

# Required when your Aurora cluster is in private subnets.
export LAMBDA_SUBNETS="subnet-aaa,subnet-bbb"
export LAMBDA_SECURITY_GROUPS="sg-aaa"
```

### 7.2 Build and push Lambda image

```bash
aws ecr describe-repositories --repository-names "${ECR_REPO}" --region "${AWS_REGION}" >/dev/null 2>&1 || \
aws ecr create-repository --repository-name "${ECR_REPO}" --region "${AWS_REGION}"

aws ecr get-login-password --region "${AWS_REGION}" | \
docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

docker build -f lambda_v2.Dockerfile -t "${ECR_REPO}:${IMAGE_TAG}" .
docker tag "${ECR_REPO}:${IMAGE_TAG}" "${IMAGE_URI}"
docker push "${IMAGE_URI}"
```

### 7.3 Create (or update) the Lambda function

`PackageType` is immutable in Lambda.
If an existing function was created as `Zip`, `aws lambda update-function-code --image-uri ...`
will fail with `InvalidParameterValueException`.

Check current type:

```bash
aws lambda get-function-configuration \
  --function-name "${LAMBDA_NAME}" \
  --region "${AWS_REGION}" \
  --query 'PackageType'
```

```bash
aws lambda get-function --function-name "${LAMBDA_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1 && EXISTS=1 || EXISTS=0

if [ "$EXISTS" -eq 0 ]; then
  aws lambda create-function \
    --function-name "${LAMBDA_NAME}" \
    --package-type Image \
    --code ImageUri="${IMAGE_URI}" \
    --role "${LAMBDA_ROLE_ARN}" \
    --timeout 900 \
    --memory-size 10240 \
    --ephemeral-storage '{"Size": 10240}' \
    --vpc-config SubnetIds="${LAMBDA_SUBNETS}",SecurityGroupIds="${LAMBDA_SECURITY_GROUPS}" \
    --environment 'Variables={SOURCE_BUCKET=stall-munezero-final-project,KEY_SUFFIX=.tar.zst,REFRESH_STATS_AFTER_INGEST=true,MAX_LINES=0,MAX_OBJECT_MB=8192}' \
    --region "${AWS_REGION}"
else
  aws lambda update-function-code \
    --function-name "${LAMBDA_NAME}" \
    --image-uri "${IMAGE_URI}" \
    --region "${AWS_REGION}"

  aws lambda update-function-configuration \
    --function-name "${LAMBDA_NAME}" \
    --timeout 900 \
    --memory-size 10240 \
    --ephemeral-storage '{"Size": 10240}' \
    --vpc-config SubnetIds="${LAMBDA_SUBNETS}",SecurityGroupIds="${LAMBDA_SECURITY_GROUPS}" \
    --environment 'Variables={SOURCE_BUCKET=stall-munezero-final-project,KEY_SUFFIX=.tar.zst,REFRESH_STATS_AFTER_INGEST=true,MAX_LINES=0,MAX_OBJECT_MB=8192}' \
    --region "${AWS_REGION}"
fi
```

### 7.3a If existing function is `Zip`

Option A (recommended): create a new image-based function name and switch S3 trigger to it.

```bash
export NEW_LAMBDA_NAME="listenbrainz-v2-s3-ingest-img"

aws lambda create-function \
  --function-name "${NEW_LAMBDA_NAME}" \
  --package-type Image \
  --code ImageUri="${IMAGE_URI}" \
  --role "${LAMBDA_ROLE_ARN}" \
  --timeout 900 \
  --memory-size 10240 \
  --ephemeral-storage '{"Size": 10240}' \
  --vpc-config SubnetIds="${LAMBDA_SUBNETS}",SecurityGroupIds="${LAMBDA_SECURITY_GROUPS}" \
  --environment 'Variables={SOURCE_BUCKET=stall-munezero-final-project,KEY_SUFFIX=.tar.zst,REFRESH_STATS_AFTER_INGEST=true,MAX_LINES=0,MAX_OBJECT_MB=8192}' \
  --region "${AWS_REGION}"
```

Option B: keep same function name by deleting and recreating it as `Image`.

```bash
aws lambda delete-function --function-name "${LAMBDA_NAME}" --region "${AWS_REGION}"

aws lambda create-function \
  --function-name "${LAMBDA_NAME}" \
  --package-type Image \
  --code ImageUri="${IMAGE_URI}" \
  --role "${LAMBDA_ROLE_ARN}" \
  --timeout 900 \
  --memory-size 10240 \
  --ephemeral-storage '{"Size": 10240}' \
  --vpc-config SubnetIds="${LAMBDA_SUBNETS}",SecurityGroupIds="${LAMBDA_SECURITY_GROUPS}" \
  --environment 'Variables={SOURCE_BUCKET=stall-munezero-final-project,KEY_SUFFIX=.tar.zst,REFRESH_STATS_AFTER_INGEST=true,MAX_LINES=0,MAX_OBJECT_MB=8192}' \
  --region "${AWS_REGION}"
```

### 7.4 Allow S3 to invoke Lambda

```bash
aws lambda add-permission \
  --function-name "${LAMBDA_NAME}" \
  --statement-id "allow-s3-${BUCKET_NAME}-invoke" \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn "arn:aws:s3:::${BUCKET_NAME}" \
  --region "${AWS_REGION}" || true
```

### 7.5 Attach S3 event notification for `*.tar.zst`

```bash
LAMBDA_ARN="$(aws lambda get-function --function-name "${LAMBDA_NAME}" --query 'Configuration.FunctionArn' --output text --region "${AWS_REGION}")"

cat > /tmp/s3-lambda-notification.json <<EOF
{
  "LambdaFunctionConfigurations": [
    {
      "Id": "listenbrainz-v2-ingest",
      "LambdaFunctionArn": "${LAMBDA_ARN}",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {"Name": "suffix", "Value": ".tar.zst"}
          ]
        }
      }
    }
  ]
}
EOF

aws s3api put-bucket-notification-configuration \
  --bucket "${BUCKET_NAME}" \
  --notification-configuration file:///tmp/s3-lambda-notification.json \
  --region "${AWS_REGION}"
```

### 7.6 Minimum IAM permissions for Lambda execution role

- `s3:GetObject` on `arn:aws:s3:::stall-munezero-final-project/*`
- `secretsmanager:GetSecretValue` on the DB secret used by `load_db_credentials()`
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`
- `ec2:CreateNetworkInterface`, `ec2:DescribeNetworkInterfaces`, `ec2:DeleteNetworkInterface` (for VPC Lambda)

### 7.7 Quick test by uploading one dump

```bash
aws s3 cp \
  "/path/to/listenbrainz-listens-dump-xxxx.tar.zst" \
  "s3://${BUCKET_NAME}/listenbrainz/incremental/"
```

Then monitor CloudWatch logs for function `${LAMBDA_NAME}`.

## 7.8 Local Image Build/Push + Lambda Update

```bash
cd "/Users/didiermunezero/Documents/NU/Junior/DE 300/de300-2026wi-stall/project/local"

export AWS_REGION="us-east-1"
export AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export ECR_REPO="listenbrainz-v2-lambda"
export IMAGE_TAG="$(date +%Y%m%d-%H%M%S)"
export IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}"
export LAMBDA_NAME="listenbrainz-v2-s3-ingest"

# 1) Ensure ECR repo exists

aws ecr describe-repositories --repository-names "${ECR_REPO}" --region "${AWS_REGION}" >/dev/null 2>&1 || \
aws ecr create-repository --repository-name "${ECR_REPO}" --region "${AWS_REGION}"

# 2) Login Docker to ECR

aws ecr get-login-password --region "${AWS_REGION}" | \
docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

# 3) Build locally

docker build -f lambda_v2.Dockerfile -t "${ECR_REPO}:${IMAGE_TAG}" .

# 4) Tag + push

docker tag "${ECR_REPO}:${IMAGE_TAG}" "${IMAGE_URI}"
docker push "${IMAGE_URI}"

# 5) Point Lambda to new image

aws lambda update-function-code \
  --function-name "${LAMBDA_NAME}" \
  --image-uri "${IMAGE_URI}" \
  --region "${AWS_REGION}"
```
