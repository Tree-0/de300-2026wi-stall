# AWS ListenBrainz Ingestion Pipeline - Setup Guide

## Architecture Overview

```
S3 (Dump Files)
    ↓
EC2 Instance (your code runs here)
    ├─ Download from S3 (boto3)
    ├─ Parse tar.zst (local)
    ├─ Upsert to RDS (psycopg2)
    └─ Compute Stats (PySpark → RDS)
    ↓
RDS PostgreSQL (final data destination)
```

## Prerequisites

- AWS Account with S3, RDS, and EC2 access
- PostgreSQL RDS instance (running)
- S3 bucket with your `.tar.zst` dumps
- Dump files you want to process

## Step-by-Step Setup

### STEP 1: Gather AWS Connection Details

Before launching the EC2 instance, collect these details:

```bash
# RDS Details (from AWS Console → RDS)
RDS_HOST=http://database-1.chm317to06o1.us-east-1.rds.amazonaws.com/
RDS_PORT=5432
RDS_DATABASE=postgres
RDS_USER=postgres
RDS_PASSWORD=password=$(aws secretsmanager get-secret-value --secret-id 'arn:aws:secretsmanager:us-east-1:549787090008:secret:rds!db-6323faa7-77d3-4952-af08-bcd6d623f642-g3XgW6' --query SecretString --output text | jq -r '.password')

# S3 Details
S3_BUCKET=stall-munezero-final-project
S3_DUMP_KEY=listenbrainz/incremental/listenbrainz-listens-dump-2400-20260118-000003-incremental.tar.zst

# Your AWS Region
AWS_REGION=us-east-1
```

---

### STEP 2: Create/Configure EC2 Instance

#### Option A: Launch a New EC2 Instance (Recommended)

1. **AWS Console → EC2 → Launch Instances**
   - **AMI**: Ubuntu 22.04 LTS (ami-0c55b159cbfafe1f0 or latest)
   - **Instance Type**: `t3.xlarge` (4 vCPU, 16GB RAM - good for Spark)
   - **Storage**: 100GB EBS volume (for temp tar extraction)
   - **Security Group**:
     - Inbound: SSH (port 22) from your IP
     - Outbound: Allow all (for S3, RDS, package downloads)
   - **IAM Instance Profile**: Create or select one with policies below

2. **Attach IAM Policies** (create if needed):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:ListBucket"],
         "Resource": [
           "arn:aws:s3:::your-bucket-name",
           "arn:aws:s3:::your-bucket-name/*"
         ]
       },
       {
         "Effect": "Allow",
         "Action": [
           "logs:CreateLogGroup",
           "logs:CreateLogStream",
           "logs:PutLogEvents"
         ],
         "Resource": "arn:aws:logs:*:*:*"
       }
     ]
   }
   ```

3. **Security Group for RDS**:
   - Ensure RDS security group allows inbound traffic on port 5432 from the EC2 instance's security group
   - **RDS Console → Security Groups → Edit Inbound Rules**
     - Add rule: PostgreSQL (port 5432) from EC2 instance's security group

---

### STEP 3: SSH into EC2 and Install Dependencies

```bash
# SSH into your instance
ssh -i your-key.pem ubuntu@your-ec2-public-ip

# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install Python 3.10+ and dependencies
sudo apt-get install -y python3 python3-pip python3-venv git
sudo apt-get install -y openjdk-11-jdk  # Required for Spark/PySpark
sudo apt-get install -y build-essential libssl-dev libffi-dev

# Create virtual environment
python3 -m venv ~/listenbrainz_env
source ~/listenbrainz_env/bin/activate

# Install Python packages
pip install --upgrade pip
pip install boto3 psycopg2-binary pandas zstandard orjson tqdm pyspark

# Verify Spark installation
python3 -c "from pyspark.sql import SparkSession; print(SparkSession.builder.getOrCreate().version)"
```

---

### STEP 4: Upload and Configure the Script

```bash
# On your LOCAL machine, upload the script to EC2
scp -i your-key.pem /path/to/aws_listenbrainz_ingest.py ubuntu@your-ec2-public-ip:~/

# SSH back in
ssh -i your-key.pem ubuntu@your-ec2-public-ip

# Edit the configuration at the top of the script
nano ~/aws_listenbrainz_ingest.py
```

Update these values in the script:

```python
# S3 Configuration
S3_BUCKET = "your-actual-bucket-name"
S3_DUMP_KEY = "musicbrainz/listenbrainz-listens-dump-XXXX-YYYYMMDD-ZZZZZZ-incremental.tar.zst"

# RDS Configuration
RDS_HOST = "your-actual-rds-endpoint.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DATABASE = "newsfeed"
RDS_USER = "postgres"
RDS_PASSWORD = "your-actual-password"  # Or use env var (below)

# AWS Region
AWS_REGION = "us-east-1"  # Change if needed
```

---

### STEP 5: Set Environment Variables (Secure Method)

Instead of hardcoding password, set it as environment variable:

```bash
# In EC2 session
export RDS_PASSWORD="your-actual-rds-password"

# Verify
echo $RDS_PASSWORD
```

Then in the script, change:

```python
RDS_PASSWORD = os.getenv("RDS_PASSWORD")  # Reads from environment
```

---

### STEP 6: Create Logging Directory

```bash
# EC2 session
sudo mkdir -p /var/log/listenbrainz
sudo chown $USER:$USER /var/log/listenbrainz
```

---

### STEP 7: Run the Pipeline (First Test)

```bash
# Activate virtual environment
source ~/listenbrainz_env/bin/activate

# Optional: Test with limited records (for quick validation)
export MAX_LINES=50000
export RDS_PASSWORD="your-password"

# Run the pipeline
python3 ~/aws_listenbrainz_ingest.py

# Monitor logs
tail -f /var/log/listenbrainz_ingest.log
```

**Expected Output:**

```
2026-02-20 15:30:45 - INFO - Starting ListenBrainz AWS Ingestion Pipeline
2026-02-20 15:30:46 - INFO - Downloading s3://your-bucket/musicbrainz/listenbrainz-listens-dump-...
2026-02-20 15:31:00 - INFO - Downloaded to /tmp/listenbrainz/listenbrainz-listens-dump-...
2026-02-20 15:31:05 - INFO - Connected to RDS: postgres@your-instance.c9akciq32.us-east-1.rds.amazonaws.com:5432/newsfeed
2026-02-20 15:31:06 - INFO - Tables created/verified
2026-02-20 15:31:15 - INFO - Parsing .../listens.tar
Parse listens: 45000it [00:10, 4500.23it/s]
2026-02-20 15:31:25 - INFO - Upserting artist_info...
2026-02-20 15:31:30 - INFO - Upserting track_info...
2026-02-20 15:31:35 - INFO - Upserting track_daily_listens...
2026-02-20 15:31:40 - INFO - Upserting artist_daily_listens...
2026-02-20 15:32:00 - INFO - Starting Spark session for stats computation...
2026-02-20 15:32:15 - INFO - Computing artist stats...
2026-02-20 15:32:45 - INFO - Wrote 1,250 artist stats
2026-02-20 15:32:50 - INFO - Computing track stats...
2026-02-20 15:33:10 - INFO - Wrote 850 track stats
2026-02-20 15:33:15 - INFO - Spark session closed
2026-02-20 15:33:16 - INFO - Pipeline completed successfully!
```

---

### STEP 8: Verify Data in RDS

```bash
# From your LOCAL machine with psql, or from EC2:
psql -h your-instance.c9akciq32.us-east-1.rds.amazonaws.com \
     -U postgres \
     -d newsfeed \
     -c "SELECT COUNT(*) FROM artist_daily_listens;"

# Expected: Should show row count > 0

psql -h your-instance.c9akciq32.us-east-1.rds.amazonaws.com \
     -U postgres \
     -d newsfeed \
     -c "SELECT COUNT(*) FROM artist_daily_stats;"

# Expected: Should show computed stats
```

---

### STEP 9: Process Additional Dumps

Once the first one works, process more dumps:

```bash
# In EC2, edit to point to next dump
nano ~/aws_listenbrainz_ingest.py
# Change S3_DUMP_KEY to next file

export RDS_PASSWORD="your-password"
python3 ~/aws_listenbrainz_ingest.py
```

---

## Troubleshooting

### Error: S3 Connection Refused

- **Check**: EC2 has internet access (security group outbound rule)
- **Check**: EC2 IAM role has S3 permissions

```bash
aws s3 ls s3://your-bucket/  # Should work from EC2
```

### Error: RDS Connection Failed

- **Check**: RDS security group allows port 5432 from EC2 instance
- **Check**: RDS endpoint is correct (copy from AWS Console)
- **Test**: `psql -h RDS_HOST -U RDS_USER -d RDS_DATABASE`

### Error: Not enough memory for Spark

- Use smaller instance type first with `MAX_LINES=10000`
- Or increase Spark driver memory in script:

```python
.config("spark.driver.memory", "2g")  # Reduce from 4g
```

### Error: "Column ... does not exist"

- Tables might have old schema; Re-run after dropping:

```sql
DROP TABLE IF EXISTS track_daily_stats CASCADE;
DROP TABLE IF EXISTS artist_daily_stats CASCADE;
DROP TABLE IF EXISTS track_daily_listens CASCADE;
DROP TABLE IF EXISTS artist_daily_listens CASCADE;
DROP TABLE IF EXISTS track_info CASCADE;
DROP TABLE IF EXISTS artist_info CASCADE;
```

---

## Next Steps (Later Automation)

Once you verify it works manually, we can automate with:

1. **AWS Lambda** + **EventBridge**: Schedule daily runs
2. **CloudWatch Logs**: Monitor execution
3. **SNS Notifications**: Email on success/failure
4. **Step Functions**: Orchestrate multi-dump processing

For now, you have a working manual pipeline! 🚀
