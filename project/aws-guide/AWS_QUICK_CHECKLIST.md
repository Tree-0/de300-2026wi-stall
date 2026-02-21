# AWS ListenBrainz Setup - Quick Checklist

## Pre-Launch Checklist

- [ ] AWS account with access to S3, RDS, EC2
- [ ] RDS PostgreSQL instance running
- [ ] S3 bucket with `.tar.zst` dump files uploaded
- [ ] Collected RDS connection details:
  - [ ] Host: `_____________________`
  - [ ] Port: `5432`
  - [ ] Database: `_____________________`
  - [ ] User: `_____________________`
  - [ ] Password: `_____________________`
- [ ] Identified S3 dump location:
  - [ ] Bucket: `_____________________`
  - [ ] Key: `_____________________`

---

## EC2 Setup Checklist

- [ ] **Launch EC2 Instance**
  - Type: `t3.xlarge` (4vCPU, 16GB RAM)
  - AMI: Ubuntu 22.04 LTS
  - Storage: 100GB
  - [ ] Security group allows SSH (port 22) inbound
  - [ ] Security group allows outbound to S3, RDS

- [ ] **Configure RDS Security Group**
  - [ ] Allow inbound PostgreSQL (5432) from EC2 instance

- [ ] **Create/Attach IAM Role**
  - [ ] S3 GetObject, ListBucket permissions
  - [ ] CloudWatch Logs permissions

---

## Installation on EC2 Checklist

```bash
# Copy-paste these commands in order:

# 1. Update system
sudo apt-get update && sudo apt-get upgrade -y

# 2. Install dependencies
sudo apt-get install -y python3 python3-pip python3-venv git openjdk-11-jdk build-essential

# 3. Create virtual environment
python3 -m venv ~/listenbrainz_env
source ~/listenbrainz_env/bin/activate

# 4. Install Python packages
pip install --upgrade pip
pip install boto3 psycopg2-binary pandas zstandard orjson tqdm pyspark

# 5. Create logging directory
sudo mkdir -p /var/log/listenbrainz
sudo chown $USER:$USER /var/log/listenbrainz
```

---

## Configuration Checklist

- [ ] **Upload script to EC2**: `scp aws_listenbrainz_ingest.py ubuntu@IP:~/`

- [ ] **Edit script with your values**:

  ```bash
  nano ~/aws_listenbrainz_ingest.py
  ```

  - [ ] `S3_BUCKET` = your bucket name
  - [ ] `S3_DUMP_KEY` = path to dump file
  - [ ] `RDS_HOST` = your RDS endpoint
  - [ ] `RDS_DATABASE` = your DB name
  - [ ] `RDS_USER` = your RDS user
  - [ ] `AWS_REGION` = your region (e.g., `us-east-1`)

- [ ] **Set environment variable**:
  ```bash
  export RDS_PASSWORD="your-password"
  ```

---

## First Run Checklist

- [ ] **Optional: Test with limited data**:

  ```bash
  export MAX_LINES=50000
  python3 ~/aws_listenbrainz_ingest.py
  ```

- [ ] **Full run**:

  ```bash
  export RDS_PASSWORD="your-password"
  source ~/listenbrainz_env/bin/activate
  python3 ~/aws_listenbrainz_ingest.py
  ```

- [ ] **Monitor logs**:
  ```bash
  tail -f /var/log/listenbrainz_ingest.log
  ```

---

## Validation Checklist

- [ ] **Check data was inserted**:

  ```bash
  psql -h RDS_HOST -U postgres -d newsfeed \
    -c "SELECT COUNT(*) as rows FROM artist_daily_listens;"
  ```

  Expected: `rows > 0`

- [ ] **Check stats were computed**:

  ```bash
  psql -h RDS_HOST -U postgres -d newsfeed \
    -c "SELECT COUNT(*) as rows FROM artist_daily_stats;"
  ```

  Expected: `rows > 0`

- [ ] **Verify logs show success**:
  ```bash
  grep "Pipeline completed successfully" /var/log/listenbrainz_ingest.log
  ```

---

## Estimated Costs

| Service           | Cost           | Notes                         |
| ----------------- | -------------- | ----------------------------- |
| EC2 t3.xlarge     | ~$0.17/hr      | ~$4/day, 24hr run ~$120/month |
| RDS (existing)    | Already paying | No additional cost            |
| S3                | ~$0.02/GB      | Data transfer OUT only        |
| **Total per run** | **< $1**       | One-time per 30-day dump      |

---

## Next Steps After First Success

1. ✅ Test with single dump
2. 🔄 Process remaining 30 days of dumps
3. 🔔 Set up EventBridge/Lambda for daily automation
4. 📊 Create dashboards in QuickSight
5. 📧 Add SNS notifications for pipeline status

---

## Commands Reference

```bash
# Activate environment
source ~/listenbrainz_env/bin/activate

# Set password (do this each session)
export RDS_PASSWORD="your-password"

# Run pipeline
python3 ~/aws_listenbrainz_ingest.py

# Monitor
tail -f /var/log/listenbrainz_ingest.log

# Test DB connection
psql -h $RDS_HOST -U postgres -d newsfeed -c "SELECT version();"

# List S3 dumps
aws s3 ls s3://your-bucket/musicbrainz/ --recursive | grep "listenbrainz-listens"

# Check disk space on EC2
df -h

# Free up space (delete temp files)
rm -rf /tmp/listenbrainz/*
```

---

## Support

If you encounter issues, check:

1. `/var/log/listenbrainz_ingest.log` for detailed errors
2. Verify AWS security groups allow traffic
3. Confirm RDS connection works: `psql -h ENDPOINT -U USER -d DB -c "SELECT 1;"`
4. Check EC2 IAM role has S3 permissions: `aws s3 ls` from EC2
