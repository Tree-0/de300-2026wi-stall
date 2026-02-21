# Local vs AWS Code Comparison

## What Changed (and Why)

### 1. Data Source: Local File → S3

**Local (Jupyter Notebook):**

```python
DUMP_PATH = Path("/Users/didiermunezero/Documents/NU/Junior/DE 300/.../dump.tar.zst")
# Reads directly from local disk
```

**AWS:**

```python
S3_BUCKET = "your-bucket-name"
S3_DUMP_KEY = "musicbrainz/dump.tar.zst"
# Downloads from S3 to /tmp/listenbrainz/ on EC2
download_s3_dump(bucket, key, local_path)
```

**Why**:

- Dump files are too large to store on EC2 root volume
- S3 is cheaper for storage
- Multiple EC2 instances can process different dumps in parallel

---

### 2. Database Connection: Env Vars → RDS Endpoint

**Local:**

```python
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
```

**AWS:**

```python
RDS_HOST = "your-instance.c9akciq32.us-east-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_PASSWORD = os.getenv("RDS_PASSWORD")  # Set in EC2 environment
```

**Why**:

- Local uses `localhost` (your machine)
- AWS RDS has a network endpoint (publicly accessible if configured)
- Password kept in environment variable (never hardcode secrets)

---

### 3. Processing: Jupyter Cells → Standalone Script

**Local:**

- Multiple Jupyter cells to run manually
- Interactive (run one cell, inspect, run next)
- Good for exploration/debugging

**AWS:**

```python
def main():
    # 1. Download from S3
    # 2. Create DB tables
    # 3. Parse dump
    # 4. Upsert data
    # 5. Compute stats
    # 6. Cleanup
```

**Why**:

- AWS runs unattended on EC2
- No interactive terminal/Jupyter
- Single script execution with error handling
- Logging to file instead of cell outputs

---

### 4. Logging: Print → Structured Logs

**Local:**

```python
print("Lines parsed:", lines)
```

**AWS:**

```python
logger.info(f"Parsed {lines:,} records")
```

Logs written to: `/var/log/listenbrainz_ingest.log`

**Why**:

- Persistent logs survive EC2 termination
- Timestamped for debugging
- Can be shipped to CloudWatch
- Can be analyzed for performance trends

---

### 5. Spark Configuration: Local → Remote via JDBC

**Local:**

```python
# PySpark on local machine, reads/writes locally
spark.read.jdbc(url=jdbc_url, table="track_daily_listens")
```

**AWS:**

```python
# Same JDBC but:
# - Spark runs ON EC2 instance
# - Connects to RDS (not localhost)
# - More parallelization possible (with multiple executors)

jdbc_url = f"jdbc:postgresql://{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
spark_config = f"spark.driver.memory=4g"  # EC2 memory
```

**Why**:

- EC2 has more CPU/RAM than your laptop
- Spark can be distributed if needed
- RDS is purpose-built database server

---

### 6. Error Handling: Optional → Required

**Local:**

```python
try:
    rec = orjson.loads(line)
except Exception:
    bad_json += 1
    continue
```

**AWS:**

```python
try:
    success = main()
    sys.exit(0 if success else 1)
except Exception as e:
    logger.error(f"Pipeline failed: {e}", exc_info=True)
    sys.exit(1)
```

**Why**:

- Unattended process needs clear success/failure status
- Exit codes (0 = success, 1 = failure) for CI/CD
- Full stack traces logged for debugging
- Cleanup happens even if errors occur

---

### 7. Cleanup: Manual → Automatic

**Local:**

- Manually delete `/tmp/listenbrainz/dump.tar.zst`

**AWS:**

```python
finally:
    if os.path.exists(local_dump):
        os.remove(local_dump)
        logger.info(f"Cleaned up {local_dump}")
```

**Why**:

- EC2 storage is limited/paid
- Prevents disk space issues on subsequent runs
- Happens even if script exits early

---

## Performance Differences

### Local (Jupyter)

- **Processing Speed**: Depends on your laptop CPU/RAM
- **Network**: Direct file access (fast local SSD)
- **Cost**: Electric bill + your time

### AWS (EC2 + RDS)

- **Processing Speed**: t3.xlarge = 4 CPUs, 16GB RAM (faster than typical laptop)
- **Network**: S3 → EC2 → RDS (data travels within AWS VPC, very fast)
- **Cost**: ~$0.17/hr for t3.xlarge (test with 50k records = ~5 min = ~$0.01)

**30-day Pipeline Estimate:**

- Parse 30 files: ~2-3 hours total
- Compute stats with Spark: ~1 hour total
- **Total**: ~4 hours = ~$0.70 in EC2 costs (+ RDS which you already pay)

---

## Code Structure

### Local: Monolithic Notebook

```
Cells 1-20: Setup, import, config
Cells 21-30: Parse and aggregate
Cells 31-35: Upsert to DB
Cells 36-40: Spark stats
Cells 41-42: Validation
```

### AWS: Modular Script

```python
def download_s3_dump()      # S3 integration
def create_tables()          # DDL
def iter_listens_from_tar_zst()  # Streaming parser
def parse_dump()             # Main aggregation logic
def upsert_data()            # Batch inserts
def compute_stats_with_spark()   # Spark jobs
def main()                   # Orchestrator
```

**Why**: Reusability, testability, scheduling

---

## Configuration Differences

### Local (Hardcoded in Notebook)

```python
DUMP_PATH = Path("/exact/path/on/my/machine")
PGHOST = "localhost"
```

### AWS (Config at Top of Script)

```python
S3_BUCKET = "your-bucket-name"        # Must change for each user
S3_DUMP_KEY = "path/to/specific/dump"  # Must change for each dump
RDS_HOST = "your-rds-endpoint"         # Must change for each deployment
RDS_PASSWORD = os.getenv("RDS_PASSWORD")  # Set in environment
```

The AWS script is **parameterized** - you edit the config section once, and it works for different dumps.

---

## Key AWS Concepts Used

### boto3 (AWS SDK for Python)

```python
s3_client = boto3.client('s3', region_name=AWS_REGION)
s3_client.download_file(bucket, key, local_path)
```

- Communicates with S3 using your EC2 IAM role (no keys needed)
- Similar to `aws s3 cp` command but in Python

### JDBC Connection

```python
jdbc_url = f"jdbc:postgresql://{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
spark.read.jdbc(url=jdbc_url, table="track_daily_listens", properties=jdbc_props)
```

- Java Database Connectivity (standard way apps talk to databases)
- Spark includes PostgreSQL JDBC driver automatically
- Works across network (not just localhost)

### IAM Roles (No Credentials in Code!)

```python
# boto3 automatically uses EC2's IAM role
s3_client = boto3.client('s3')  # No AWS_ACCESS_KEY_ID needed!
```

- EC2 instances assume IAM roles
- Credentials are temporary and managed by AWS
- More secure than storing keys in code

---

## Cost Breakdown (30-Day Full Run)

```
EC2 t3.xlarge:
  - Parse 30 dumps: 30 dumps × 5 min = 2.5 hours = $0.42
  - Compute stats: 1 hour = $0.17
  - Overhead: 1 hour = $0.17
  Total EC2: ~$0.76

RDS PostgreSQL:
  - Already running (your existing cost)
  - No additional charge for processing
  - Storage: +few MB per 30-day period = negligible

S3:
  - Storage: 30 × 5GB = 150GB = $3.60 (one-time)
  - Transfer OUT to EC2: ~150GB = $3.00 (free within region)

TOTAL: ~$0.76 + ~$3.60 = ~$4.36 for one 30-day run
```

Then:

- **Next 30-day run**: Only $0.76 (data already in S3)
- **Daily incremental**: $0.10-0.20/day (much smaller files)

---

## Migration Path

1. **Phase 1** (Current): Run manually once on EC2 ✓
2. **Phase 2**: Process all 30 dumps manually
3. **Phase 3**: Add Secrets Manager + SNS notifications
4. **Phase 4**: Setup EventBridge for daily automation
5. **Phase 5**: Add data pipeline visualization + alerts

---

## Questions?

See `AWS_SETUP_GUIDE.md` for detailed step-by-step instructions
See `AWS_QUICK_CHECKLIST.md` for quick reference
