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
  "/Users/didiermunezero/Documents/NU/Junior/DE 300/de300-2026wi-munezero/Final Project/datadumps/musicbrainz/listenbrainz-listens-dump-2428-20260213-000003-incremental.tar.zst" \
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
