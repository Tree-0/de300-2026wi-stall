"""
ListenBrainz dump parsing pipeline.

Core functions:
  - iter_listens_from_tar_zst(path)  — low-level line generator
  - parse_dump(source, max_lines)    — builds daily aggregates from a .tar.zst
  - upsert_aggregates(conn, ...)     — bulk INSERT ON CONFLICT into Postgres
  - compute_daily_stats(conn_params)  — rolling-window stats via PySpark
"""

from __future__ import annotations

import re
import tarfile
from collections import defaultdict
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING

import orjson
import zstandard as zstd
from tqdm import tqdm

if TYPE_CHECKING:
    import psycopg2.extensions


def day_from_unix(ts: int) -> str:
    """Convert a unix timestamp to an ISO-8601 date string (UTC)."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()


def _get_any(d, *keys):
    """Return the value of the first matching key in *d*, or None."""
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d[k]
    return None


def iter_listens_from_tar_zst(path: Path):
    """
    Yield ``(member_name, line_bytes)`` for every non-empty line in
    ``*.listens`` files inside a ``tar.zst`` archive at *path*.

    Streams decompression — the full archive is never held in memory.
    """
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


def parse_dump(
    source: Path | str,
    max_lines: int = 0,
) -> tuple[dict, dict, dict, dict, dict]:
    """
    Parse a ListenBrainz ``.tar.zst`` dump and return daily aggregates.

    Parameters
    ----------
    source : Path or str
        Path to the local ``.tar.zst`` file.
    max_lines : int
        Stop after this many lines (0 = no limit).

    Returns
    -------
    track_daily : dict[(day, recording_id), int]
    artist_daily : dict[(day, artist_mbid), int]
    track_info : dict[recording_id, (track_name, artist_mbids, release_name)]
    artist_info : dict[artist_mbid, artist_name]
    summary : dict  — parse quality counters
    """
    source = Path(source)

    track_daily: dict = defaultdict(int)
    artist_daily: dict = defaultdict(int)
    track_info: dict = {}
    artist_info: dict = {}

    lines = 0
    bad_json = 0
    missing_ts = 0
    missing_artist = 0

    for _, line in tqdm(iter_listens_from_tar_zst(source), desc="Parsing listens"):
        lines += 1
        if max_lines and lines > max_lines:
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

        artist_mbids = _get_any(add, "artist_mbids") or []
        track_name = tm.get("track_name")
        release_name = tm.get("release_name")
        artist_name = tm.get("artist_name")

        recording_id = f"{'_'.join(artist_mbids)}_{track_name}"

        track_daily[(day, recording_id)] += 1
        if recording_id not in track_info:
            track_info[recording_id] = (track_name, artist_mbids, release_name)

        if isinstance(artist_mbids, list):
            for ambid in artist_mbids:
                if not ambid:
                    continue
                artist_daily[(day, ambid)] += 1
                if ambid not in artist_info and artist_name:
                    artist_info[ambid] = artist_name
        else:
            if not artist_mbids:
                missing_artist += 1

    summary = {
        "lines_parsed": lines,
        "bad_json": bad_json,
        "missing_timestamp": missing_ts,
        "missing_artist_mbid": missing_artist,
        "unique_track_day_keys": len(track_daily),
        "unique_artist_day_keys": len(artist_daily),
        "unique_tracks": len({k[1] for k in track_daily}),
        "unique_artists": len({k[1] for k in artist_daily}),
    }

    return track_daily, artist_daily, track_info, artist_info, summary


# ---------------------------------------------------------------------------
# Bulk upsert into Postgres
# ---------------------------------------------------------------------------

def _chunked(iterable, n: int):
    """Yield successive *n*-sized lists from *iterable*."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch


def upsert_aggregates(
    conn: psycopg2.extensions.connection,
    track_daily: dict,
    artist_daily: dict,
    track_info: dict,
    artist_info: dict,
    dump_path: str | Path = "",
    chunk_size: int = 20_000,
) -> None:
    """
    Bulk-upsert parsed aggregates into Postgres.

    Uses ``psycopg2.extras.execute_values`` for performance.
    Listen counts are *additive* on conflict so re-running is safe.

    Parameters
    ----------
    conn : psycopg2 connection (autocommit should be *False*)
    track_daily : dict[(day, recording_id), int]
    artist_daily : dict[(day, artist_mbid), int]
    track_info : dict[recording_id, (track_name, artist_mbids, release_name)]
    artist_info : dict[artist_mbid, artist_name]
    dump_path : str or Path
        Used to extract the dump-id and update ``ingestion_state``.
    chunk_size : int
        Rows per ``execute_values`` call.
    """
    import time
    from psycopg2.extras import execute_values

    dump_path = str(dump_path)
    m = re.search(r"dump-(\d+)-", dump_path)
    dump_id = m.group(1) if m else None

    conn.autocommit = False
    with conn, conn.cursor() as cur:
        # --- artist_info ---
        t0 = time.perf_counter()
        n_artists = len(artist_info)
        print(f"  Upserting artist_info ({n_artists:,} rows)...", end=" ", flush=True)
        for batch in _chunked(
            [(k, v) for k, v in artist_info.items()], chunk_size
        ):
            execute_values(
                cur,
                """
                INSERT INTO artist_info (artist_mbid, artist_name)
                VALUES %s
                ON CONFLICT (artist_mbid) DO UPDATE
                  SET artist_name = COALESCE(EXCLUDED.artist_name,
                                             artist_info.artist_name)
                """,
                batch,
            )
        print(f"done ({time.perf_counter() - t0:.1f}s)")

        # --- track_info ---
        t0 = time.perf_counter()
        n_tracks = len(track_info)
        print(f"  Upserting track_info ({n_tracks:,} rows)...", end=" ", flush=True)
        for batch in _chunked(
            [(k, v[0], v[1], v[2]) for k, v in track_info.items()], chunk_size
        ):
            execute_values(
                cur,
                """
                INSERT INTO track_info (recording_id, track_name,
                                        artist_mbids, release_name)
                VALUES %s
                ON CONFLICT (recording_id) DO UPDATE
                  SET track_name   = COALESCE(EXCLUDED.track_name,
                                              track_info.track_name),
                      artist_mbids = COALESCE(EXCLUDED.artist_mbids,
                                              track_info.artist_mbids),
                      release_name = COALESCE(EXCLUDED.release_name,
                                              track_info.release_name)
                """,
                batch,
            )
        print(f"done ({time.perf_counter() - t0:.1f}s)")

        # --- track_daily_listens (additive) ---
        t0 = time.perf_counter()
        n_track_daily = len(track_daily)
        print(f"  Upserting track_daily_listens ({n_track_daily:,} rows)...", end=" ", flush=True)
        for batch in _chunked(
            [(day, rid, cnt) for (day, rid), cnt in track_daily.items()],
            50_000,
        ):
            execute_values(
                cur,
                """
                INSERT INTO track_daily_listens (day, recording_id, listen_count)
                VALUES %s
                ON CONFLICT (day, recording_id) DO UPDATE
                  SET listen_count = track_daily_listens.listen_count
                                   + EXCLUDED.listen_count
                """,
                batch,
            )
        print(f"done ({time.perf_counter() - t0:.1f}s)")

        # --- artist_daily_listens (additive) ---
        t0 = time.perf_counter()
        n_artist_daily = len(artist_daily)
        print(f"  Upserting artist_daily_listens ({n_artist_daily:,} rows)...", end=" ", flush=True)
        for batch in _chunked(
            [(day, mbid, cnt) for (day, mbid), cnt in artist_daily.items()],
            50_000,
        ):
            execute_values(
                cur,
                """
                INSERT INTO artist_daily_listens (day, artist_mbid, listen_count)
                VALUES %s
                ON CONFLICT (day, artist_mbid) DO UPDATE
                  SET listen_count = artist_daily_listens.listen_count
                                   + EXCLUDED.listen_count
                """,
                batch,
            )
        print(f"done ({time.perf_counter() - t0:.1f}s)")

        # --- ingestion_state ---
        cur.execute(
            """
            UPDATE ingestion_state
               SET last_dump_id = %s, last_dump_path = %s, loaded_at = now()
             WHERE id = 1
            """,
            (dump_id, dump_path),
        )

    print("Upserts complete.")


# ---------------------------------------------------------------------------
# Compute daily stats via PySpark
# ---------------------------------------------------------------------------

def _compute_entity_stats(spark, jdbc_url: str, jdbc_props: dict,
                          table_in: str, table_out: str, id_col: str) -> int:
    """
    Shared logic for artist / track rolling-window stat computation.

    Returns the number of stat rows written.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df = spark.read.jdbc(url=jdbc_url, table=table_in, properties=jdbc_props)
    print(f"Loaded {df.count():,} rows from {table_in}")

    df = df.sort(id_col, "day")

    w_unbounded = Window.partitionBy(id_col).orderBy("day").rowsBetween(
        Window.unboundedPreceding, 0
    )
    w_7d = Window.partitionBy(id_col).orderBy("day").rowsBetween(-6, 0)
    w_30d = Window.partitionBy(id_col).orderBy("day").rowsBetween(-29, 0)
    w_lag = Window.partitionBy(id_col).orderBy("day")

    df = (
        df
        .withColumn("cumulative_listen_count", F.sum("listen_count").over(w_unbounded))
        .withColumn("listen_count_past_7_days", F.sum("listen_count").over(w_7d))
        .withColumn("listen_count_past_30_days", F.sum("listen_count").over(w_30d))
        .withColumn("cumulative_yesterday", F.lag("cumulative_listen_count", 1).over(w_lag))
        .withColumn(
            "growth_rate",
            F.when(
                F.col("cumulative_yesterday").isNotNull()
                & (F.col("cumulative_yesterday") > 0),
                F.col("listen_count") / F.col("cumulative_yesterday"),
            ).otherwise(None),
        )
        .withColumn(
            "growth_percentile",
            F.percent_rank().over(
                Window.partitionBy("day").orderBy(F.col("growth_rate").asc_nulls_first())
            ),
        )
        .withColumn(
            "listen_pctl_past_7_days",
            F.percent_rank().over(
                Window.partitionBy("day").orderBy("listen_count_past_7_days")
            ),
        )
        .withColumn(
            "listen_pctl_past_30_days",
            F.percent_rank().over(
                Window.partitionBy("day").orderBy("listen_count_past_30_days")
            ),
        )
    )

    stats = df.select(
        "day", id_col,
        "growth_percentile", "cumulative_listen_count",
        "listen_count_past_7_days", "listen_pctl_past_7_days",
        "listen_count_past_30_days", "listen_pctl_past_30_days",
    )

    stats.write.jdbc(url=jdbc_url, table=table_out, mode="overwrite",
                     properties=jdbc_props)
    count = stats.count()
    print(f"Wrote {count:,} rows to {table_out}")
    return count


def compute_daily_stats(conn_params: dict, spark=None) -> None:
    """
    Compute rolling-window stats for artists and tracks.

    Parameters
    ----------
    conn_params : dict
        Keys: host, port, dbname, user, password  (as returned by
        ``utils.load_db_credentials()``).
    spark : SparkSession, optional
        If *None*, a local session is created (and stopped at the end).
    """
    from pyspark.sql import SparkSession

    own_spark = spark is None
    if own_spark:
        spark = (
            SparkSession.builder
            .appName("ListenBrainz Stats")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
            .config("spark.driver.memory", "4g")
            .getOrCreate()
        )

    jdbc_url = (
        f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}"
        f"/{conn_params['dbname']}"
    )
    jdbc_props = {
        "user": conn_params["user"],
        "password": conn_params["password"],
        "driver": "org.postgresql.Driver",
    }

    try:
        _compute_entity_stats(
            spark, jdbc_url, jdbc_props,
            "artist_daily_listens", "artist_daily_stats", "artist_mbid",
        )
        _compute_entity_stats(
            spark, jdbc_url, jdbc_props,
            "track_daily_listens", "track_daily_stats", "recording_id",
        )
    finally:
        if own_spark:
            spark.stop()
            print("Spark session stopped.")
