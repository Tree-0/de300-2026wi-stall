"""
Side-by-side v2 identity pipeline for tracks and artists.

Design goals:
1. Keep the original pipeline untouched.
2. Prefer canonical MBIDs when present.
3. Use normalized fallback keys only when canonical IDs are missing.
4. Persist synthetic-key flags for data quality analysis.
5. Persist release date when available in dump metadata.

This module writes to *_v2 tables so you can validate behavior safely.
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING

import orjson
from psycopg2 import errors

from artist_identity import build_alias_map, normalize_artist_token
from pipeline import day_from_unix, iter_listens_from_tar_zst
from utils import connect_postgres, load_db_credentials

if TYPE_CHECKING:
    import psycopg2.extensions


def _get_any(d, *keys):
    if not isinstance(d, dict):
        return None
    for key in keys:
        if key in d:
            return d[key]
    return None


def _chunked(iterable, n: int):
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch


def normalize_text(value) -> str | None:
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", str(value)).strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("||", " ")
    return text or None


def _load_alias_map_from_db(conn: psycopg2.extensions.connection) -> dict[str, str]:
    """Load known artist-name aliases from artist_info_v2 for canonical ID reuse."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT artist_id, artist_name
                FROM artist_info_v2
                WHERE artist_name IS NOT NULL
                """
            )
            rows = cur.fetchall()
        return build_alias_map(rows)
    except errors.UndefinedTable:
        # parse_dump_v2 can run before ensure_v2_tables(); treat as empty alias map.
        conn.rollback()
        return {}


def normalize_artist_mbids(raw_artist_mbids) -> list[str]:
    if isinstance(raw_artist_mbids, list):
        return [str(value).strip() for value in raw_artist_mbids if str(value).strip()]
    if raw_artist_mbids is None:
        return []
    if isinstance(raw_artist_mbids, str):
        value = raw_artist_mbids.strip()
        return [value] if value else []
    return []


def normalize_mbid(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_release_date(value) -> str | None:
    """
    Parse candidate release date to ISO YYYY-MM-DD when possible.
    """
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    # Keep first 10 chars for timestamp-like values.
    if len(text) >= 10 and re.match(r"^\d{4}-\d{2}-\d{2}", text):
        text = text[:10]

    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m",
        "%Y/%m",
        "%Y",
    )
    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt).date()
            if fmt in ("%Y-%m", "%Y/%m"):
                dt = dt.replace(day=1)
            if fmt == "%Y":
                dt = dt.replace(month=1, day=1)
            return dt.isoformat()
        except ValueError:
            continue

    return None


def extract_release_date(track_metadata: dict, additional_info: dict) -> str | None:
    candidates = (
        _get_any(
            additional_info,
            "release_date",
            "first_release_date",
            "date",
            "year",
            "release_year",
            "original_release_date",
        ),
        _get_any(track_metadata, "release_date", "first_release_date", "date"),
    )
    for candidate in candidates:
        parsed = parse_release_date(candidate)
        if parsed:
            return parsed
    return None


def choose_recording_identity(recording_mbid, artist_name, track_name, release_name):
    """
    Return (recording_id, fallback_key, is_synthetic) or (None, None, None) if unusable.
    """
    mbid = normalize_mbid(recording_mbid)
    if mbid:
        return mbid, None, False

    n_artist = normalize_text(artist_name)
    n_track = normalize_text(track_name)
    n_release = normalize_text(release_name)

    if n_artist and n_track and n_release:
        fallback_key = f"{n_artist}||{n_track}||{n_release}"
        return f"fallback::{fallback_key}", fallback_key, True

    return None, None, None


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def split_artist_name_collabs(artist_name) -> list[str]:
    """
    Split artist collab strings into individual artist names.

    Handles common patterns such as:
      - "x feat. y"
      - "x featuring y"
      - "x ft. y"
            - "x with y"
      - "x x y" (common collab marker in some catalogs)

        Intentionally avoids auto-splitting plain commas and "and" because
        many valid single-artist names contain them (e.g. "Tyler, The Creator").
    """
    if not artist_name:
        return []

    text = str(artist_name)
    split_patterns = [
        r"\s*;\s*",
        r"\s*•\s*",
        r"\s+feat\.?\s+",
        r"\s+featuring\s+",
        r"\s+ft\.?\s+",
        r"\s+with\s+",
        r"\s+x\s+",
    ]

    for pattern in split_patterns:
        text = re.sub(pattern, "|", text, flags=re.IGNORECASE)

    parts = [normalize_text(part) for part in text.split("|")]
    parts = [part for part in parts if part]
    return _dedupe_preserve_order(parts)


def extract_artist_name_candidates(track_metadata: dict, additional_info: dict) -> list[str]:
    """
    Build ordered candidate artist names from richer metadata first, then fallback.
    """
    raw_candidates = []

    for key in ("artist_names", "release_artist_names"):
        value = _get_any(additional_info, key)
        if isinstance(value, list):
            raw_candidates.extend([item for item in value if item])
        elif value:
            raw_candidates.append(value)

    artist_name = track_metadata.get("artist_name")
    if artist_name:
        raw_candidates.append(artist_name)

    split_candidates = []
    for candidate in raw_candidates:
        split_candidates.extend(split_artist_name_collabs(candidate))

    return _dedupe_preserve_order(split_candidates)


def has_collab_markers(artist_name) -> bool:
    if not artist_name:
        return False
    text = str(artist_name).lower()
    return bool(
        re.search(
            r"\b(feat\.?|featuring|ft\.?|with)\b|\s+x\s+|;|•|&|,",
            text,
        )
    )


def choose_artist_identities(
    artist_mbids: list[str],
    track_metadata: dict,
    additional_info: dict,
    alias_to_mbid: dict[str, str] | None = None,
):
    """
    Return tuple:
      (identities, used_multi_name_candidates)

    identities is a list of tuples:
      (artist_id, artist_mbid, fallback_key, is_synthetic, artist_name_value)
    """
    candidate_names = extract_artist_name_candidates(track_metadata, additional_info)
    used_multi_name_candidates = len(candidate_names) > 1

    identities = []
    canonical_mbids = []

    for mbid in artist_mbids:
        canon = normalize_mbid(mbid)
        if canon:
            canonical_mbids.append(canon)

    canonical_mbids = _dedupe_preserve_order(canonical_mbids)

    if canonical_mbids:
        for idx, mbid in enumerate(canonical_mbids):
            name_value = candidate_names[idx] if idx < len(candidate_names) else None
            identities.append((mbid, mbid, None, False, name_value))
        return identities, used_multi_name_candidates

    for name_value in candidate_names:
        if alias_to_mbid:
            known_artist_id = alias_to_mbid.get(normalize_artist_token(name_value))
            if known_artist_id:
                identities.append((known_artist_id, known_artist_id, None, False, name_value))
                continue

        fallback_key = name_value
        identities.append((f"fallback_artist::{fallback_key}", None, fallback_key, True, name_value))

    return identities, used_multi_name_candidates


def ensure_v2_tables(conn: psycopg2.extensions.connection) -> None:
    ddl = """
        CREATE TABLE IF NOT EXISTS artist_info_v2 (
            artist_id TEXT PRIMARY KEY,
            artist_mbid TEXT,
            fallback_key TEXT,
            is_synthetic_fallback_key BOOLEAN NOT NULL DEFAULT FALSE,
            artist_name TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_artist_info_v2_artist_mbid
            ON artist_info_v2(artist_mbid);
        CREATE INDEX IF NOT EXISTS idx_artist_info_v2_is_synth
            ON artist_info_v2(is_synthetic_fallback_key);

    CREATE TABLE IF NOT EXISTS track_info_v2 (
      recording_id TEXT PRIMARY KEY,
      recording_mbid TEXT,
      fallback_key TEXT,
      is_synthetic_fallback_key BOOLEAN NOT NULL DEFAULT FALSE,
      track_name TEXT,
      artist_name TEXT,
      artist_mbids TEXT[],
      release_name TEXT,
      release_date DATE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_track_info_v2_recording_mbid
      ON track_info_v2(recording_mbid);
    CREATE INDEX IF NOT EXISTS idx_track_info_v2_is_synth
      ON track_info_v2(is_synthetic_fallback_key);

        CREATE TABLE IF NOT EXISTS artist_daily_listens_v2 (
            day DATE NOT NULL,
            artist_id TEXT NOT NULL,
            dump_id TEXT NOT NULL,
            listen_count BIGINT NOT NULL,
            PRIMARY KEY (day, artist_id, dump_id),
            FOREIGN KEY (artist_id) REFERENCES artist_info_v2(artist_id)
        );

        CREATE INDEX IF NOT EXISTS idx_artist_daily_v2_day
            ON artist_daily_listens_v2(day);
        CREATE INDEX IF NOT EXISTS idx_artist_daily_v2_dump
            ON artist_daily_listens_v2(dump_id);

    CREATE TABLE IF NOT EXISTS track_daily_listens_v2 (
      day DATE NOT NULL,
      recording_id TEXT NOT NULL,
      dump_id TEXT NOT NULL,
      listen_count BIGINT NOT NULL,
      PRIMARY KEY (day, recording_id, dump_id),
      FOREIGN KEY (recording_id) REFERENCES track_info_v2(recording_id)
    );

    CREATE INDEX IF NOT EXISTS idx_track_daily_v2_day
      ON track_daily_listens_v2(day);
    CREATE INDEX IF NOT EXISTS idx_track_daily_v2_dump
      ON track_daily_listens_v2(dump_id);

        CREATE TABLE IF NOT EXISTS artist_daily_stats_v2 (
            day DATE NOT NULL,
            artist_id TEXT NOT NULL,
            growth_percentile FLOAT,
            cumulative_listen_count BIGINT,
            listen_count_past_7_days BIGINT,
            listen_pctl_past_7_days FLOAT,
            listen_count_past_30_days BIGINT,
            listen_pctl_past_30_days FLOAT,
            PRIMARY KEY (day, artist_id),
            FOREIGN KEY (artist_id) REFERENCES artist_info_v2(artist_id)
        );

    CREATE TABLE IF NOT EXISTS track_daily_stats_v2 (
      day DATE NOT NULL,
      recording_id TEXT NOT NULL,
      growth_percentile FLOAT,
      cumulative_listen_count BIGINT,
      listen_count_past_7_days BIGINT,
      listen_pctl_past_7_days FLOAT,
      listen_count_past_30_days BIGINT,
      listen_pctl_past_30_days FLOAT,
      PRIMARY KEY (day, recording_id),
      FOREIGN KEY (recording_id) REFERENCES track_info_v2(recording_id)
    );
    """

    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(ddl)


def parse_dump_v2(source: Path | str, max_lines: int = 0, alias_to_mbid: dict[str, str] | None = None):
    """
    Parse one dump with canonical-first track identity logic.

    Returns:
      track_daily: dict[(day, recording_id), int]
      artist_daily: dict[(day, artist_id), int]
      track_info: dict[recording_id, tuple]
      artist_info: dict[artist_id, tuple]
      summary: dict
    """
    source = Path(source)

    if alias_to_mbid is None:
        alias_to_mbid = {}

    track_daily: dict = defaultdict(int)
    artist_daily: dict = defaultdict(int)
    track_info: dict = {}
    artist_info: dict = {}

    lines = 0
    bad_json = 0
    missing_ts = 0
    usable_rows = 0

    artist_mbids_missing_key = 0
    artist_mbids_empty_list = 0
    artist_name_missing = 0
    rows_with_artist_names_array = 0
    rows_with_release_artist_names_array = 0
    rows_with_collab_markers_in_artist_name = 0

    used_recording_mbid = 0
    used_fallback_key = 0
    dropped_missing_identity = 0

    used_artist_mbid_rows = 0
    used_artist_fallback_rows = 0
    dropped_artist_identity_rows = 0
    generated_artist_daily_rows = 0
    rows_with_multi_artist_candidates = 0

    release_date_present_rows = 0

    for _, line in iter_listens_from_tar_zst(source):
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

        usable_rows += 1

        day = day_from_unix(int(ts))
        tm = rec.get("track_metadata") or {}
        add = tm.get("additional_info") or {}

        recording_mbid = _get_any(add, "recording_mbid", "Recording_mbid")
        artist_name = tm.get("artist_name")
        track_name = tm.get("track_name")
        release_name = tm.get("release_name") or _get_any(add, "release_name")
        raw_artist_mbids = _get_any(add, "artist_mbids")
        raw_artist_names = _get_any(add, "artist_names")
        raw_release_artist_names = _get_any(add, "release_artist_names")
        artist_mbids = normalize_artist_mbids(raw_artist_mbids)
        release_date = extract_release_date(tm, add)

        if raw_artist_mbids is None:
            artist_mbids_missing_key += 1
        elif isinstance(raw_artist_mbids, list) and len(raw_artist_mbids) == 0:
            artist_mbids_empty_list += 1

        if not artist_name:
            artist_name_missing += 1
        elif has_collab_markers(artist_name):
            rows_with_collab_markers_in_artist_name += 1

        if isinstance(raw_artist_names, list) and len(raw_artist_names) > 0:
            rows_with_artist_names_array += 1
        if isinstance(raw_release_artist_names, list) and len(raw_release_artist_names) > 0:
            rows_with_release_artist_names_array += 1

        if release_date:
            release_date_present_rows += 1

        artist_identities, used_multi_name_candidates = choose_artist_identities(
            artist_mbids,
            tm,
            add,
            alias_to_mbid=alias_to_mbid,
        )
        artist_ids_for_track = _dedupe_preserve_order([item[0] for item in artist_identities])
        if used_multi_name_candidates:
            rows_with_multi_artist_candidates += 1

        recording_id, fallback_key, is_synthetic = choose_recording_identity(
            recording_mbid=recording_mbid,
            artist_name=artist_name,
            track_name=track_name,
            release_name=release_name,
        )

        if not recording_id:
            dropped_missing_identity += 1
        else:
            if is_synthetic:
                used_fallback_key += 1
            else:
                used_recording_mbid += 1

            track_daily[(day, recording_id)] += 1

            if recording_id not in track_info:
                track_info[recording_id] = (
                    recording_mbid,
                    fallback_key,
                    is_synthetic,
                    track_name,
                    artist_name,
                    artist_ids_for_track,
                    release_name,
                    release_date,
                )

        if not artist_identities:
            dropped_artist_identity_rows += 1
        else:
            if any(not item[3] for item in artist_identities):
                used_artist_mbid_rows += 1
            else:
                used_artist_fallback_rows += 1

            for artist_id, artist_mbid, artist_fallback_key, artist_is_synthetic, artist_name_value in artist_identities:
                artist_daily[(day, artist_id)] += 1
                generated_artist_daily_rows += 1
                if artist_id not in artist_info:
                    artist_info[artist_id] = (
                        artist_mbid,
                        artist_fallback_key,
                        artist_is_synthetic,
                        artist_name_value,
                    )

                if artist_name_value:
                    alias_to_mbid[normalize_artist_token(artist_name_value)] = artist_id

    summary = {
        "lines_parsed": lines,
        "usable_rows": usable_rows,
        "bad_json": bad_json,
        "missing_timestamp": missing_ts,

        "artist_mbids_missing_key": artist_mbids_missing_key,
        "artist_mbids_empty_list": artist_mbids_empty_list,
        "artist_name_missing": artist_name_missing,
        "rows_with_artist_names_array": rows_with_artist_names_array,
        "rows_with_release_artist_names_array": rows_with_release_artist_names_array,
        "rows_with_collab_markers_in_artist_name": rows_with_collab_markers_in_artist_name,

        "used_recording_mbid": used_recording_mbid,
        "used_fallback_key": used_fallback_key,
        "dropped_missing_identity": dropped_missing_identity,

        "used_artist_mbid_rows": used_artist_mbid_rows,
        "used_artist_fallback_rows": used_artist_fallback_rows,
        "dropped_artist_identity_rows": dropped_artist_identity_rows,
        "generated_artist_daily_rows": generated_artist_daily_rows,
        "rows_with_multi_artist_candidates": rows_with_multi_artist_candidates,

        # Track rows where a parseable release date was found.
        "track_release_date_present_rows": release_date_present_rows,
        # Backward-compatible alias used by earlier prints.
        "release_date_present_rows": release_date_present_rows,

        "unique_track_day_keys_v2": len(track_daily),
        "unique_artist_day_keys_v2": len(artist_daily),
        "unique_tracks_v2": len({key[1] for key in track_daily.keys()}),
        "unique_artists_v2": len({key[1] for key in artist_daily.keys()}),
    }

    return track_daily, artist_daily, track_info, artist_info, summary


def _pct(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return (100.0 * numerator) / denominator


def print_parser_details(summary: dict) -> None:
    usable = int(summary.get("usable_rows", 0))
    track_release_rows = int(
        summary.get(
            "track_release_date_present_rows",
            summary.get("release_date_present_rows", 0),
        )
    )

    print("Parse summary (v2):")
    print("=" * 72)
    print("RAW")
    print(f"  lines_parsed:            {summary['lines_parsed']:,}")
    print(f"  usable_rows:             {summary['usable_rows']:,}")
    print(f"  bad_json:                {summary['bad_json']:,}")
    print(f"  missing_timestamp:       {summary['missing_timestamp']:,}")
    print()

    print("TRACK IDENTITY")
    print(f"  used_recording_mbid:     {summary['used_recording_mbid']:,} ({_pct(summary['used_recording_mbid'], usable):6.2f}%)")
    print(f"  used_fallback_key:       {summary['used_fallback_key']:,} ({_pct(summary['used_fallback_key'], usable):6.2f}%)")
    print(f"  dropped_missing_identity:{summary['dropped_missing_identity']:,} ({_pct(summary['dropped_missing_identity'], usable):6.2f}%)")
    print(f"  track_release_date_present_rows:{track_release_rows:,} ({_pct(track_release_rows, usable):6.2f}%)")
    print(f"  unique_tracks_v2:        {summary['unique_tracks_v2']:,}")
    print(f"  unique_track_day_keys_v2 (distinct day+recording_id): {summary['unique_track_day_keys_v2']:,}")
    print()

    print("ARTIST IDENTITY")
    print(f"  used_artist_mbid_rows:   {summary['used_artist_mbid_rows']:,} ({_pct(summary['used_artist_mbid_rows'], usable):6.2f}%)")
    print(f"  used_artist_fallback_rows:{summary['used_artist_fallback_rows']:,} ({_pct(summary['used_artist_fallback_rows'], usable):6.2f}%)")
    print(f"  dropped_artist_identity_rows:{summary['dropped_artist_identity_rows']:,} ({_pct(summary['dropped_artist_identity_rows'], usable):6.2f}%)")
    print(f"  generated_artist_daily_rows:{summary['generated_artist_daily_rows']:,}")
    print(f"  rows_with_multi_artist_candidates:{summary['rows_with_multi_artist_candidates']:,} ({_pct(summary['rows_with_multi_artist_candidates'], usable):6.2f}%)")
    print(f"  unique_artists_v2:       {summary['unique_artists_v2']:,}")
    print(f"  unique_artist_day_keys_v2 (distinct day+artist_id): {summary['unique_artist_day_keys_v2']:,}")
    print()

    print("FIELD QUALITY")
    print(f"  artist_mbids_missing_key:{summary['artist_mbids_missing_key']:,}")
    print(f"  artist_mbids_empty_list: {summary['artist_mbids_empty_list']:,}")
    print(f"  artist_name_missing:     {summary['artist_name_missing']:,}")
    print(f"  rows_with_artist_names_array:{summary['rows_with_artist_names_array']:,}")
    print(f"  rows_with_release_artist_names_array:{summary['rows_with_release_artist_names_array']:,}")
    print(f"  rows_with_collab_markers_in_artist_name:{summary['rows_with_collab_markers_in_artist_name']:,} ({_pct(summary['rows_with_collab_markers_in_artist_name'], usable):6.2f}%)")
    print("=" * 72)


def upsert_v2(
    conn: psycopg2.extensions.connection,
    track_daily: dict,
    artist_daily: dict,
    track_info: dict,
    artist_info: dict,
    dump_path: str | Path,
    chunk_size: int = 20_000,
) -> None:
    import time
    from psycopg2.extras import execute_values

    dump_path = str(dump_path)
    match = re.search(r"dump-(\d+)-", dump_path)
    dump_id = match.group(1) if match else (dump_path or "manual")

    conn.autocommit = False
    with conn, conn.cursor() as cur:
        t0 = time.perf_counter()
        print(f"Upserting artist_info_v2 ({len(artist_info):,} rows)...", end=" ", flush=True)
        rows = [
            (
                artist_id,
                values[0],
                values[1],
                values[2],
                values[3],
            )
            for artist_id, values in artist_info.items()
        ]
        for batch in _chunked(rows, chunk_size):
            execute_values(
                cur,
                """
                INSERT INTO artist_info_v2 (
                  artist_id,
                  artist_mbid,
                  fallback_key,
                  is_synthetic_fallback_key,
                  artist_name
                )
                VALUES %s
                ON CONFLICT (artist_id) DO UPDATE
                  SET artist_mbid = COALESCE(EXCLUDED.artist_mbid, artist_info_v2.artist_mbid),
                      fallback_key = COALESCE(EXCLUDED.fallback_key, artist_info_v2.fallback_key),
                      is_synthetic_fallback_key = EXCLUDED.is_synthetic_fallback_key,
                      artist_name = COALESCE(EXCLUDED.artist_name, artist_info_v2.artist_name),
                      updated_at = now()
                """,
                batch,
            )
        print(f"done ({time.perf_counter() - t0:.1f}s)")

        t0 = time.perf_counter()
        print(f"Upserting track_info_v2 ({len(track_info):,} rows)...", end=" ", flush=True)
        rows = [
            (
                rid,
                values[0],
                values[1],
                values[2],
                values[3],
                values[4],
                values[5],
                values[6],
                values[7],
            )
            for rid, values in track_info.items()
        ]
        for batch in _chunked(rows, chunk_size):
            execute_values(
                cur,
                """
                INSERT INTO track_info_v2 (
                  recording_id,
                  recording_mbid,
                  fallback_key,
                  is_synthetic_fallback_key,
                  track_name,
                  artist_name,
                  artist_mbids,
                  release_name,
                  release_date
                )
                VALUES %s
                ON CONFLICT (recording_id) DO UPDATE
                  SET recording_mbid = COALESCE(EXCLUDED.recording_mbid, track_info_v2.recording_mbid),
                      fallback_key = COALESCE(EXCLUDED.fallback_key, track_info_v2.fallback_key),
                      is_synthetic_fallback_key = EXCLUDED.is_synthetic_fallback_key,
                      track_name = COALESCE(EXCLUDED.track_name, track_info_v2.track_name),
                      artist_name = COALESCE(EXCLUDED.artist_name, track_info_v2.artist_name),
                      artist_mbids = COALESCE(EXCLUDED.artist_mbids, track_info_v2.artist_mbids),
                      release_name = COALESCE(EXCLUDED.release_name, track_info_v2.release_name),
                      release_date = COALESCE(EXCLUDED.release_date, track_info_v2.release_date),
                      updated_at = now()
                """,
                batch,
            )
        print(f"done ({time.perf_counter() - t0:.1f}s)")

        t0 = time.perf_counter()
        print(f"Upserting artist_daily_listens_v2 ({len(artist_daily):,} rows)...", end=" ", flush=True)
        rows = [(day, artist_id, dump_id, cnt) for (day, artist_id), cnt in artist_daily.items()]
        for batch in _chunked(rows, 50_000):
            execute_values(
                cur,
                """
                INSERT INTO artist_daily_listens_v2 (day, artist_id, dump_id, listen_count)
                VALUES %s
                ON CONFLICT (day, artist_id, dump_id) DO UPDATE
                  SET listen_count = EXCLUDED.listen_count
                """,
                batch,
            )
        print(f"done ({time.perf_counter() - t0:.1f}s)")

        t0 = time.perf_counter()
        print(f"Upserting track_daily_listens_v2 ({len(track_daily):,} rows)...", end=" ", flush=True)
        rows = [(day, rid, dump_id, cnt) for (day, rid), cnt in track_daily.items()]
        for batch in _chunked(rows, 50_000):
            execute_values(
                cur,
                """
                INSERT INTO track_daily_listens_v2 (day, recording_id, dump_id, listen_count)
                VALUES %s
                ON CONFLICT (day, recording_id, dump_id) DO UPDATE
                  SET listen_count = EXCLUDED.listen_count
                """,
                batch,
            )
        print(f"done ({time.perf_counter() - t0:.1f}s)")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run v2 track identity parsing on one dump (canonical MBID + fallback key).",
    )
    parser.add_argument("dump_path", type=Path, help="Path to one *.tar.zst dump")
    parser.add_argument("--max-lines", type=int, default=0, help="Optional line cap for faster test")
    parser.add_argument(
        "--upsert",
        action="store_true",
        help="Create v2 tables and write parsed output to Postgres",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    alias_conn = connect_postgres()
    try:
        alias_to_mbid = _load_alias_map_from_db(alias_conn)
    finally:
        alias_conn.close()

    track_daily, artist_daily, track_info, artist_info, summary = parse_dump_v2(
        args.dump_path,
        max_lines=args.max_lines,
        alias_to_mbid=alias_to_mbid,
    )

    print_parser_details(summary)

    if not args.upsert:
        return

    # connect_postgres() loads credentials from .env / Secrets Manager.
    conn = connect_postgres()
    try:
        ensure_v2_tables(conn)
        upsert_v2(conn, track_daily, artist_daily, track_info, artist_info, args.dump_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
