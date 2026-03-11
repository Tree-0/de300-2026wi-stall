from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

import orjson

from pipeline import iter_listens_from_tar_zst


def get_any(d, *keys):
    if not isinstance(d, dict):
        return None
    for key in keys:
        if key in d:
            return d[key]
    return None


def normalize_artist_mbids(raw_artist_mbids):
    if isinstance(raw_artist_mbids, list):
        return [str(value).strip() for value in raw_artist_mbids if str(value).strip()]
    if raw_artist_mbids is None:
        return []
    if isinstance(raw_artist_mbids, str):
        value = raw_artist_mbids.strip()
        return [value] if value else []
    return []


def build_current_recording_id(artist_mbids, track_name):
    return f"{'_'.join(artist_mbids)}_{track_name}"


def pct(numerator, denominator):
    if denominator == 0:
        return 0.0
    return 100.0 * numerator / denominator


def audit_dump(dump_path: Path, max_lines: int = 0, example_limit: int = 10):
    counters = Counter()
    no_artist_track_keys = set()
    no_artist_names = set()
    examples = []

    for member_name, line in iter_listens_from_tar_zst(dump_path):
        counters["lines_seen"] += 1
        if max_lines and counters["lines_seen"] > max_lines:
            break

        try:
            record = orjson.loads(line)
        except Exception:
            counters["bad_json"] += 1
            continue

        timestamp = record.get("timestamp")
        if timestamp is None:
            counters["missing_timestamp"] += 1
            continue

        counters["usable_rows"] += 1

        track_metadata = record.get("track_metadata") or {}
        additional_info = track_metadata.get("additional_info") or {}

        raw_artist_mbids = get_any(additional_info, "artist_mbids")
        artist_mbids = normalize_artist_mbids(raw_artist_mbids)
        recording_mbid = get_any(additional_info, "recording_mbid", "Recording_mbid")
        track_name = track_metadata.get("track_name")
        artist_name = track_metadata.get("artist_name")
        release_name = track_metadata.get("release_name")

        current_recording_id = build_current_recording_id(artist_mbids, track_name)
        counters["track_rows_generated_current_pipeline"] += 1

        if recording_mbid:
            counters["recording_mbid_present"] += 1
        else:
            counters["recording_mbid_missing"] += 1

        if raw_artist_mbids is None:
            counters["artist_mbids_missing_key"] += 1
        elif isinstance(raw_artist_mbids, list):
            counters["artist_mbids_list"] += 1
            if len(raw_artist_mbids) == 0:
                counters["artist_mbids_empty_list"] += 1
            elif len(artist_mbids) == 0:
                counters["artist_mbids_blank_values_only"] += 1
        else:
            counters["artist_mbids_non_list"] += 1

        if artist_name:
            counters["artist_name_present"] += 1
        else:
            counters["artist_name_missing"] += 1

        if track_name:
            counters["track_name_present"] += 1
        else:
            counters["track_name_missing"] += 1

        if release_name:
            counters["release_name_present"] += 1
        else:
            counters["release_name_missing"] += 1

        if artist_mbids:
            counters["rows_with_artist_mbids"] += 1
            counters["artist_daily_rows_generated_current_pipeline"] += len(artist_mbids)
        else:
            counters["rows_without_artist_mbids"] += 1
            counters["rows_dropped_from_artist_daily_current_pipeline"] += 1
            no_artist_track_keys.add(current_recording_id)
            if current_recording_id.startswith("_"):
                counters["underscore_prefixed_recording_ids"] += 1
            if artist_name:
                no_artist_names.add(artist_name)
                counters["rows_without_artist_mbids_but_artist_name_present"] += 1
            if recording_mbid:
                counters["rows_without_artist_mbids_but_recording_mbid_present"] += 1
            else:
                counters["rows_without_artist_mbids_and_recording_mbid_missing"] += 1
            if track_name:
                counters["rows_without_artist_mbids_but_track_name_present"] += 1

            if len(examples) < example_limit:
                examples.append(
                    {
                        "member_name": member_name,
                        "track_name": track_name,
                        "artist_name": artist_name,
                        "artist_mbids": raw_artist_mbids,
                        "recording_mbid": recording_mbid,
                        "current_recording_id": current_recording_id,
                    }
                )

        if not isinstance(raw_artist_mbids, list) and not raw_artist_mbids:
            counters["pipeline_missing_artist_counter_behavior"] += 1

    return counters, no_artist_track_keys, no_artist_names, examples


def print_report(dump_path: Path, counters, no_artist_track_keys, no_artist_names, examples):
    usable = counters["usable_rows"]
    no_artist = counters["rows_without_artist_mbids"]
    with_artist = counters["rows_with_artist_mbids"]

    print(f"Dump: {dump_path}")
    print("=" * 88)
    print("RAW PARSE COUNTS")
    print("=" * 88)
    print(f"Lines seen:                              {counters['lines_seen']:,}")
    print(f"Usable rows:                             {usable:,}")
    print(f"Bad JSON:                                {counters['bad_json']:,}")
    print(f"Missing timestamp:                       {counters['missing_timestamp']:,}")
    print()
    print("ARTIST IDENTIFIER QUALITY")
    print("=" * 88)
    print(f"Rows with usable artist_mbids:           {with_artist:,} ({pct(with_artist, usable):6.2f}%)")
    print(f"Rows without usable artist_mbids:        {no_artist:,} ({pct(no_artist, usable):6.2f}%)")
    print(f"artist_mbids key missing:                {counters['artist_mbids_missing_key']:,}")
    print(f"artist_mbids empty list:                 {counters['artist_mbids_empty_list']:,}")
    print(f"artist_mbids blank values only:          {counters['artist_mbids_blank_values_only']:,}")
    print(f"artist_mbids non-list:                   {counters['artist_mbids_non_list']:,}")
    print(f"Artist name present but artist_mbids missing: {counters['rows_without_artist_mbids_but_artist_name_present']:,}")
    print()
    print("TRACK IDENTIFIER QUALITY")
    print("=" * 88)
    print(f"Rows with recording_mbid present:        {counters['recording_mbid_present']:,} ({pct(counters['recording_mbid_present'], usable):6.2f}%)")
    print(f"Rows with recording_mbid missing:        {counters['recording_mbid_missing']:,} ({pct(counters['recording_mbid_missing'], usable):6.2f}%)")
    print(f"Rows without artist_mbids but recording_mbid present: {counters['rows_without_artist_mbids_but_recording_mbid_present']:,}")
    print(f"Rows without artist_mbids and recording_mbid missing: {counters['rows_without_artist_mbids_and_recording_mbid_missing']:,}")
    print(f"Underscore-prefixed recording_id rows:   {counters['underscore_prefixed_recording_ids']:,}")
    print(f"Unique synthetic track keys with no artist_mbids: {len(no_artist_track_keys):,}")
    print()
    print("CURRENT PIPELINE IMPACT")
    print("=" * 88)
    print(f"Track rows generated:                    {counters['track_rows_generated_current_pipeline']:,}")
    print(f"Artist-daily rows generated:             {counters['artist_daily_rows_generated_current_pipeline']:,}")
    print(f"Rows dropped from artist_daily:          {counters['rows_dropped_from_artist_daily_current_pipeline']:,} ({pct(counters['rows_dropped_from_artist_daily_current_pipeline'], usable):6.2f}%)")
    print(f"Distinct artist names lost to artist_info without mbids: {len(no_artist_names):,}")
    print()
    print("PIPELINE SUMMARY BUG CHECK")
    print("=" * 88)
    print(f"Current parse_dump missing_artist_mbid counter would report: {counters['pipeline_missing_artist_counter_behavior']:,}")
    print(f"Actual rows without usable artist_mbids:                     {counters['rows_without_artist_mbids']:,}")
    if counters['rows_without_artist_mbids'] != counters['pipeline_missing_artist_counter_behavior']:
        print("Note: the current pipeline undercounts missing artists when artist_mbids is an empty list.")
    print()
    if examples:
        print("EXAMPLE ROWS WITH NO ARTIST MBIDS")
        print("=" * 88)
        for idx, example in enumerate(examples, 1):
            print(f"[{idx}] member={example['member_name']}")
            print(f"     track_name={example['track_name']!r}")
            print(f"     artist_name={example['artist_name']!r}")
            print(f"     artist_mbids={example['artist_mbids']!r}")
            print(f"     recording_mbid={example['recording_mbid']!r}")
            print(f"     current_recording_id={example['current_recording_id']!r}")
            print()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Audit one ListenBrainz dump for missing artist identifiers and synthetic recording_id impact.",
    )
    parser.add_argument("dump_path", type=Path, help="Path to one listenbrainz *.tar.zst dump")
    parser.add_argument("--max-lines", type=int, default=0, help="Optional line cap for faster sampling")
    parser.add_argument("--examples", type=int, default=10, help="How many missing-artist examples to print")
    return parser.parse_args()


def main():
    args = parse_args()
    counters, no_artist_track_keys, no_artist_names, examples = audit_dump(
        dump_path=args.dump_path,
        max_lines=args.max_lines,
        example_limit=args.examples,
    )
    print_report(args.dump_path, counters, no_artist_track_keys, no_artist_names, examples)


if __name__ == "__main__":
    main()