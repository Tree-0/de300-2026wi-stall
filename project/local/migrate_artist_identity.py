from __future__ import annotations

import argparse

from psycopg2.extras import execute_values

from artist_identity import (
    build_alias_map,
    split_artist_credit,
    synth_artist_mbid_from_token,
)
from utils import connect_postgres, load_db_credentials


def _build_remap_rows(artist_rows: list[tuple[str, str | None]]) -> list[tuple[str, str, str | None, bool]]:
    """
    Build deterministic old->new artist mapping rows.

    One old MBID can map to many new MBIDs when its name looks like a multi-artist credit.
    """
    alias_to_mbid = build_alias_map(artist_rows)
    remap_rows: list[tuple[str, str, str | None, bool]] = []

    for old_mbid, artist_name in artist_rows:
        tokens = split_artist_credit(artist_name)
        if not tokens:
            remap_rows.append((old_mbid, old_mbid, artist_name, False))
            continue

        # Keep old MBID bound to the first token if the old MBID is valid.
        first_name = tokens[0]
        remap_rows.append((old_mbid, old_mbid, first_name, False))
        alias_to_mbid[first_name.strip().lower()] = old_mbid

        for token in tokens[1:]:
            key = token.strip().lower()
            existing = alias_to_mbid.get(key)
            if existing:
                remap_rows.append((old_mbid, existing, token, False))
                continue

            # User-specified rule: unresolved split tokens get a synthetic UUID MBID.
            synthetic = synth_artist_mbid_from_token(token)
            remap_rows.append((old_mbid, synthetic, token, True))
            alias_to_mbid[key] = synthetic

    deduped: list[tuple[str, str, str | None, bool]] = []
    seen: set[tuple[str, str]] = set()
    for old_mbid, new_mbid, new_name, is_synth in remap_rows:
        key = (old_mbid, new_mbid)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((old_mbid, new_mbid, new_name, is_synth))

    return deduped


def _has_column(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )
    return cur.fetchone() is not None


def run_migration(apply_changes: bool) -> None:
    creds = load_db_credentials()
    conn = connect_postgres(creds)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT artist_mbid, artist_name FROM artist_info")
            artist_rows = cur.fetchall()

        if not artist_rows:
            print("artist_info is empty; nothing to migrate.")
            return

        remap_rows = _build_remap_rows(artist_rows)
        old_mbids = {row[0] for row in remap_rows}
        new_mbids = {row[1] for row in remap_rows}
        synthetic_count = sum(1 for row in remap_rows if row[3])
        split_rows = sum(1 for _, name in artist_rows if len(split_artist_credit(name)) > 1)

        print("Migration preview:")
        print(f"  artist_info rows: {len(artist_rows):,}")
        print(f"  old MBIDs in mapping: {len(old_mbids):,}")
        print(f"  new MBIDs in mapping: {len(new_mbids):,}")
        print(f"  synthetic MBIDs: {synthetic_count:,}")
        print(f"  multi-artist name rows detected: {split_rows:,}")

        if not apply_changes:
            print("Dry run only. Re-run with --apply to execute migration.")
            return

        # Preview SELECTs above open a transaction on psycopg2 connections.
        # Reset it before starting the write transaction.
        conn.rollback()
        with conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS _artist_mbid_remap")
            cur.execute(
                """
                CREATE TEMP TABLE _artist_mbid_remap (
                    old_artist_mbid TEXT NOT NULL,
                    new_artist_mbid TEXT NOT NULL,
                    new_artist_name TEXT,
                    is_synthetic BOOLEAN NOT NULL,
                    PRIMARY KEY (old_artist_mbid, new_artist_mbid)
                ) ON COMMIT DROP
                """
            )

            execute_values(
                cur,
                """
                INSERT INTO _artist_mbid_remap (old_artist_mbid, new_artist_mbid, new_artist_name, is_synthetic)
                VALUES %s
                """,
                remap_rows,
                page_size=10_000,
            )

            # Ensure all target MBIDs exist in artist_info with a canonicalized name.
            cur.execute(
                """
                INSERT INTO artist_info (artist_mbid, artist_name)
                SELECT
                    new_artist_mbid,
                    MIN(NULLIF(new_artist_name, '')) AS artist_name
                FROM _artist_mbid_remap
                GROUP BY new_artist_mbid
                ON CONFLICT (artist_mbid) DO UPDATE
                  SET artist_name = COALESCE(artist_info.artist_name, EXCLUDED.artist_name)
                """
            )

            # Rebuild track_info.artist_mbids arrays with remapped MBIDs.
            cur.execute(
                """
                WITH expanded AS (
                    SELECT
                        t.recording_id,
                        u.ord,
                        m.new_artist_mbid
                    FROM track_info t
                    CROSS JOIN LATERAL unnest(t.artist_mbids) WITH ORDINALITY AS u(old_artist_mbid, ord)
                    JOIN _artist_mbid_remap m
                      ON m.old_artist_mbid = u.old_artist_mbid
                ),
                dedup AS (
                    SELECT DISTINCT recording_id, ord, new_artist_mbid
                    FROM expanded
                ),
                rebuilt AS (
                    SELECT
                        recording_id,
                        array_agg(new_artist_mbid ORDER BY ord, new_artist_mbid) AS new_artist_mbids
                    FROM dedup
                    GROUP BY recording_id
                )
                UPDATE track_info t
                SET artist_mbids = r.new_artist_mbids
                FROM rebuilt r
                WHERE t.recording_id = r.recording_id
                """
            )

            has_dump_id = _has_column(cur, "artist_daily_listens", "dump_id")
            if has_dump_id:
                cur.execute(
                    """
                    CREATE TEMP TABLE _artist_daily_listens_clean AS
                    SELECT
                        adl.day,
                        m.new_artist_mbid AS artist_mbid,
                        adl.dump_id,
                        SUM(adl.listen_count)::BIGINT AS listen_count
                    FROM artist_daily_listens adl
                    JOIN _artist_mbid_remap m
                      ON adl.artist_mbid = m.old_artist_mbid
                    GROUP BY adl.day, m.new_artist_mbid, adl.dump_id
                    """
                )
                cur.execute("TRUNCATE artist_daily_listens")
                cur.execute(
                    """
                    INSERT INTO artist_daily_listens (day, artist_mbid, dump_id, listen_count)
                    SELECT day, artist_mbid, dump_id, listen_count
                    FROM _artist_daily_listens_clean
                    """
                )
            else:
                cur.execute(
                    """
                    CREATE TEMP TABLE _artist_daily_listens_clean AS
                    SELECT
                        adl.day,
                        m.new_artist_mbid AS artist_mbid,
                        SUM(adl.listen_count)::BIGINT AS listen_count
                    FROM artist_daily_listens adl
                    JOIN _artist_mbid_remap m
                      ON adl.artist_mbid = m.old_artist_mbid
                    GROUP BY adl.day, m.new_artist_mbid
                    """
                )
                cur.execute("TRUNCATE artist_daily_listens")
                cur.execute(
                    """
                    INSERT INTO artist_daily_listens (day, artist_mbid, listen_count)
                    SELECT day, artist_mbid, listen_count
                    FROM _artist_daily_listens_clean
                    """
                )

            # Stats become invalid after MBID remap; force recomputation from listens.
            cur.execute("TRUNCATE artist_daily_stats")

        print("Migration applied successfully.")
        print("Next: run compute_daily_stats(...) to rebuild artist_daily_stats.")

        # Optional post-check summary.
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM artist_info")
            artist_info_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM artist_daily_listens")
            artist_daily_count = cur.fetchone()[0]

        print("Post-migration counts:")
        print(f"  artist_info: {artist_info_count:,}")
        print(f"  artist_daily_listens: {artist_daily_count:,}")

    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean and remap artist identities in existing tables.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply migration changes. Without this flag, only a dry-run preview is shown.",
    )
    args = parser.parse_args()
    run_migration(apply_changes=args.apply)


if __name__ == "__main__":
    main()
