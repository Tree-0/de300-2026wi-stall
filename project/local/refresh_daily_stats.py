"""Windowed recompute of ListenBrainz daily stats tables (now SQL-only, no Spark).

This script fills (by default for the most recent 14 days):
- artist_daily_stats  (reads from artist_daily_listens, key: artist_id)
- track_daily_stats   (reads from track_daily_listens,  key: recording_id)

Why windowed?
- Computing percentiles requires ranking all entities per day, but we only need
  recent days for short-horizon modeling.
- Full refresh becomes expensive as history grows.

Behavior
--------
For each entity type requested:
1) Compute start_day = max(day in *_daily_listens) - window_days
2) DELETE existing stats rows where day >= start_day
3) INSERT recomputed stats for day >= start_day

To keep rolling sums correct inside the output window, the computation reads
input listens from (start_day - 30 days) onward.

For cumulative_listen_count, we seed each entity's running total from the most
recent existing stats row strictly before the lookback range (when available).
This avoids scanning all historical listens each run.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

from utils import connect_postgres, load_db_credentials


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute recent artist/track daily stats in Postgres (windowed, SQL-only)"
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional path to a .env file (default: project/.env if present).",
    )
    parser.add_argument(
        "--skip-dotenv",
        action="store_true",
        help="Skip loading environment variables from .env.",
    )
    parser.add_argument("--host",     default=None, help="Postgres host override.")
    parser.add_argument("--port",     default=None, help="Postgres port override.")
    parser.add_argument("--dbname",   default=None, help="Postgres database override.")
    parser.add_argument("--user",     default=None, help="Postgres user override.")
    parser.add_argument("--password", default=None, help="Postgres password override.")
    parser.add_argument(
        "--entity",
        choices=["artist", "track", "both"],
        default="both",
        help="Which entity to compute stats for (default: both).",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=14,
        help="Number of most recent days (per listens table) to recompute stats for (default: 14).",
    )
    return parser.parse_args()


def maybe_load_dotenv(args: argparse.Namespace) -> None:
    if args.skip_dotenv:
        return
    env_path = args.env_file
    if env_path is None:
        env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"Loaded env from: {env_path}")
    else:
        print(f"No .env found at: {env_path} (continuing with existing env)")


def main() -> None:
    args = parse_args()
    maybe_load_dotenv(args)

    conn_params = load_db_credentials()
    if args.host:
        conn_params["host"] = args.host
    if args.port:
        conn_params["port"] = args.port
    if args.dbname:
        conn_params["dbname"] = args.dbname
    if args.user:
        conn_params["user"] = args.user
    if args.password is not None:
        conn_params["password"] = args.password

    print(
        f"Target DB: {conn_params['host']}:{conn_params['port']}"
        f"/{conn_params['dbname']} as {conn_params['user']}"
    )

    window_days = max(int(args.window_days), 1)
    lookback_days = 30  # needed for 30-day rolling windows

    def _max_day(cur, table: str) -> date | None:
        cur.execute(f"SELECT MAX(day) FROM {table}")
        return cur.fetchone()[0]

    def _refresh_entity(
        cur,
        listens_table: str,
        stats_table: str,
        id_col: str,
    ) -> int:
        max_day = _max_day(cur, listens_table)
        if max_day is None:
            print(f"Skipping {stats_table}: {listens_table} is empty.")
            return 0

        start_day = max_day - timedelta(days=window_days)
        lookback_start = start_day - timedelta(days=lookback_days)

        print(
            f"Refreshing {stats_table} for day >= {start_day} "
            f"(input lookback from {lookback_start}, max_day={max_day})"
        )

        # Replace only the recent window in the stats table.
        cur.execute(f"DELETE FROM {stats_table} WHERE day >= %s", (start_day,))

        cur.execute(
            f"""
            WITH daily AS (
                SELECT
                    day::date AS day,
                    {id_col} AS id_value,
                    SUM(listen_count)::bigint AS listen_count
                FROM {listens_table}
                WHERE day >= %s
                GROUP BY day::date, {id_col}
            ),
            prior AS (
                SELECT DISTINCT ON (s.{id_col})
                    s.{id_col} AS id_value,
                    COALESCE(s.cumulative_listen_count, 0)::bigint AS cum_before
                FROM {stats_table} s
                WHERE s.day < %s
                ORDER BY s.{id_col}, s.day DESC
            ),
            base AS (
                SELECT
                    d.day,
                    d.id_value,
                    d.listen_count,
                    COALESCE(p.cum_before, 0)::bigint
                      + SUM(d.listen_count) OVER (
                            PARTITION BY d.id_value
                            ORDER BY d.day
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS cumulative_listen_count,
                    SUM(d.listen_count) OVER (
                        PARTITION BY d.id_value
                        ORDER BY d.day
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS listen_count_past_7_days,
                    SUM(d.listen_count) OVER (
                        PARTITION BY d.id_value
                        ORDER BY d.day
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS listen_count_past_30_days
                FROM daily d
                LEFT JOIN prior p
                  ON p.id_value = d.id_value
            ),
            enriched AS (
                SELECT
                    day,
                    id_value,
                    cumulative_listen_count,
                    listen_count_past_7_days,
                    listen_count_past_30_days,
                    CASE
                        WHEN LAG(cumulative_listen_count) OVER (
                            PARTITION BY id_value
                            ORDER BY day
                        ) > 0
                        THEN listen_count::double precision
                             / LAG(cumulative_listen_count) OVER (
                                 PARTITION BY id_value
                                 ORDER BY day
                             )
                        ELSE NULL
                    END AS growth_rate
                FROM base
            ),
            ranked AS (
                SELECT
                    day,
                    id_value,
                    PERCENT_RANK() OVER (
                        PARTITION BY day
                        ORDER BY growth_rate ASC NULLS FIRST
                    ) AS growth_percentile,
                    cumulative_listen_count,
                    listen_count_past_7_days,
                    PERCENT_RANK() OVER (
                        PARTITION BY day
                        ORDER BY listen_count_past_7_days
                    ) AS listen_pctl_past_7_days,
                    listen_count_past_30_days,
                    PERCENT_RANK() OVER (
                        PARTITION BY day
                        ORDER BY listen_count_past_30_days
                    ) AS listen_pctl_past_30_days
                FROM enriched
            )
            INSERT INTO {stats_table} (
                day,
                {id_col},
                growth_percentile,
                cumulative_listen_count,
                listen_count_past_7_days,
                listen_pctl_past_7_days,
                listen_count_past_30_days,
                listen_pctl_past_30_days
            )
            SELECT
                day,
                id_value,
                growth_percentile,
                cumulative_listen_count,
                listen_count_past_7_days,
                listen_pctl_past_7_days,
                listen_count_past_30_days,
                listen_pctl_past_30_days
            FROM ranked
            WHERE day >= %s
            """,
            (lookback_start, lookback_start, start_day),
        )
        return int(cur.rowcount or 0)

    conn = connect_postgres(conn_params)
    try:
        conn.autocommit = False
        with conn, conn.cursor() as cur:
            rows_written = {}

            if args.entity in ("artist", "both"):
                rows_written["artist_daily_stats"] = _refresh_entity(
                    cur,
                    listens_table="artist_daily_listens",
                    stats_table="artist_daily_stats",
                    id_col="artist_id",
                )

            if args.entity in ("track", "both"):
                rows_written["track_daily_stats"] = _refresh_entity(
                    cur,
                    listens_table="track_daily_listens",
                    stats_table="track_daily_stats",
                    id_col="recording_id",
                )

        print("Daily stats refresh complete.")
        for k, v in rows_written.items():
            print(f"  {k}: wrote {v:,} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
