#!/usr/bin/env python3
"""Freeze MLB provider rows for a deterministic linker fixture.

Usage:
  PYTHONPATH=... python polybot2/tests/fixtures/freeze_mlb_window_fixture.py \\
      --db /Users/reda/polymarket_bot/polybot2/data/prediction_markets.db \\
      --out /Users/reda/polymarket_bot/polybot2/tests/fixtures/linking_mlb_window_2026_04_17_19.json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="SQLite DB path")
    ap.add_argument("--out", required=True, help="Output JSON fixture path")
    ap.add_argument("--provider", default="kalstrop")
    ap.add_argument("--date-from", default="2026-04-17")
    ap.add_argument("--date-to", default="2026-04-19")
    args = ap.parse_args()

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT provider_game_id, game_date_et, home_raw, away_raw, parse_status, start_ts_utc
        FROM provider_games
        WHERE LOWER(provider) = LOWER(?)
          AND UPPER(sport_raw) = 'MLB'
          AND game_date_et >= ?
          AND game_date_et <= ?
          AND parse_status = 'ok'
        ORDER BY game_date_et, provider_game_id
        """,
        (str(args.provider), str(args.date_from), str(args.date_to)),
    ).fetchall()
    conn.close()

    out = {
        "provider": str(args.provider),
        "league": "mlb",
        "window": {"from": str(args.date_from), "to": str(args.date_to)},
        "provider_games": [
            {
                "provider_game_id": str(r["provider_game_id"]),
                "game_date_et": str(r["game_date_et"]),
                "home_raw": str(r["home_raw"]),
                "away_raw": str(r["away_raw"]),
                "hour_et": (
                    int((int(r["start_ts_utc"]) % 86_400) // 3600)
                    if r["start_ts_utc"] is not None
                    else 0
                ),
                "parse_status": str(r["parse_status"]),
            }
            for r in rows
        ],
    }
    out_path = Path(str(args.out))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
    print(f"wrote {out_path} rows={len(out['provider_games'])}")


if __name__ == "__main__":
    main()
