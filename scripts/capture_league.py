#!/usr/bin/env python3
"""
One-command capture: discover today's games for a league, then stream
V1 scores + odds for all of them.

Usage:
    python scripts/capture_league.py --league cs2 --duration 14400
    python scripts/capture_league.py --league cs2 mlb --date 2026-06-06
    python scripts/capture_league.py --league epl --tz et --duration 7200

Prerequisites:
    polybot2 provider sync --provider kalstrop_v1
"""

import argparse
import asyncio
import signal
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Import game discovery from build_capture_plan.py
sys.path.insert(0, str(Path(__file__).parent))
from build_capture_plan import (
    load_config,
    find_games,
    match_games,
    build_alias_index,
    make_name,
    ts_to_et,
)
from capture_odds import capture


ET = ZoneInfo("America/New_York")


def main():
    ap = argparse.ArgumentParser(description="Auto-discover + capture V1 scores + odds")
    ap.add_argument("--league", nargs="+", required=True, help="Canonical league key(s)")
    ap.add_argument("--date", default="", help="Date YYYY-MM-DD (default: today)")
    ap.add_argument("--tz", default="utc", choices=["utc", "et"],
                    help="Timezone for date range (default: utc)")
    ap.add_argument("--duration", type=int, default=14400, help="Max seconds (default 14400 = 4h)")
    ap.add_argument("--db", default="data/prediction_markets.db", help="DB path")
    ap.add_argument("--out", default="", help="Output dir (default: captures/{date}/{league}/)")
    ap.add_argument("--config-dir", default="", help="Config dir (default: auto)")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    config_dir = args.config_dir or str(Path(__file__).resolve().parents[1] / "config")
    cfg = load_config(config_dir)

    # Parse date
    if args.date:
        date_str = args.date
    else:
        now = datetime.now(ET if args.tz == "et" else timezone.utc)
        date_str = now.strftime("%Y-%m-%d")

    if args.tz == "et":
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ET)
    else:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    date_start = int(dt.timestamp())
    date_end = date_start + 86400

    if not Path(args.db).exists():
        print(f"ERROR: database not found: {args.db}")
        return 1

    conn = sqlite3.connect(args.db)
    conn.row_factory = None

    # Discover games
    fixture_ids: list[str] = []
    game_names: list[str] = []
    league_label = "+".join(args.league)

    for league in args.league:
        league = league.strip().lower()
        league_cfg = cfg["LEAGUES"].get(league, {})
        if not league_cfg:
            print(f"WARNING: league '{league}' not found in config")
            continue

        alias_index = build_alias_index(cfg["TEAM_MAP"], league)
        by_provider = find_games(
            conn, league, date_start, date_end,
            cfg["PROVIDER_LEAGUE_ALIASES"], cfg["PROVIDER_LEAGUE_COUNTRY"],
        )

        total = sum(len(v) for v in by_provider.values())
        provider_summary = ", ".join(f"{k}={len(v)}" for k, v in sorted(by_provider.items()))
        print(f"[{league}] Found {total} provider entries: {provider_summary}")

        matched = match_games(by_provider, league, alias_index, verbose=args.verbose)

        for m in matched:
            providers = m["providers"]
            if "kalstrop_v1" not in providers:
                continue
            fid = providers["kalstrop_v1"]["provider_game_id"]
            name = make_name(m["canon_home"], m["canon_away"])
            kickoff = ts_to_et(m["start_ts_utc"])
            fixture_ids.append(fid)
            game_names.append(name)
            print(f"  [{len(fixture_ids)}] {name} ({kickoff} ET) — {fid}")

    conn.close()

    if not fixture_ids:
        print("\nNo V1 fixtures found for the given league(s) and date.")
        return 0

    print(f"\n[+] {len(fixture_ids)} fixture(s) ready for capture")

    # Output dir
    out_dir = Path(args.out) if args.out else Path(f"captures/{date_str.replace('-', '_')}/{league_label}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Signal handling
    stop = asyncio.Event()
    def handle_sig(*_):
        print("\n[signal] stopping...")
        stop.set()
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    # Capture
    async def run():
        try:
            await asyncio.wait_for(
                capture(fixture_ids, game_names, out_dir, args.duration, stop),
                timeout=args.duration,
            )
        except asyncio.TimeoutError:
            print(f"\n[+] Duration ({args.duration}s) elapsed")
        finally:
            stop.set()

    asyncio.run(run())

    # Summary
    print(f"\n[+] Output in {out_dir}/")
    for game_dir in sorted(out_dir.iterdir()):
        if game_dir.is_dir():
            for p in sorted(game_dir.glob("*.jsonl")):
                lines = sum(1 for _ in p.open())
                print(f"  {game_dir.name}/{p.name}: {lines} lines")


if __name__ == "__main__":
    sys.exit(main() or 0)
