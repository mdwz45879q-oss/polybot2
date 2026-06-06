#!/usr/bin/env python3
"""
BoltOdds dual capture: stream both odds and live scores for a league.

Usage:
    python scripts/capture_boltodds.py --league lol --duration 14400
    python scripts/capture_boltodds.py --league cs2 dota2 --horizon-hours 12
    python scripts/capture_boltodds.py --league lol --date 2026-06-07

Prerequisites:
    polybot2 provider sync --provider boltodds
"""

import argparse
import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import websockets
except ImportError:
    print("pip install websockets"); sys.exit(2)

sys.path.insert(0, str(Path(__file__).parent))
from build_capture_plan import load_config, find_games, make_name, ts_to_et

ET = ZoneInfo("America/New_York")

BOLTODDS_API_KEY = os.environ.get("BOLTODDS_API_KEY", "")
BOLTODDS_ODDS_WS = "wss://spro.agency/api"
BOLTODDS_SCORES_WS = "wss://spro.agency/api/livescores"

RECV_TIMEOUT = 45  # declare connection dead after this long with zero frames


# ─── Generic BoltOdds WS stream ───────────────────────────────────────

async def _bo_stream(
    label: str,
    ws_base: str,
    game_labels: list[str],
    label_to_name: dict[str, str],
    files: dict[str, any],
    events_file: any,
    stop: asyncio.Event,
    on_event=None,
):
    """Generic BoltOdds WS capture loop. Used for both odds and scores."""
    count = 0
    backoff = 2.0

    while not stop.is_set():
        try:
            uri = f"{ws_base}?key={BOLTODDS_API_KEY}"
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20,
                                          max_size=10*1024*1024) as ws:
                # Wait for socket_connected ack
                try:
                    ack = await asyncio.wait_for(ws.recv(), timeout=10)
                    ts_ns = time.time_ns()
                    try:
                        ack_parsed = json.loads(ack)
                    except Exception:
                        ack_parsed = ack
                    events_file.write(json.dumps({"ts_ns": ts_ns, "source": f"bo_{label}", "frame": ack_parsed}) + "\n")
                    events_file.flush()
                except asyncio.TimeoutError:
                    print(f"  [bo_{label}] no ack — reconnecting")
                    continue

                # Subscribe
                sub = {"action": "subscribe", "filters": {"games": game_labels}}
                await ws.send(json.dumps(sub))
                print(f"  [bo_{label}] subscribed to {len(game_labels)} games")
                backoff = 2.0
                last_activity = time.monotonic()

                while not stop.is_set():
                    idle = time.monotonic() - last_activity

                    # Receive with timeout — detect silent disconnects
                    remaining = max(1.0, RECV_TIMEOUT - idle)
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    except asyncio.TimeoutError:
                        print(f"  [bo_{label}] no data for {RECV_TIMEOUT}s — reconnecting")
                        break

                    last_activity = time.monotonic()
                    ts_ns = time.time_ns()

                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    # BoltOdds odds messages are JSON arrays; scores are objects
                    events = msg if isinstance(msg, list) else [msg]

                    for evt in events:
                        if not isinstance(evt, dict):
                            continue

                        action = evt.get("action", "")

                        # Skip pings
                        if action == "ping":
                            continue

                        # Route to per-game file
                        game_key = _extract_game_label(evt)
                        game_name = label_to_name.get(game_key, "") if game_key else ""
                        target_f = files.get(game_name) if game_name else None

                        line = {
                            "ts_ns": ts_ns,
                            "source": f"bo_{label}",
                            "action": action,
                        }
                        if game_key:
                            line["game"] = game_key
                        line["data"] = evt.get("data", evt)

                        if target_f:
                            target_f.write(json.dumps(line) + "\n")
                            target_f.flush()
                        else:
                            events_file.write(json.dumps(line) + "\n")
                            events_file.flush()

                        count += 1

                        if on_event:
                            on_event(game_name or game_key or "?", action, evt, ts_ns)

        except Exception as e:
            if stop.is_set():
                break
            print(f"  [bo_{label}] {type(e).__name__}: {e} — reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    print(f"  [bo_{label}] done — {count} events")


def _extract_game_label(evt: dict) -> str:
    """Extract the game label from a BoltOdds event."""
    # Scores: top-level "game" (traditional sports) or "event" (esports)
    game = evt.get("game") or evt.get("event")
    if game and isinstance(game, str):
        return game
    # Odds: nested in "data.game" or "data.event"
    data = evt.get("data")
    if isinstance(data, dict):
        game = data.get("game") or data.get("event")
        if game and isinstance(game, str):
            return game
    return ""


# ─── Main ──────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="BoltOdds odds + scores capture")
    ap.add_argument("--league", nargs="+", required=True, help="Canonical league key(s)")
    ap.add_argument("--date", default="", help="Date YYYY-MM-DD (calendar day in ET)")
    ap.add_argument("--horizon-hours", type=float, default=0,
                    help="Capture window: games starting within N hours from now (default: 24 if --date not given)")
    ap.add_argument("--duration", type=int, default=14400, help="Max seconds (default 14400 = 4h)")
    ap.add_argument("--db", default="data/prediction_markets.db", help="DB path")
    ap.add_argument("--out", default="", help="Output dir (default: captures/{date}/{league}/)")
    ap.add_argument("--games", nargs="+", default=[], metavar="TEAM",
                    help="Filter to games matching these team names (substring match, case-insensitive)")
    ap.add_argument("--config-dir", default="", help="Config dir (default: auto)")
    args = ap.parse_args()

    if not BOLTODDS_API_KEY:
        print("ERROR: set BOLTODDS_API_KEY environment variable")
        return 1

    config_dir = args.config_dir or str(Path(__file__).resolve().parents[1] / "config")
    cfg = load_config(config_dir)

    # Parse time window
    if args.date:
        dt = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=ET)
        date_start = int(dt.timestamp())
        date_end = date_start + 86400
        date_str = args.date
    else:
        horizon = args.horizon_hours if args.horizon_hours > 0 else 24
        now = int(time.time())
        date_start = now - 6 * 3600  # 6h lookback for live games
        date_end = now + int(horizon * 3600)
        date_str = datetime.now(ET).strftime("%Y-%m-%d")

    if not Path(args.db).exists():
        print(f"ERROR: database not found: {args.db}")
        return 1

    conn = sqlite3.connect(args.db)
    conn.row_factory = None

    # Discover BoltOdds games
    all_games: list[dict] = []
    league_label = "+".join(args.league)

    for league in args.league:
        league = league.strip().lower()
        league_cfg = cfg["LEAGUES"].get(league, {})
        if not league_cfg:
            print(f"WARNING: league '{league}' not found in config")
            continue

        by_provider = find_games(
            conn, league, date_start, date_end,
            cfg["PROVIDER_LEAGUE_ALIASES"], cfg["PROVIDER_LEAGUE_COUNTRY"],
        )

        bo_games = by_provider.get("boltodds", [])
        if not bo_games:
            print(f"[{league}] No BoltOdds games found")
            continue

        print(f"[{league}] Found {len(bo_games)} BoltOdds games")
        for i, g in enumerate(bo_games, 1):
            name = make_name(g["home_raw"], g["away_raw"])
            kickoff = ts_to_et(g["start_ts_utc"])
            gl = g["game_label"]
            print(f"  [{i}] {name} ({kickoff} ET) — {gl}")
            all_games.append({
                "name": name,
                "game_label": gl,
                "home_raw": g["home_raw"],
                "away_raw": g["away_raw"],
                "start_ts_utc": g["start_ts_utc"],
                "league": league,
            })

    conn.close()

    # Filter by --games if provided
    if args.games and all_games:
        filters = [f.strip().lower() for f in args.games]
        before = len(all_games)
        all_games = [
            g for g in all_games
            if any(f in g["game_label"].lower() or f in g["name"].lower() for f in filters)
        ]
        print(f"\n[filter] {before} → {len(all_games)} games matching: {', '.join(args.games)}")

    if not all_games:
        print("\nNo BoltOdds games found for the given league(s) and date.")
        return 0

    print(f"\n[+] {len(all_games)} game(s) ready for capture")

    # Output dir
    out_dir = Path(args.out) if args.out else Path(f"captures/{date_str.replace('-', '_')}/{league_label}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build game label → name mapping and open per-game files
    game_labels: list[str] = []
    label_to_name: dict[str, str] = {}
    odds_files: dict[str, any] = {}
    scores_files: dict[str, any] = {}

    for g in all_games:
        name = g["name"]
        gl = g["game_label"]
        game_labels.append(gl)
        label_to_name[gl] = name

        game_dir = out_dir / name
        game_dir.mkdir(parents=True, exist_ok=True)
        odds_files[name] = (game_dir / "bo_odds.jsonl").open("a")
        scores_files[name] = (game_dir / "bo_scores.jsonl").open("a")

    events_file = (out_dir / "bo_events.jsonl").open("a")

    # Event printers
    def on_odds_event(game_name, action, evt, ts_ns):
        if action == "initial_state":
            data = evt.get("data", {})
            n = len(data.get("outcomes", {}))
            print(f"  [odds]  {game_name}: initial_state ({n} outcomes)")
        elif action == "game_removed":
            print(f"  [odds]  {game_name}: game_removed")
        elif action == "game_update":
            data = evt.get("data", {})
            n = len(data.get("outcomes", {}))
            print(f"  [odds]  {game_name}: game_update ({n} outcomes)")
        elif action == "line_update":
            data = evt.get("data", {})
            outcomes = data.get("outcomes", {})
            for k, v in outcomes.items():
                odds = v.get("odds", "?")
                status = "suspended" if odds in (None, "", "None") else odds
                print(f"  [odds]  {game_name}: {k} → {status}")

    def on_scores_event(game_name, action, evt, ts_ns):
        if action == "match_update":
            state = evt.get("state", {})
            score_a = state.get("teams", {}).get("home", {}).get("score", "?")
            score_b = state.get("teams", {}).get("away", {}).get("score", "?")
            period = state.get("matchPeriod", "")
            if isinstance(period, list) and len(period) > 1:
                period = period[1]
            completed = state.get("matchCompleted", False)
            tag = " [FINISHED]" if completed else ""
            print(f"  [score] {game_name}: {score_a}-{score_b} ({period}){tag}")

    # Signal handling
    stop = asyncio.Event()
    def handle_sig(*_):
        stop.set()
    signal.signal(signal.SIGTERM, handle_sig)

    async def run():
        odds_task = asyncio.create_task(_bo_stream(
            label="odds",
            ws_base=BOLTODDS_ODDS_WS,
            game_labels=game_labels,
            label_to_name=label_to_name,
            files=odds_files,
            events_file=events_file,
            stop=stop,
            on_event=on_odds_event,
        ))
        scores_task = asyncio.create_task(_bo_stream(
            label="scores",
            ws_base=BOLTODDS_SCORES_WS,
            game_labels=game_labels,
            label_to_name=label_to_name,
            files=scores_files,
            events_file=events_file,
            stop=stop,
            on_event=on_scores_event,
        ))

        try:
            await asyncio.wait_for(
                asyncio.gather(odds_task, scores_task),
                timeout=args.duration,
            )
        except asyncio.TimeoutError:
            print(f"\n[+] Duration ({args.duration}s) elapsed")
        except asyncio.CancelledError:
            pass
        finally:
            stop.set()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[+] Interrupted")
    finally:
        for f in odds_files.values():
            try: f.flush(); f.close()
            except Exception: pass
        for f in scores_files.values():
            try: f.flush(); f.close()
            except Exception: pass
        try: events_file.flush(); events_file.close()
        except Exception: pass

    # Summary
    print(f"\n[+] Output in {out_dir}/")
    for item in sorted(out_dir.iterdir()):
        if item.is_dir():
            for p in sorted(item.glob("*.jsonl")):
                lines = sum(1 for _ in p.open())
                print(f"  {item.name}/{p.name}: {lines} lines")
    events_path = out_dir / "bo_events.jsonl"
    if events_path.exists():
        lines = sum(1 for _ in events_path.open())
        print(f"  bo_events.jsonl: {lines} lines")


if __name__ == "__main__":
    sys.exit(main() or 0)
