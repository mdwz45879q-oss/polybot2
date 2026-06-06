#!/usr/bin/env python3
"""
Standalone V2 (BetGenius) capture: auto-discover games, resolve fixture IDs,
and stream Socket.IO events to JSONL files.

Usage:
    python scripts/capture_v2.py --league fifa_friendly --duration 14400
    python scripts/capture_v2.py --league ucl laliga --duration 7200
    python scripts/capture_v2.py --league fifa_friendly --date 2026-06-07

Prerequisites:
    polybot2 provider sync --provider kalstrop_v2
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

try:
    import websockets
except ImportError:
    print("pip install websockets"); sys.exit(2)

# Add scripts/ and project root to path for imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from build_capture_plan import load_config, find_games, make_name, ts_to_et
from polybot2.hotpath.v2_resolver import (
    _fetch_tournament_fixtures,
    _match_fixture,
    _resolve_fixture_id,
)
from polybot2.sports.kalstrop_auth import kalstrop_auth_headers

ET = ZoneInfo("America/New_York")

CLIENT_ID = os.environ.get("KALSTROP_CLIENT_ID") or os.environ.get("CLIENT_ID", "")
SECRET_RAW = os.environ.get("KALSTROP_SHARED_SECRET_RAW") or os.environ.get("SHARED_SECRET_RAW", "")

# Keepalive / silent-disconnect detection
PING_INTERVAL = 15   # send Engine.IO ping if idle this long (seconds)
RECV_TIMEOUT  = 45   # declare connection dead after this long with zero frames


# ─── V2 auth ───────────────────────────────────────────────────────────

def _v2_auth_qs() -> str:
    headers = kalstrop_auth_headers(CLIENT_ID, SECRET_RAW)
    return urlencode({"product": "genius-stats", **headers})


# ─── Per-game V2 capture ───────────────────────────────────────────────

async def _v2_capture(
    game_name: str,
    provider: dict,
    out_dir: Path,
    stop: asyncio.Event,
):
    """Capture V2 Genius Stats via raw websocket (Engine.IO/Socket.IO).

    Mirrors the Rust implementation in kalstrop_v2_sio.rs.
    Adds keepalive pings + receive timeout for reliability.
    """
    fixture_id = str(provider["fixture_id"])
    game_dir = out_dir / game_name
    game_dir.mkdir(parents=True, exist_ok=True)
    out_path = game_dir / "v2_raw.jsonl"

    count = 0
    backoff = 2.0

    with out_path.open("a") as f:
        while not stop.is_set():
            try:
                auth_qs = _v2_auth_qs()
                ws_url = f"wss://stats.kalstropservice.com/socket.io/?EIO=4&transport=websocket&{auth_qs}"

                async with websockets.connect(ws_url, ping_interval=None, ping_timeout=None,
                                              max_size=10*1024*1024) as ws:
                    # Engine.IO OPEN: 0{"sid":"...","pingInterval":25000,...}
                    msg = await asyncio.wait_for(ws.recv(), timeout=10)
                    if isinstance(msg, bytes):
                        msg = msg.decode()
                    if not msg.startswith("0"):
                        print(f"  [v2/{game_name}] bad OPEN: {msg[:60]}")
                        break

                    # Socket.IO CONNECT
                    await ws.send("40")

                    # Socket.IO CONNECT ACK: 40{"sid":"..."}
                    msg = await asyncio.wait_for(ws.recv(), timeout=10)
                    if isinstance(msg, bytes):
                        msg = msg.decode()
                    if not msg.startswith("40"):
                        print(f"  [v2/{game_name}] bad ACK: {msg[:60]}")
                        break

                    # Subscribe
                    sub_payload = json.dumps(["genius_subscribe", {
                        "fixtureId": fixture_id,
                        "activeContent": "court",
                        "sport": provider.get("sport"),
                        "sportId": provider.get("sport_id"),
                        "competitionId": provider.get("competition_id"),
                    }])
                    await ws.send(f"42{sub_payload}")
                    print(f"  [v2/{game_name}] subscribed to fixture_id={fixture_id}")
                    backoff = 2.0
                    last_activity = time.monotonic()

                    try:
                        while not stop.is_set():
                            now = time.monotonic()
                            idle = now - last_activity

                            # Client-initiated Engine.IO keepalive ping
                            if idle >= PING_INTERVAL:
                                await ws.send("2")
                                last_activity = now
                                idle = 0.0

                            # Receive with timeout — detect silent disconnects
                            remaining = max(1.0, RECV_TIMEOUT - idle)
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                            except asyncio.TimeoutError:
                                print(f"  [v2/{game_name}] no data for {RECV_TIMEOUT}s — reconnecting")
                                break

                            last_activity = time.monotonic()

                            if isinstance(raw, bytes):
                                raw = raw.decode()

                            # Engine.IO PING → PONG
                            if raw == "2":
                                await ws.send("3")
                                continue

                            # Engine.IO PONG (response to our ping)
                            if raw == "3":
                                continue

                            # Socket.IO EVENT: 42["event_name", data]
                            if raw.startswith("42"):
                                ts_ns = time.time_ns()
                                try:
                                    payload = json.loads(raw[2:])
                                    event_name = payload[0] if isinstance(payload, list) else "unknown"
                                    event_data = payload[1] if isinstance(payload, list) and len(payload) > 1 else {}
                                except (json.JSONDecodeError, IndexError):
                                    event_name = "parse_error"
                                    event_data = raw[2:100]

                                f.write(json.dumps({
                                    "ts_ns": ts_ns,
                                    "source": "v2",
                                    "event": event_name,
                                    "fixture_id": fixture_id,
                                    "data": event_data,
                                }) + "\n")
                                f.flush()
                                count += 1

                                # Print summary for key events
                                if event_name == "subscribed":
                                    d = event_data.get("data", {}) if isinstance(event_data, dict) else {}
                                    sb = d.get("scoreboardInfo", {})
                                    if sb:
                                        print(f"  [v2/{game_name}] initial: {sb.get('homeScore')}-{sb.get('awayScore')} phase={sb.get('currentPhase')}")
                                elif event_name == "genius_update":
                                    d = event_data if isinstance(event_data, dict) else {}
                                    sb = d.get("scoreboardInfo", {})
                                    if sb:
                                        hs = sb.get("homeScore", "?")
                                        aws = sb.get("awayScore", "?")
                                        cp = sb.get("currentPhase", "?")
                                        print(f"  [v2/{game_name}] {hs}-{aws} ({cp}) [{count} frames]")
                                elif event_name == "error":
                                    print(f"  [v2/{game_name}] error: {event_data}")
                    finally:
                        # Clean unsubscribe
                        try:
                            unsub = json.dumps(["genius_unsubscribe", {
                                "fixtureId": fixture_id,
                                "activeContent": "court",
                            }])
                            await ws.send(f"42{unsub}")
                        except Exception:
                            pass

            except Exception as e:
                if stop.is_set():
                    break
                print(f"  [v2/{game_name}] {type(e).__name__}: {e} — reconnecting in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    print(f"  [v2/{game_name}] done — {count} events")


# ─── Resolution poll loop ──────────────────────────────────────────────

async def _resolve_and_capture(
    pending_games: list[dict],
    out_dir: Path,
    stop: asyncio.Event,
    resolve_interval: int,
    sport_slug: str = "football",
):
    """Poll for V2 fixture resolution. Spawn capture tasks as games resolve."""
    if not CLIENT_ID or not SECRET_RAW:
        print("[!] No credentials — set KALSTROP_CLIENT_ID and KALSTROP_SHARED_SECRET_RAW")
        return

    pending = list(pending_games)
    capture_tasks: list[asyncio.Task] = []

    while pending and not stop.is_set():
        now = int(time.time())

        # Split into eligible (kickoff passed) and waiting (still prematch)
        eligible = [g for g in pending if (g["start_ts_utc"] or 0) <= now]
        waiting = [g for g in pending if (g["start_ts_utc"] or 0) > now]

        if waiting and not eligible:
            # All games are prematch — sleep until the earliest kickoff
            next_kickoff = min(g["start_ts_utc"] for g in waiting)
            wait_secs = next_kickoff - now
            next_name = min(waiting, key=lambda g: g["start_ts_utc"])["name"]
            print(f"  [resolve] {len(waiting)} game(s) prematch — next kickoff: {next_name} in {wait_secs//60}m{wait_secs%60}s")
            try:
                await asyncio.wait_for(stop.wait(), timeout=min(wait_secs, resolve_interval))
            except asyncio.TimeoutError:
                pass
            continue

        if not eligible:
            break

        headers = kalstrop_auth_headers(CLIENT_ID, SECRET_RAW)

        # Group eligible games by (category_slug, tournament_slug) to minimize API calls
        by_tournament: dict[tuple[str, str], list[dict]] = {}
        for g in eligible:
            key = (g["category_slug"], g["tournament_slug"])
            by_tournament.setdefault(key, []).append(g)

        newly_resolved: list[dict] = []

        for (cat, tourn), games in by_tournament.items():
            fixtures = _fetch_tournament_fixtures(cat, tourn, headers, sport_slug=sport_slug)
            if not fixtures:
                continue

            for game in games:
                match, is_finished = _match_fixture(
                    fixtures, game["home_raw"], game["away_raw"],
                    game["start_ts_utc"], 900,
                )
                if is_finished:
                    print(f"  [{game['name']}] already finished — skipping")
                    newly_resolved.append(game)
                    continue
                if match is None:
                    continue

                live_eid = str(match.get("event_id") or "").strip()
                if not live_eid:
                    continue

                if live_eid != game["event_id"]:
                    print(f"  [{game['name']}] event_id rotated: {game['event_id']} → {live_eid}")

                provider = _resolve_fixture_id(live_eid, headers, sport_slug=sport_slug)
                if not provider or not provider.get("fixture_id"):
                    continue

                fid = provider["fixture_id"]
                print(f"  [{game['name']}] resolved → fixture_id={fid}")
                newly_resolved.append(game)

                # Spawn capture task
                task = asyncio.create_task(
                    _v2_capture(game["name"], provider, out_dir, stop)
                )
                capture_tasks.append(task)

        for g in newly_resolved:
            if g in pending:
                pending.remove(g)

        if pending:
            now2 = int(time.time())
            eligible_pending = [g for g in pending if (g["start_ts_utc"] or 0) <= now2]
            waiting_pending = [g for g in pending if (g["start_ts_utc"] or 0) > now2]
            parts = []
            if eligible_pending:
                parts.append(f"{len(eligible_pending)} live")
            if waiting_pending:
                parts.append(f"{len(waiting_pending)} prematch")
            # Sleep until next kickoff or resolve_interval, whichever is sooner
            sleep_secs = resolve_interval
            if waiting_pending and not eligible_pending:
                next_ko = min(g["start_ts_utc"] for g in waiting_pending)
                sleep_secs = min(max(1, next_ko - now2), resolve_interval)
            print(f"  [resolve] {' + '.join(parts)} pending — retrying in {sleep_secs}s")
            try:
                await asyncio.wait_for(stop.wait(), timeout=sleep_secs)
            except asyncio.TimeoutError:
                pass

    if not pending:
        print("  [resolve] all games resolved")

    # Wait for capture tasks to complete
    if capture_tasks:
        await asyncio.gather(*capture_tasks, return_exceptions=True)


# ─── Main ──────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Standalone V2 (BetGenius) capture")
    ap.add_argument("--league", nargs="+", required=True, help="Canonical league key(s)")
    ap.add_argument("--date", default="", help="Date YYYY-MM-DD (calendar day in ET)")
    ap.add_argument("--horizon-hours", type=float, default=0,
                    help="Capture window: games starting within N hours from now (default: 24 if --date not given)")
    ap.add_argument("--duration", type=int, default=14400, help="Max seconds (default 14400 = 4h)")
    ap.add_argument("--db", default="data/prediction_markets.db", help="DB path")
    ap.add_argument("--out", default="", help="Output dir (default: captures/{date}/{league}/)")
    ap.add_argument("--resolve-interval", type=int, default=60,
                    help="Seconds between resolution polls (default 60)")
    ap.add_argument("--config-dir", default="", help="Config dir (default: auto)")
    args = ap.parse_args()

    import sqlite3

    config_dir = args.config_dir or str(Path(__file__).resolve().parents[1] / "config")
    cfg = load_config(config_dir)

    # Parse time window
    if args.date:
        # Calendar day mode
        dt = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=ET)
        date_start = int(dt.timestamp())
        date_end = date_start + 86400
        date_str = args.date
    else:
        # Horizon mode: from (now - lookback) to (now + horizon)
        # Lookback catches games that already kicked off and are still live
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

    # Discover V2 games
    all_v2_games: list[dict] = []
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

        v2_games = by_provider.get("kalstrop_v2", [])
        if not v2_games:
            print(f"[{league}] No V2 games found for {date_str}")
            continue

        print(f"[{league}] Found {len(v2_games)} V2 games for {date_str}")
        for i, g in enumerate(v2_games, 1):
            name = make_name(g["home_raw"], g["away_raw"])
            kickoff = ts_to_et(g["start_ts_utc"])
            print(f"  [{i}] {name} ({kickoff} ET) — event_id={g['provider_game_id']}")
            all_v2_games.append({
                "name": name,
                "event_id": g["provider_game_id"],
                "category_slug": g["category_name"],
                "tournament_slug": g["league_raw"],
                "home_raw": g["home_raw"],
                "away_raw": g["away_raw"],
                "start_ts_utc": g["start_ts_utc"],
                "league": league,
            })

    conn.close()

    if not all_v2_games:
        print("\nNo V2 games found for the given league(s) and date.")
        return 0

    print(f"\n[+] {len(all_v2_games)} game(s) ready for capture")

    # Output dir
    out_dir = Path(args.out) if args.out else Path(f"captures/{date_str.replace('-', '_')}/{league_label}")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Signal handling
    stop = asyncio.Event()
    def handle_sig(*_):
        stop.set()
    signal.signal(signal.SIGTERM, handle_sig)

    # Run
    async def run():
        try:
            await asyncio.wait_for(
                _resolve_and_capture(all_v2_games, out_dir, stop, args.resolve_interval),
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

    # Summary
    print(f"\n[+] Output in {out_dir}/")
    for game_dir in sorted(out_dir.iterdir()):
        if game_dir.is_dir():
            for p in sorted(game_dir.glob("*.jsonl")):
                lines = sum(1 for _ in p.open())
                print(f"  {game_dir.name}/{p.name}: {lines} lines")


if __name__ == "__main__":
    sys.exit(main() or 0)
