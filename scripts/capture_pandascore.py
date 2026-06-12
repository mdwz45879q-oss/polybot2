#!/usr/bin/env python3
"""
PandaScore + V1 multi-feed latency capture.

Captures all available PandaScore WebSocket feeds (frames, events) alongside
Kalstrop V1 for the same games. Writes timestamped JSONL files for post-hoc
latency comparison.

Usage (auto-discover from DB):
    python capture_pandascore.py \
        --db ../../data/prediction_markets.db \
        --out ./captures/ps_trial_day1 \
        --duration 14400

Usage (manual single-game):
    python capture_pandascore.py \
        --pandascore-match-id 1508687 \
        --v1-fixture-id abc-123 \
        --out ./captures/g2_vs_fut

Output per game:
    {out}/{game_name}/pandascore_frames.jsonl
    {out}/{game_name}/pandascore_events.jsonl
    {out}/{game_name}/v1_raw.jsonl
    {out}/capture_meta.json
"""

import argparse
import asyncio
import hashlib
import hmac as _hmac
import json
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

try:
    import websockets
    import websockets.exceptions
except ImportError:
    print("pip install websockets")
    sys.exit(2)


def _map_winner(h: int, a: int) -> bool:
    """CS2 closed-form map winner condition. True if the score is a decided map."""
    big, small, d = max(h, a), min(h, a), abs(h - a)
    return (big == 13 and small <= 11) or (big >= 16 and big % 3 == 1 and d >= 2 and d <= 4)


# ─── Credentials ──────────────────────────────────────────────────────

PS_TOKEN = os.environ.get("PANDASCORE_API_TOKEN", "")
V1_WS = "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"
V1_CLIENT_ID = os.environ.get("KALSTROP_CLIENT_ID") or os.environ.get("CLIENT_ID", "")
V1_SECRET_RAW = os.environ.get("KALSTROP_SHARED_SECRET_RAW") or os.environ.get("SHARED_SECRET_RAW", "")


def _v1_auth_qs() -> str:
    if not V1_CLIENT_ID or not V1_SECRET_RAW:
        return ""
    ts = str(int(time.time()))
    hashed = hashlib.sha256(V1_SECRET_RAW.encode()).hexdigest()
    sig = _hmac.new(hashed.encode(), f"{V1_CLIENT_ID}:{ts}".encode(), hashlib.sha256).hexdigest()
    return urlencode({
        "X-Client-ID": V1_CLIENT_ID,
        "X-Timestamp": ts,
        "Authorization": f"Bearer {sig}",
    })


# ─── PandaScore feed capture ─────────────────────────────────────────

async def capture_pandascore_feed(
    match_id: str,
    feed_type: str,  # "frames", "events", or "low_latency"
    out_path: Path,
    token: str,
    stop: asyncio.Event,
    start_ts_utc: int | None = None,
):
    """Capture one PandaScore WebSocket feed with reconnection.

    If `start_ts_utc` is provided and the WS isn't open yet (4004),
    waits until ~20 minutes before start then retries every 60s.
    """
    url = f"wss://live.pandascore.co/matches/{match_id}"
    if feed_type == "events":
        url += "/events"
    elif feed_type == "low_latency":
        url += "/low_latency_feed"
    url += f"?token={token}"

    source = f"ps:{feed_type}"
    count = 0
    backoff = 2.0
    last_frame_at = time.monotonic()
    last_heartbeat = time.monotonic()

    with out_path.open("a") as f:
        while not stop.is_set():
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20, max_size=10 * 1024 * 1024
                ) as ws:
                    backoff = 2.0
                    last_frame_at = time.monotonic()
                    prev_score = None
                    async for raw in ws:
                        if stop.is_set():
                            break
                        ts_ns = time.time_ns()
                        last_frame_at = time.monotonic()
                        try:
                            frame = json.loads(raw)
                        except Exception:
                            frame = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")

                        # Hello message — write to JSONL and log
                        if isinstance(frame, dict) and frame.get("type") == "hello":
                            payload = frame.get("payload", {})
                            print(f"[{source}] connected to match {match_id} (status={payload.get('status')}, game={payload.get('videogame')})")
                            f.write(json.dumps({"ts_ns": ts_ns, "source": source, "frame": frame}) + "\n")
                            f.flush()
                            continue

                        f.write(json.dumps({"ts_ns": ts_ns, "source": source, "frame": frame}) + "\n")
                        f.flush()
                        count += 1

                        # Score-reset detection (low_latency feed only)
                        if feed_type == "low_latency" and isinstance(frame, dict) and "home_team" in frame:
                            h = frame.get("home_team", {}).get("score")
                            a = frame.get("away_team", {}).get("score")
                            if h is not None and a is not None:
                                cur = (h, a)
                                if prev_score and cur == (0, 0) and prev_score != (0, 0):
                                    ph, pa = prev_score
                                    if _map_winner(ph + 1, pa):
                                        print(f"*** [{source}] MAP DECIDED: {ph}-{pa} → 0-0 (home wins) at ts={ts_ns} (match {match_id}) ***")
                                    elif _map_winner(ph, pa + 1):
                                        print(f"*** [{source}] MAP DECIDED: {ph}-{pa} → 0-0 (away wins) at ts={ts_ns} (match {match_id}) ***")
                                    else:
                                        print(f"*** [{source}] ANOMALOUS RESET: {ph}-{pa} → 0-0 at ts={ts_ns} (match {match_id}) — possible forfeit ***")
                                prev_score = cur

                        # Periodic heartbeat every 60s
                        now_mono = time.monotonic()
                        if now_mono - last_heartbeat >= 60:
                            silence = now_mono - last_frame_at
                            if silence > 120:
                                print(f"[{source}] WARNING: no data for {silence:.0f}s — connection may be stale (match {match_id})")
                            else:
                                print(f"[{source}] {count} frames captured (match {match_id})")
                            last_heartbeat = now_mono

            except websockets.exceptions.ConnectionClosedError as e:
                err_str = str(e)
                # 4003 = insufficient permissions (wrong plan) — skip permanently
                if "4003" in err_str or "Unsufficient" in err_str:
                    print(f"[{source}] access denied — skipping (match {match_id})")
                    return
                # 4004 = match unavailable (WS not open yet — opens 15min before start)
                if "4004" in err_str:
                    if start_ts_utc:
                        wait_until = start_ts_utc - 20 * 60  # 20 min before start
                        now = int(time.time())
                        if now < wait_until:
                            delay = wait_until - now
                            print(f"[{source}] match {match_id} not open yet — waiting {delay//60}min until ~20min before start")
                            try:
                                await asyncio.wait_for(stop.wait(), timeout=delay)
                            except asyncio.TimeoutError:
                                pass
                            if stop.is_set():
                                break
                            continue  # retry connection
                        else:
                            # We're within 20min of start — retry every 60s
                            print(f"[{source}] match {match_id} not open yet — retrying in 60s")
                            try:
                                await asyncio.wait_for(stop.wait(), timeout=60)
                            except asyncio.TimeoutError:
                                pass
                            if stop.is_set():
                                break
                            continue
                    else:
                        print(f"[{source}] match WS not open yet — skipping (match {match_id}, no start time)")
                        return
                if stop.is_set():
                    break
                print(f"[{source}] connection closed: {e} — reconnecting in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

            except Exception as e:
                if stop.is_set():
                    break
                print(f"[{source}] {type(e).__name__}: {e} — reconnecting in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    print(f"[{source}] done — {count} frames captured")


# ─── V1 capture (reuses pattern from capture_kalstrop_v1.py) ─────────

async def capture_v1(fixture_ids: list[str], out_path: Path, stop: asyncio.Event):
    """Capture Kalstrop V1 WebSocket frames."""
    if not V1_CLIENT_ID or not V1_SECRET_RAW:
        print("[v1] no credentials — skipping (set KALSTROP_CLIENT_ID + KALSTROP_SHARED_SECRET_RAW)")
        return

    count = 0
    backoff = 2.0
    with out_path.open("a") as f:
        while not stop.is_set():
            try:
                qs = _v1_auth_qs()
                uri = f"{V1_WS}?{qs}"
                async with websockets.connect(
                    uri, ping_interval=20, ping_timeout=20, max_size=10 * 1024 * 1024
                ) as ws:
                    await ws.send(json.dumps({"type": "connection_init", "payload": {}}))
                    await ws.send(json.dumps({
                        "id": "v1_sub",
                        "type": "subscribe",
                        "payload": {
                            "operationName": "sportsMatchStateUpdatedV2",
                            "query": (
                                "subscription sportsMatchStateUpdatedV2($fixtureIds: [String!]!)"
                                " { sportsMatchStateUpdatedV2(fixtureIds: $fixtureIds) }"
                            ),
                            "variables": {"fixtureIds": fixture_ids},
                        },
                    }))
                    print(f"[v1] subscribed to {len(fixture_ids)} fixture(s)")
                    backoff = 2.0
                    async for raw in ws:
                        if stop.is_set():
                            break
                        ts_ns = time.time_ns()
                        try:
                            frame = json.loads(raw)
                        except Exception:
                            frame = raw
                        f.write(json.dumps({"ts_ns": ts_ns, "source": "v1", "frame": frame}) + "\n")
                        f.flush()
                        count += 1
                        if isinstance(frame, dict) and frame.get("type") == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
            except Exception as e:
                if stop.is_set():
                    break
                print(f"[v1] {type(e).__name__}: {e} — reconnecting in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
    print(f"[v1] done — {count} frames captured")


# ─── Game discovery ──────────────────────────────────────────────────

def _norm(s: str) -> str:
    return s.strip().lower()


def discover_games_from_db(db_path: str, horizon_hours: int = 6) -> list[dict]:
    """Find PandaScore games starting within horizon, match to V1 fixture IDs."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now_ts = int(time.time())
    end_ts = now_ts + horizon_hours * 3600

    # PandaScore games within horizon (or already running = start_ts in past 4 hours)
    ps_rows = conn.execute(
        """SELECT provider_game_id, home_raw, away_raw, sport_raw, league_raw,
                  start_ts_utc, extra_json
           FROM provider_games
           WHERE provider = 'pandascore'
             AND start_ts_utc IS NOT NULL
             AND start_ts_utc BETWEEN ? AND ?
           ORDER BY start_ts_utc""",
        (now_ts - 4 * 3600, end_ts),
    ).fetchall()

    # V1 games in similar window for cross-matching
    v1_rows = conn.execute(
        """SELECT provider_game_id, home_raw, away_raw, start_ts_utc
           FROM provider_games
           WHERE provider = 'kalstrop_v1'
             AND start_ts_utc IS NOT NULL
             AND start_ts_utc BETWEEN ? AND ?""",
        (now_ts - 4 * 3600, end_ts),
    ).fetchall()
    conn.close()

    # Build V1 lookup: list of (norm_home, norm_away, game_id)
    v1_entries: list[tuple[str, str, str]] = []
    for r in v1_rows:
        v1_entries.append((_norm(r["home_raw"]), _norm(r["away_raw"]), r["provider_game_id"]))

    def _find_v1_id(ps_home: str, ps_away: str) -> str:
        """Match PS team names to V1 fixture ID (exact then substring)."""
        h, a = _norm(ps_home), _norm(ps_away)
        # Exact match (either order)
        for v1_h, v1_a, v1_id in v1_entries:
            if (v1_h == h and v1_a == a) or (v1_h == a and v1_a == h):
                return v1_id
        # Substring match (either order) — catches "Vitality" vs "Team Vitality"
        for v1_h, v1_a, v1_id in v1_entries:
            if ((h in v1_h or v1_h in h) and (a in v1_a or v1_a in a)):
                return v1_id
            if ((h in v1_a or v1_a in h) and (a in v1_h or v1_h in a)):
                return v1_id
        return ""

    games = []
    for r in ps_rows:
        extra = {}
        if r["extra_json"]:
            try:
                extra = json.loads(r["extra_json"])
            except Exception:
                pass

        home = r["home_raw"] or ""
        away = r["away_raw"] or ""
        safe_name = f"{_norm(home).replace(' ', '_')}_{_norm(away).replace(' ', '_')}"
        safe_name = "".join(c for c in safe_name if c.isalnum() or c in "_-")[:60]

        # Try to find V1 fixture ID (exact then substring matching)
        v1_id = _find_v1_id(home, away)

        games.append({
            "name": safe_name,
            "pandascore_match_id": str(r["provider_game_id"]),
            "v1_fixture_id": v1_id,
            "home": home,
            "away": away,
            "sport": r["sport_raw"] or "",
            "league": r["league_raw"] or "",
            "start_ts_utc": r["start_ts_utc"],
            "live_supported": extra.get("live_supported", False),
            "number_of_games": extra.get("number_of_games"),
        })

    return games


# ─── Main ────────────────────────────────────────────────────────────

async def capture_game(game: dict, out_dir: Path, token: str, stop: asyncio.Event, skip_v1: bool):
    """Capture all feeds for one game."""
    game_dir = out_dir / game["name"]
    game_dir.mkdir(parents=True, exist_ok=True)

    tasks = []
    ps_id = game.get("pandascore_match_id")
    start_ts = game.get("start_ts_utc")
    if ps_id and token:
        tasks.append(asyncio.create_task(
            capture_pandascore_feed(ps_id, "low_latency", game_dir / "pandascore_low_latency.jsonl", token, stop, start_ts)
        ))
        tasks.append(asyncio.create_task(
            capture_pandascore_feed(ps_id, "frames", game_dir / "pandascore_frames.jsonl", token, stop, start_ts)
        ))
        tasks.append(asyncio.create_task(
            capture_pandascore_feed(ps_id, "events", game_dir / "pandascore_events.jsonl", token, stop, start_ts)
        ))

    v1_id = game.get("v1_fixture_id")
    if v1_id and not skip_v1:
        tasks.append(asyncio.create_task(
            capture_v1([v1_id], game_dir / "v1_raw.jsonl", stop)
        ))

    if not tasks:
        print(f"[{game['name']}] no feeds to capture — skipping")
        return

    print(f"[{game['name']}] capturing: ps={ps_id or 'none'}, v1={v1_id or 'none'}")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            print(f"[{game['name']}] task error: {r}")


def main():
    ap = argparse.ArgumentParser(description="PandaScore + V1 multi-feed latency capture")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=14400, help="Max seconds (default 14400 = 4h)")

    # Manual mode
    ap.add_argument("--pandascore-match-id", type=str, default="", help="Manual: PandaScore match ID")
    ap.add_argument("--v1-fixture-id", type=str, default="", help="Manual: V1 fixture ID")
    ap.add_argument("--game-name", type=str, default="manual_game", help="Manual: output dir name")

    # Auto-discover mode
    ap.add_argument("--db", type=str, default="", help="SQLite DB path for auto-discovery")
    ap.add_argument("--horizon-hours", type=int, default=6, help="Discover games starting within N hours")
    ap.add_argument("--games-file", type=str, default="", help="JSON file with game list")

    # Options
    ap.add_argument("--no-v1", action="store_true", help="Skip V1 capture")
    ap.add_argument("--live-only", action="store_true", help="Only capture games with live_supported=true")

    args = ap.parse_args()

    if not PS_TOKEN:
        print("WARNING: PANDASCORE_API_TOKEN not set — PandaScore feeds will fail")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Discover games
    games: list[dict] = []

    if args.pandascore_match_id:
        # Manual mode
        games.append({
            "name": args.game_name,
            "pandascore_match_id": args.pandascore_match_id,
            "v1_fixture_id": args.v1_fixture_id,
            "home": "",
            "away": "",
            "sport": "",
            "league": "",
            "live_supported": True,
        })
    elif args.games_file:
        with open(args.games_file) as f:
            games = json.load(f)
    elif args.db:
        games = discover_games_from_db(args.db, args.horizon_hours)
    else:
        print("ERROR: provide --pandascore-match-id, --games-file, or --db for game discovery")
        return 1

    if args.live_only:
        games = [g for g in games if g.get("live_supported")]

    if not games:
        print("No games to capture.")
        return 0

    # Write metadata
    meta = {
        "start_time": datetime.now(timezone.utc).isoformat(),
        "duration": args.duration,
        "ps_token_prefix": PS_TOKEN[:8] + "..." if PS_TOKEN else "none",
        "v1_credentials": "present" if V1_CLIENT_ID else "missing",
        "skip_v1": args.no_v1,
        "games": games,
    }
    with (out_dir / "capture_meta.json").open("w") as f:
        json.dump(meta, f, indent=2, default=str)

    print(f"Capturing {len(games)} game(s) for up to {args.duration}s")
    print(f"Output: {out_dir}")
    for g in games:
        live = "✓" if g.get("live_supported") else "✗"
        v1 = g.get("v1_fixture_id", "")[:12] or "none"
        print(f"  {g.get('home', '?')} vs {g.get('away', '?')} | ps={g.get('pandascore_match_id')} v1={v1} live={live}")
    print()

    # Signal handling
    stop_event = asyncio.Event()

    def handle_sig(*_):
        print("\n[signal] stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    # Run capture
    async def run():
        tasks = [
            asyncio.create_task(capture_game(g, out_dir, PS_TOKEN, stop_event, args.no_v1))
            for g in games
        ]
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=args.duration)
        except asyncio.TimeoutError:
            print(f"\n[+] duration ({args.duration}s) elapsed")
        finally:
            stop_event.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        pass

    # Summary
    print("\n=== Capture summary ===")
    for g in games:
        game_dir = out_dir / g["name"]
        for fname in ("pandascore_frames.jsonl", "pandascore_events.jsonl", "v1_raw.jsonl"):
            fpath = game_dir / fname
            if fpath.exists():
                lines = sum(1 for _ in fpath.open())
                print(f"  {g['name']}/{fname}: {lines} lines")

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
