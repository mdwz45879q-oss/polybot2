#!/usr/bin/env python3
"""
Triple-capture: record raw messages from Kalstrop V1 (GraphQL WS),
Kalstrop V2 (Socket.IO), and BoltOdds (WS) for sports fixtures.

Single-game usage:
    python capture_multi.py \
        --v1-fixture-id 6534302a-c0ca-42ff-b384-c9598dc9ebc8 \
        --v2-event-id 7490622 \
        --boltodds-game-label "Man Utd vs Liverpool" \
        --out ./captures/manutd_liverpool \
        --duration 7200

Multi-game usage (from games.json):
    python capture_multi.py \
        --games-file ./captures/soccer_2026_05_03/games.json \
        --out ./captures/soccer_2026_05_03 \
        --duration 7200

Output (multi-game):
    {out}/{game_name}/v1_raw.jsonl
    {out}/{game_name}/v2_raw.jsonl       (soccer only -- skipped for baseball)
    {out}/{game_name}/boltodds_raw.jsonl

games.json format:
    [
      {
        "name": "manutd_liverpool",
        "sport": "soccer",
        "kickoff_et": "14:30",
        "v1_fixture_id": "6534302a-...",
        "v2_event_id": "7490622",
        "boltodds_game_label": "Man Utd vs Liverpool"
      },
      {
        "name": "cubs_dbacks",
        "sport": "baseball",
        "kickoff_et": "20:10",
        "v1_fixture_id": "d3f41158-...",
        "boltodds_game_label": "Chicago Cubs vs Arizona Diamondbacks, 2026-05-03"
      }
    ]

    Fields:
      name            -- directory name for output files (required)
      sport           -- "soccer" (default) or "baseball". V2 capture is skipped
                         for non-soccer sports (V2/BetGenius covers soccer only).
      kickoff_et      -- kickoff time in US Eastern, "HH:MM" format (optional).
                         V2 resolution is deferred until 10 minutes before kickoff.
      v1_fixture_id   -- Kalstrop V1 fixture UUID (optional)
      v2_event_id     -- Kalstrop V2 event ID (optional, soccer only)
      boltodds_game_label -- BoltOdds game label string (optional)

Timestamps: all JSONL output uses "ts_ns" (integer nanoseconds from time.time_ns()).
"""

import argparse
import asyncio
import hashlib
import hmac as _hmac
import json
import os
import signal
import sys
import time
from pathlib import Path

import requests

try:
    import websockets
except ImportError:
    print("pip install websockets"); sys.exit(2)

try:
    import socketio
except ImportError:
    print("pip install 'python-socketio[client]'"); sys.exit(2)


# ─── Endpoints & credentials ────────────────────────────────────────────

V1_WS = "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"
V2_BASE = "https://stats.kalstropservice.com"
V2_API = f"{V2_BASE}/api/v2"
V2_SIO_PATH = "/socket.io"
BOLTODDS_SCORES_WS = "wss://spro.agency/api/livescores"

CLIENT_ID = os.environ.get("KALSTROP_CLIENT_ID") or os.environ.get("CLIENT_ID", "")
SECRET_RAW = os.environ.get("KALSTROP_SHARED_SECRET_RAW") or os.environ.get("SHARED_SECRET_RAW", "")
BOLTODDS_API_KEY = os.environ.get("BOLTODDS_API_KEY", "")


# ─── V1 auth ────────────────────────────────────────────────────────────

def v1_auth_qs():
    if not CLIENT_ID or not SECRET_RAW:
        return ""
    ts = str(int(time.time()))
    hashed = hashlib.sha256(SECRET_RAW.encode()).hexdigest()
    sig = _hmac.new(hashed.encode(), f"{CLIENT_ID}:{ts}".encode(), hashlib.sha256).hexdigest()
    from urllib.parse import urlencode
    return urlencode({
        "X-Client-ID": CLIENT_ID,
        "X-Timestamp": ts,
        "Authorization": f"Bearer {sig}",
    })


# ─── V2 provider resolution ─────────────────────────────────────────────

def resolve_v2_provider(event_id: str, max_wait: int = 1800, interval: int = 30):
    deadline = time.time() + max(max_wait, 0)
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.get(f"{V2_API}/fixtures/{event_id}/providers",
                             params={"sport": "football"}, timeout=15)
            if r.status_code == 200:
                bg = r.json().get("providers", {}).get("bet_genius", {})
                fid = bg.get("fixture_id")
                if fid and str(fid) != str(event_id):
                    print(f"  [v2] resolved event_id={event_id} → fixture_id={fid}")
                    return bg
                msg = "echoed event_id or missing fixture_id"
            else:
                msg = f"HTTP {r.status_code}: {r.text[:100]}"
        except Exception as e:
            msg = str(e)

        if time.time() >= deadline:
            print(f"  [v2] gave up resolving event_id={event_id} after {attempt} attempts")
            return None
        print(f"  [v2] {event_id}: {msg} — retry in {interval}s (attempt {attempt})")
        time.sleep(interval)


# ─── V1 capture (async websockets) ──────────────────────────────────────

async def v1_capture(fixture_id: str, out_path: Path, stop: asyncio.Event):
    if not CLIENT_ID or not SECRET_RAW:
        print("  [v1] no credentials — skipping")
        return
    count = 0
    backoff = 2.0
    with out_path.open("w") as f:
        while not stop.is_set():
            try:
                qs = v1_auth_qs()
                uri = f"{V1_WS}?{qs}"
                async with websockets.connect(uri, ping_interval=20, ping_timeout=20,
                                              max_size=10*1024*1024) as ws:
                    await ws.send(json.dumps({"type": "connection_init", "payload": {}}))
                    await ws.send(json.dumps({
                        "id": "v1_sub",
                        "type": "subscribe",
                        "payload": {
                            "operationName": "sportsMatchStateUpdatedV2",
                            "query": ("subscription sportsMatchStateUpdatedV2($fixtureIds: [String!]!)"
                                      " { sportsMatchStateUpdatedV2(fixtureIds: $fixtureIds) }"),
                            "variables": {"fixtureIds": [fixture_id]},
                        },
                    }))
                    print(f"  [v1] subscribed to {fixture_id}")
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
                print(f"  [v1] {type(e).__name__}: {e} — reconnecting in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
    print(f"  [v1] captured {count} frames")


# ─── V2 capture (python-socketio, runs in a thread) ─────────────────────

def v2_capture_sync(provider: dict, out_path: Path, stop_flag: list):
    fixture_id = provider["fixture_id"]
    sio = socketio.Client(reconnection=True, reconnection_attempts=0,
                          logger=False, engineio_logger=False)
    count = 0
    f = out_path.open("w")

    @sio.event
    def connect():
        params = {
            "fixtureId": str(fixture_id),
            "activeContent": "court",
            "sport": provider.get("sport"),
            "sportId": provider.get("sport_id"),
            "competitionId": provider.get("competition_id"),
        }
        sio.emit("subscribe", params)
        print(f"  [v2] subscribed to fixture_id={fixture_id}")

    @sio.on("subscribed")
    def on_sub(data):
        nonlocal count
        ts_ns = time.time_ns()
        f.write(json.dumps({"ts_ns": ts_ns, "source": "v2", "event": "subscribed", "data": data}) + "\n")
        f.flush()
        count += 1
        d = data.get("data", {}) if isinstance(data, dict) else {}
        sb = d.get("scoreboardInfo", {})
        if sb:
            print(f"  [v2] initial: {sb.get('homeScore')}-{sb.get('awayScore')} phase={sb.get('currentPhase')}")

    @sio.on("genius_update")
    def on_update(data):
        nonlocal count
        ts_ns = time.time_ns()
        f.write(json.dumps({"ts_ns": ts_ns, "source": "v2", "event": "genius_update", "data": data}) + "\n")
        count += 1
        if count % 50 == 0:
            f.flush()

    @sio.on("error")
    def on_error(data):
        print(f"  [v2] error: {data}")

    try:
        sio.connect(V2_BASE, socketio_path=V2_SIO_PATH, transports=["websocket"])
        while not stop_flag[0]:
            sio.sleep(1)
    except Exception as e:
        print(f"  [v2] {type(e).__name__}: {e}")
    finally:
        try:
            sio.emit("unsubscribe", {"fixtureId": str(fixture_id), "activeContent": "court"})
            sio.disconnect()
        except Exception:
            pass
        f.flush()
        f.close()
    print(f"  [v2] captured {count} events")


# ─── BoltOdds capture (async websockets, single shared connection) ────────

async def boltodds_capture_multi(
    game_entries: list[tuple[str, Path]],
    stop: asyncio.Event,
):
    """Single BoltOdds WS connection subscribing to all games at once.

    game_entries: list of (game_label, out_path) tuples.
    All games share one connection; frames are routed to per-game files.
    """
    if not BOLTODDS_API_KEY:
        print("  [boltodds] no BOLTODDS_API_KEY — skipping")
        return
    if not game_entries:
        return

    all_labels = [label for label, _ in game_entries]
    # Open all output files
    files: dict[str, Any] = {}
    for label, out_path in game_entries:
        files[label] = out_path.open("w")

    # Shared output file for unrouted frames
    count = 0
    backoff = 2.0
    uri = f"{BOLTODDS_SCORES_WS}?key={BOLTODDS_API_KEY}"

    try:
        while not stop.is_set():
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=20,
                                              max_size=10*1024*1024) as ws:
                    # Wait for connection ack
                    try:
                        ack = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        ts_ns = time.time_ns()
                        try:
                            frame = json.loads(ack)
                        except Exception:
                            frame = ack
                        # Write ack to first file
                        first_f = next(iter(files.values()))
                        first_f.write(json.dumps({"ts_ns": ts_ns, "source": "boltodds", "frame": frame}) + "\n")
                        first_f.flush()
                        count += 1
                    except asyncio.TimeoutError:
                        pass

                    # Subscribe to ALL games in one payload
                    sub_payload = {
                        "action": "subscribe",
                        "filters": {"games": all_labels},
                    }
                    await ws.send(json.dumps(sub_payload))
                    print(f"  [boltodds] subscribed to {len(all_labels)} games on single connection")
                    backoff = 2.0

                    async for raw in ws:
                        if stop.is_set():
                            break
                        ts_ns = time.time_ns()
                        try:
                            frame = json.loads(raw)
                        except Exception:
                            frame = raw

                        # Route frame to the right game file based on content
                        target_f = _route_boltodds_frame(frame, game_entries, files)
                        if target_f:
                            target_f.write(json.dumps({"ts_ns": ts_ns, "source": "boltodds", "frame": frame}) + "\n")
                            target_f.flush()
                        else:
                            # Write to first file as fallback
                            first_f = next(iter(files.values()))
                            first_f.write(json.dumps({"ts_ns": ts_ns, "source": "boltodds", "frame": frame}) + "\n")
                            first_f.flush()
                        count += 1
            except Exception as e:
                if stop.is_set():
                    break
                print(f"  [boltodds] {type(e).__name__}: {e} — reconnecting in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
    finally:
        for f in files.values():
            try:
                f.flush()
                f.close()
            except Exception:
                pass
    print(f"  [boltodds] captured {count} frames total")


def _route_boltodds_frame(frame, game_entries, files):
    """Try to route a BoltOdds frame to the correct game file."""
    if not isinstance(frame, dict):
        return None
    # BoltOdds frames typically contain a "game" or "name" field
    game_name = str(
        frame.get("game") or frame.get("name") or frame.get("match") or ""
    ).strip().lower()
    if not game_name:
        # Check nested data
        data = frame.get("data") if isinstance(frame.get("data"), dict) else {}
        game_name = str(data.get("game") or data.get("name") or "").strip().lower()
    if not game_name:
        return None
    # Match against labels
    for label, _ in game_entries:
        if label.lower().split(",")[0].strip() in game_name or game_name in label.lower():
            return files.get(label)
    return None


# Legacy single-game BoltOdds (for single-game mode)
async def boltodds_capture(game_label: str, out_path: Path, stop: asyncio.Event):
    await boltodds_capture_multi([(game_label, out_path)], stop)


# ─── Main ────────────────────────────────────────────────────────────────

def _load_games_file(path: str) -> list[dict]:
    with open(path) as f:
        games = json.load(f)
    if not isinstance(games, list):
        raise ValueError("games.json must be a JSON array")
    return games


def _run_single_game(
    *,
    v1_fixture_id: str,
    v2_event_id: str,
    boltodds_game_label: str,
    out_dir: Path,
    duration: int,
    resolve_timeout: int,
    stop_event: asyncio.Event,
    stop_flag: list,
):
    """Capture a single game from all available sources."""
    import threading

    v2_thread = None
    if v2_event_id:
        def _v2_resolve_and_capture():
            provider = resolve_v2_provider(v2_event_id, max_wait=resolve_timeout)
            if not provider:
                print(f"  [v2] resolution failed for {v2_event_id}")
                return
            if stop_flag[0]:
                return
            v2_capture_sync(provider, out_dir / "v2_raw.jsonl", stop_flag)

        v2_thread = threading.Thread(target=_v2_resolve_and_capture, daemon=True)
        v2_thread.start()

    async def run():
        tasks = []
        if v1_fixture_id:
            tasks.append(asyncio.create_task(
                v1_capture(v1_fixture_id, out_dir / "v1_raw.jsonl", stop_event)))
        if boltodds_game_label:
            tasks.append(asyncio.create_task(
                boltodds_capture(boltodds_game_label, out_dir / "boltodds_raw.jsonl", stop_event)))

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=duration)
        except asyncio.TimeoutError:
            pass
        finally:
            stop_event.set()
            stop_flag[0] = True
            for t in tasks:
                t.cancel()
                try:
                    await t
                except Exception:
                    pass

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        pass

    if v2_thread:
        v2_thread.join(timeout=5)


def main():
    ap = argparse.ArgumentParser(description="Triple V1/V2/BoltOdds soccer capture")
    ap.add_argument("--v1-fixture-id", default="",
                    help="V1 fixture UUID (single-game mode)")
    ap.add_argument("--v2-event-id", default="",
                    help="V2 event_id (single-game mode)")
    ap.add_argument("--boltodds-game-label", default="",
                    help="BoltOdds game label (single-game mode)")
    ap.add_argument("--games-file", default="",
                    help="Path to games.json for multi-game capture")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=7200, help="Max seconds (default 7200)")
    ap.add_argument("--resolve-timeout", type=int, default=1800,
                    help="Max seconds for V2 provider resolution")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Signal handling
    stop_event = asyncio.Event()
    stop_flag = [False]

    def handle_sig(*_):
        print("\n[signal] stopping...")
        stop_event.set()
        stop_flag[0] = True

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    # Multi-game mode
    if args.games_file:
        games = _load_games_file(args.games_file)
        print(f"[+] Loaded {len(games)} games from {args.games_file}")

        import threading
        from datetime import datetime
        from zoneinfo import ZoneInfo

        v2_threads: list[threading.Thread] = []
        V2_LEAD_MINUTES = 10  # start resolution this many minutes before kickoff

        def _parse_kickoff_ts(game: dict) -> int | None:
            """Parse kickoff time from game entry. Uses when_raw or start_ts_utc from DB."""
            # Try to parse from kickoff_et field (HH:MM in ET on today's date)
            kickoff_et = str(game.get("kickoff_et") or "").strip()
            if kickoff_et:
                try:
                    et = ZoneInfo("America/New_York")
                    today = datetime.now(et).date()
                    parts = kickoff_et.split(":")
                    h, m = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
                    dt = datetime(today.year, today.month, today.day, h, m, tzinfo=et)
                    return int(dt.timestamp())
                except Exception:
                    pass
            return None

        # Start V2 threads with scheduled resolution
        for game in games:
            name = game.get("name", "unknown")
            sport = game.get("sport", "soccer").strip().lower()
            v2_event_id = str(game.get("v2_event_id") or "").strip()
            game_dir = out_dir / name
            game_dir.mkdir(parents=True, exist_ok=True)

            # V2/BetGenius covers soccer only -- skip for baseball and other sports
            if v2_event_id and sport in ("soccer", "football"):
                kickoff_ts = _parse_kickoff_ts(game)

                def _v2_worker(eid=v2_event_id, gdir=game_dir, gname=name, kts=kickoff_ts):
                    # Wait until V2_LEAD_MINUTES before kickoff
                    if kts is not None:
                        start_resolve_at = kts - (V2_LEAD_MINUTES * 60)
                        wait_seconds = start_resolve_at - time.time()
                        if wait_seconds > 0:
                            print(f"  [v2/{gname}] waiting {wait_seconds:.0f}s until {V2_LEAD_MINUTES}min before kickoff")
                            while wait_seconds > 0 and not stop_flag[0]:
                                time.sleep(min(wait_seconds, 5.0))
                                wait_seconds = start_resolve_at - time.time()
                            if stop_flag[0]:
                                return

                    print(f"  [v2/{gname}] resolving event_id={eid}...")
                    provider = resolve_v2_provider(eid, max_wait=args.resolve_timeout, interval=15)
                    if not provider:
                        print(f"  [v2/{gname}] resolution failed")
                        return
                    if stop_flag[0]:
                        return
                    print(f"  [v2/{gname}] capturing...")
                    v2_capture_sync(provider, gdir / "v2_raw.jsonl", stop_flag)

                t = threading.Thread(target=_v2_worker, daemon=True)
                t.start()
                v2_threads.append(t)

        # V1 + BoltOdds in async loop
        async def run_all():
            tasks = []

            # V1: one connection per game (each has a unique fixture subscription)
            for game in games:
                name = game.get("name", "unknown")
                game_dir = out_dir / name
                game_dir.mkdir(parents=True, exist_ok=True)
                v1_id = str(game.get("v1_fixture_id") or "").strip()
                if v1_id:
                    print(f"  [v1/{name}] subscribing to {v1_id}")
                    tasks.append(asyncio.create_task(
                        v1_capture(v1_id, game_dir / "v1_raw.jsonl", stop_event)))

            # BoltOdds: single shared connection for all games
            bo_entries: list[tuple[str, Path]] = []
            for game in games:
                name = game.get("name", "unknown")
                game_dir = out_dir / name
                bo_label = str(game.get("boltodds_game_label") or "").strip()
                if bo_label:
                    bo_entries.append((bo_label, game_dir / "boltodds_raw.jsonl"))
            if bo_entries:
                print(f"  [boltodds] subscribing to {len(bo_entries)} games (single connection)")
                tasks.append(asyncio.create_task(
                    boltodds_capture_multi(bo_entries, stop_event)))

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=args.duration)
            except asyncio.TimeoutError:
                print(f"\n[+] duration ({args.duration}s) elapsed")
            finally:
                stop_event.set()
                stop_flag[0] = True
                for t in tasks:
                    t.cancel()
                    try:
                        await t
                    except Exception:
                        pass

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(run_all())
        except KeyboardInterrupt:
            pass

        for t in v2_threads:
            t.join(timeout=5)

    # Single-game mode
    elif args.v1_fixture_id or args.v2_event_id or args.boltodds_game_label:
        _run_single_game(
            v1_fixture_id=args.v1_fixture_id,
            v2_event_id=args.v2_event_id,
            boltodds_game_label=args.boltodds_game_label,
            out_dir=out_dir,
            duration=args.duration,
            resolve_timeout=args.resolve_timeout,
            stop_event=stop_event,
            stop_flag=stop_flag,
        )
    else:
        print("ERROR: provide --games-file or at least one of --v1-fixture-id / --v2-event-id / --boltodds-game-label")
        return 1

    # Summary
    print(f"\n[+] Done. Output in {out_dir}/")
    for game_dir in sorted(out_dir.iterdir()):
        if game_dir.is_dir():
            files = sorted(game_dir.glob("*.jsonl"))
            if files:
                print(f"  {game_dir.name}/")
                for p in files:
                    lines = sum(1 for _ in p.open())
                    print(f"    {p.name}: {lines} lines")


if __name__ == "__main__":
    main()
