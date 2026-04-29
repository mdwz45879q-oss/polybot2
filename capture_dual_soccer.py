#!/usr/bin/env python3
"""
Dual-capture: record raw messages from both Kalstrop V1 (GraphQL WS) and
V2 (Socket.IO) for a single soccer fixture side-by-side.

Usage:
    # Both V1 and V2:
    python capture_dual_soccer.py \
        --v1-fixture-id d3f41158-f1cc-41c0-8c5f-3b91c00703e1 \
        --v2-event-id 7475980 \
        --out ./captures/atletico_arsenal \
        --duration 7200

    # V1 only:
    python capture_dual_soccer.py \
        --v1-fixture-id d3f41158-f1cc-41c0-8c5f-3b91c00703e1 \
        --out ./captures/atletico_arsenal

    # V2 only:
    python capture_dual_soccer.py \
        --v2-event-id 7475980 \
        --out ./captures/atletico_arsenal

Output:
    {out}/v1_raw.jsonl   — raw V1 WS frames with receive timestamps
    {out}/v2_raw.jsonl   — raw V2 Socket.IO events with receive timestamps
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


# Kalstrop endpoints
V1_WS = "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"
V2_BASE = "https://stats.kalstropservice.com"
V2_API = f"{V2_BASE}/api/v2"
V2_SIO_PATH = "/socket.io"

# Credentials from env
CLIENT_ID = os.environ.get("KALSTROP_CLIENT_ID") or os.environ.get("CLIENT_ID", "")
SECRET_RAW = os.environ.get("KALSTROP_SHARED_SECRET_RAW") or os.environ.get("SHARED_SECRET_RAW", "")


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


def resolve_v2_provider(event_id: str, max_wait: int = 1800, interval: int = 30):
    """Step 4: resolve event_id → BetGenius fixture_id. Retries on 502."""
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


# ─── V1 capture (async websockets) ───────────────────────────────────────

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
                        ts = time.time()
                        try:
                            frame = json.loads(raw)
                        except Exception:
                            frame = raw
                        f.write(json.dumps({"ts": ts, "source": "v1", "frame": frame}) + "\n")
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


# ─── V2 capture (python-socketio, runs in a thread) ──────────────────────

def v2_capture_sync(provider: dict, out_path: Path, stop_flag: list):
    """Blocking Socket.IO capture using the BetGenius fixture_id from Step 4."""
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
        ts = time.time()
        f.write(json.dumps({"ts": ts, "source": "v2", "event": "subscribed", "data": data}) + "\n")
        f.flush()
        count += 1
        d = data.get("data", {}) if isinstance(data, dict) else {}
        sb = d.get("scoreboardInfo", {})
        if sb:
            print(f"  [v2] initial: {sb.get('homeScore')}-{sb.get('awayScore')} phase={sb.get('currentPhase')}")

    @sio.on("genius_update")
    def on_update(data):
        nonlocal count
        ts = time.time()
        f.write(json.dumps({"ts": ts, "source": "v2", "event": "genius_update", "data": data}) + "\n")
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


# ─── Main ────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Dual V1/V2 Kalstrop soccer capture")
    ap.add_argument("--v1-fixture-id", default="",
                    help="V1 fixture UUID (from live endpoint)")
    ap.add_argument("--v2-event-id", default="",
                    help="V2 event_id (from competitions/fixtures endpoint). "
                         "Resolved to BetGenius fixture_id via /providers.")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=7200, help="Max seconds (default 7200)")
    ap.add_argument("--resolve-timeout", type=int, default=1800,
                    help="Max seconds to wait for V2 provider resolution (default 1800)")
    args = ap.parse_args()

    if not args.v1_fixture_id and not args.v2_event_id:
        print("ERROR: at least one of --v1-fixture-id or --v2-event-id is required")
        return 1

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

    # Start V2 in background thread (resolve + capture, non-blocking)
    import threading
    v2_thread = None
    if args.v2_event_id:
        def _v2_resolve_and_capture():
            print(f"  [v2] resolving event_id={args.v2_event_id} in background...")
            provider = resolve_v2_provider(args.v2_event_id, max_wait=args.resolve_timeout)
            if not provider:
                print("[!] V2 provider resolution failed. V2 capture skipped.")
                return
            if stop_flag[0]:
                return
            v2_capture_sync(provider, out_dir / "v2_raw.jsonl", stop_flag)

        v2_thread = threading.Thread(target=_v2_resolve_and_capture, daemon=True)
        v2_thread.start()

    # V1 capture in async loop (starts immediately)
    async def run():
        tasks = []
        if args.v1_fixture_id:
            print(f"  [v1] starting capture for {args.v1_fixture_id}")
            tasks.append(asyncio.create_task(
                v1_capture(args.v1_fixture_id, out_dir / "v1_raw.jsonl", stop_event)))

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=args.duration)
        except asyncio.TimeoutError:
            print(f"[+] duration ({args.duration}s) elapsed")
        finally:
            stop_event.set()
            stop_flag[0] = True
            for t in tasks:
                t.cancel()
                try: await t
                except Exception: pass

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        pass

    if v2_thread:
        v2_thread.join(timeout=5)

    print(f"\n[+] Done. Output in {out_dir}/")
    for p in sorted(out_dir.glob("*.jsonl")):
        lines = sum(1 for _ in p.open())
        print(f"    {p.name}: {lines} lines")


if __name__ == "__main__":
    main()
