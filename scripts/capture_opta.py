#!/usr/bin/env python3
"""
Capture raw Opta live score frames via Socket.IO.

Usage:
    python capture_opta.py \
        --opta-event-id 2:7799988 \
        --out ./captures/opta_test \
        --duration 7200

    # Baseball:
    python capture_opta.py \
        --opta-event-id 19408196 \
        --sport baseball \
        --out ./captures/opta_baseball

Output:
    {out}/opta_raw.jsonl  — raw opta_message frames with receive timestamps

Resolution flow:
    1. GET /api/v2/opta/fixtures/{event_id}/providers?sport={sport}
       → providers.opta.running_ball.fixture_id
    2. GET /api/v2/opta/fixtures/{fixture_id}/match
       → matchInfo.contestant[].{id, position} for home/away mapping
    3. Socket.IO connect with ?product=opta-stats
    4. opta_subscribe {fixtureId, room: "stats"}
    5. Listen on opta_message events
"""

import argparse
import hashlib
import hmac as _hmac
import json
import os
import signal
import sys
import time
from pathlib import Path
from urllib.parse import quote as url_quote

import requests

try:
    import socketio
except ImportError:
    print("pip install 'python-socketio[client]'"); sys.exit(2)


# Kalstrop endpoints
OPTA_BASE = "https://stats.kalstropservice.com"
OPTA_API = f"{OPTA_BASE}/api/v2/opta"
SIO_PATH = "/socket.io"

# Credentials from env
CLIENT_ID = os.environ.get("KALSTROP_CLIENT_ID") or os.environ.get("CLIENT_ID", "")
SECRET_RAW = os.environ.get("KALSTROP_SHARED_SECRET_RAW") or os.environ.get("SHARED_SECRET_RAW", "")


def auth_headers():
    if not CLIENT_ID or not SECRET_RAW:
        return {}
    ts = str(int(time.time()))
    hashed = hashlib.sha256(SECRET_RAW.encode()).hexdigest()
    sig = _hmac.new(hashed.encode(), f"{CLIENT_ID}:{ts}".encode(), hashlib.sha256).hexdigest()
    return {
        "X-Client-ID": CLIENT_ID,
        "X-Timestamp": ts,
        "Authorization": f"Bearer {sig}",
    }


def resolve_opta_provider(event_id: str, sport: str = "football",
                           max_wait: int = 1800, interval: int = 30):
    """Resolve event_id → running_ball fixture_id via /providers."""
    encoded_eid = url_quote(str(event_id), safe="")
    deadline = time.time() + max(max_wait, 0)
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.get(
                f"{OPTA_API}/fixtures/{encoded_eid}/providers",
                params={"sport": sport},
                headers=auth_headers(),
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                opta = data.get("providers", {}).get("opta", {})
                rb = opta.get("running_ball", {})
                fid = rb.get("fixture_id") if isinstance(rb, dict) else None
                if fid:
                    print(f"  [opta] resolved event_id={event_id} → fixture_id={fid}")
                    return {"fixture_id": str(fid), "betradar": opta.get("betradar", {})}
                msg = "missing running_ball.fixture_id"
            else:
                msg = f"HTTP {r.status_code}: {r.text[:100]}"
        except Exception as e:
            msg = str(e)

        if time.time() >= deadline:
            print(f"  [opta] gave up resolving event_id={event_id} after {attempt} attempts")
            return None
        print(f"  [opta] {event_id}: {msg} — retry in {interval}s (attempt {attempt})")
        time.sleep(interval)


def resolve_opta_contestants(fixture_id: str) -> dict[str, str]:
    """Resolve contestantId → home/away via /match endpoint."""
    try:
        r = requests.get(
            f"{OPTA_API}/fixtures/{fixture_id}/match",
            headers=auth_headers(),
            timeout=15,
        )
        if r.status_code != 200:
            print(f"  [opta] /match HTTP {r.status_code} for fixture_id={fixture_id}")
            return {}
        data = r.json()
        match_info = data.get("matchInfo", {})
        contestants = match_info.get("contestant", [])
        mapping: dict[str, str] = {}
        for c in contestants:
            cid = str(c.get("id") or "").strip()
            pos = str(c.get("position") or "").strip().lower()
            name = str(c.get("name") or "").strip()
            if cid and pos in ("home", "away"):
                mapping[cid] = pos
                print(f"  [opta] contestant {cid} = {pos} ({name})")
        return mapping
    except Exception as e:
        print(f"  [opta] /match error for fixture_id={fixture_id}: {e}")
        return {}


def opta_capture_sync(fixture_id: str, contestant_map: dict[str, str],
                       out_path: Path, stop_flag: list):
    """Socket.IO capture for Opta stats room."""
    sio = socketio.Client(reconnection=True, reconnection_attempts=0,
                          logger=False, engineio_logger=False)
    count = 0
    f = out_path.open("a")

    @sio.event
    def connect():
        sio.emit("opta_subscribe", {"fixtureId": fixture_id, "room": "stats"})
        print(f"  [opta] subscribed to fixture_id={fixture_id} room=stats")

    @sio.on("opta_message")
    def on_opta_message(data):
        nonlocal count
        ts_ns = time.time_ns()
        f.write(json.dumps({"ts_ns": ts_ns, "source": "opta", "event": "opta_message", "data": data}) + "\n")
        count += 1
        if count % 50 == 0:
            f.flush()
        # Print score updates
        try:
            scores = data.get("content", {}).get("liveData", {}).get("matchDetails", {}).get("stats", {}).get("score", [])
            if scores:
                parts = []
                for s in scores:
                    cid = str(s.get("contestantId", ""))
                    role = contestant_map.get(cid, "?")
                    parts.append(f"{role}={s.get('value', '?')}")
                print(f"  [opta] score: {', '.join(parts)} (frame #{count})")
        except Exception:
            pass

    @sio.on("error")
    def on_error(data):
        print(f"  [opta] error: {data}")

    @sio.on("connect_error")
    def on_connect_error(data):
        print(f"  [opta] connect_error: {data}")

    try:
        from urllib.parse import urlencode as _ue
        _sio_qs = _ue({"product": "opta-stats", **auth_headers()})
        sio.connect(f"{OPTA_BASE}?{_sio_qs}", socketio_path=SIO_PATH, transports=["websocket"])
        while not stop_flag[0]:
            sio.sleep(1)
    except Exception as e:
        print(f"  [opta] {type(e).__name__}: {e}")
    finally:
        try:
            sio.emit("opta_unsubscribe", {"fixtureId": fixture_id, "room": "stats"})
            sio.disconnect()
        except Exception:
            pass
        f.flush()
        f.close()
    print(f"  [opta] captured {count} events")


def main():
    ap = argparse.ArgumentParser(description="Opta live score capture")
    ap.add_argument("--opta-event-id", required=True,
                    help="Opta event_id from catalog (e.g., 2:7799988)")
    ap.add_argument("--sport", default="football",
                    help="Sport slug (default: football)")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=7200, help="Max seconds (default 7200)")
    ap.add_argument("--resolve-timeout", type=int, default=1800,
                    help="Max seconds for provider resolution (default 1800)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    stop_flag = [False]

    def handle_sig(*_):
        print("\n[signal] stopping...")
        stop_flag[0] = True

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    # Step 1: resolve fixture_id
    print(f"  [opta] resolving event_id={args.opta_event_id} (sport={args.sport})...")
    provider = resolve_opta_provider(args.opta_event_id, sport=args.sport,
                                      max_wait=args.resolve_timeout)
    if not provider:
        print("[!] Opta provider resolution failed.")
        return 1
    if stop_flag[0]:
        return 0

    fixture_id = provider["fixture_id"]

    # Step 2: resolve contestant → home/away mapping
    print(f"  [opta] resolving contestants for fixture_id={fixture_id}...")
    contestant_map = resolve_opta_contestants(fixture_id)

    if stop_flag[0]:
        return 0

    # Step 3: capture
    print(f"  [opta] starting capture (duration={args.duration}s)...")
    import threading

    def _timeout():
        time.sleep(args.duration)
        if not stop_flag[0]:
            print(f"\n[+] duration ({args.duration}s) elapsed")
            stop_flag[0] = True

    timer = threading.Thread(target=_timeout, daemon=True)
    timer.start()

    opta_capture_sync(fixture_id, contestant_map, out_dir / "opta_raw.jsonl", stop_flag)

    # Summary
    print(f"\n[+] Done. Output in {out_dir}/")
    for p in sorted(out_dir.glob("*.jsonl")):
        lines = sum(1 for _ in p.open())
        print(f"    {p.name}: {lines} lines")


if __name__ == "__main__":
    sys.exit(main() or 0)
