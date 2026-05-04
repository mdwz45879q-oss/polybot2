#!/usr/bin/env python3
"""
Standalone Kalstrop V1 capture: record raw GraphQL WS frames to disk.

Usage:
    python capture_kalstrop_v1.py \
        --fixture-id d3f41158-f1cc-41c0-8c5f-3b91c00703e1 \
        --out ./captures/mlb_game

    python capture_kalstrop_v1.py \
        --fixture-ids-file fixtures.txt \
        --out ./captures/session

Output:
    {out}/v1_raw.jsonl  -- raw V1 WS frames with receive timestamps
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

try:
    import websockets
except ImportError:
    print("pip install websockets"); sys.exit(2)


V1_WS = "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"

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


async def v1_capture(fixture_ids: list[str], out_path: Path, stop: asyncio.Event):
    if not CLIENT_ID or not SECRET_RAW:
        print("[v1] ERROR: no credentials (set KALSTROP_CLIENT_ID + KALSTROP_SHARED_SECRET_RAW)")
        return
    count = 0
    backoff = 2.0
    with out_path.open("a") as f:
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
                print(f"[v1] {type(e).__name__}: {e} -- reconnecting in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
    print(f"[v1] captured {count} frames")


def main():
    ap = argparse.ArgumentParser(description="Kalstrop V1 raw frame capture")
    ap.add_argument("--fixture-id", action="append", dest="fixture_ids", default=[],
                    help="V1 fixture UUID (repeatable)")
    ap.add_argument("--fixture-ids-file", type=str, default="",
                    help="Path to a text file with one fixture ID per line")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=7200, help="Max seconds (default 7200)")
    args = ap.parse_args()

    # Collect fixture IDs
    ids = list(args.fixture_ids)
    if args.fixture_ids_file:
        try:
            with open(args.fixture_ids_file, encoding="utf-8") as fh:
                for line in fh:
                    token = line.split("#", 1)[0].strip()
                    if token:
                        ids.append(token)
        except OSError as exc:
            print(f"ERROR: cannot read --fixture-ids-file: {exc}")
            return 1

    # Deduplicate
    seen: set[str] = set()
    deduped: list[str] = []
    for fid in ids:
        fid = fid.strip()
        if fid and fid not in seen:
            seen.add(fid)
            deduped.append(fid)
    ids = deduped

    if not ids:
        print("ERROR: at least one fixture ID required (--fixture-id or --fixture-ids-file)")
        return 1

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Signal handling
    stop_event = asyncio.Event()

    def handle_sig(*_):
        print("\n[signal] stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    print(f"[v1] capturing {len(ids)} fixture(s) to {out_dir}/v1_raw.jsonl")
    for fid in ids:
        print(f"  {fid}")

    async def run():
        task = asyncio.create_task(v1_capture(ids, out_dir / "v1_raw.jsonl", stop_event))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=args.duration)
        except asyncio.TimeoutError:
            print(f"[+] duration ({args.duration}s) elapsed")
        finally:
            stop_event.set()
            task.cancel()
            try:
                await task
            except Exception:
                pass

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(run())
    except KeyboardInterrupt:
        pass

    out_file = out_dir / "v1_raw.jsonl"
    if out_file.exists():
        lines = sum(1 for _ in out_file.open())
        print(f"\n[+] Done. {out_file}: {lines} lines")
    else:
        print("\n[+] Done. No frames captured.")


if __name__ == "__main__":
    main()
