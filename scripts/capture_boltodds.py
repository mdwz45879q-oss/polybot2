#!/usr/bin/env python3
"""
Capture BoltOdds streams for two entries of the same game to check if
they produce different data or are duplicates.

Usage:
    python capture_boltodds_dual.py \
        --label-a "Chicago Cubs vs Arizona Diamondbacks, 2026-05-03, 01" \
        --label-b "Chicago Cubs vs Arizona Diamondbacks, 2026-05-03, 02" \
        --out ./captures/boltodds_dual_test \
        --duration 3600
"""

import argparse
import asyncio
import json
import os
import signal
import time
from pathlib import Path

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise SystemExit(2)

BOLTODDS_SCORES_WS = "wss://spro.agency/api/livescores"
BOLTODDS_API_KEY = os.environ.get("BOLTODDS_API_KEY", "")


async def main():
    ap = argparse.ArgumentParser(description="Dual BoltOdds stream capture")
    ap.add_argument("--label-a", required=True, help="Game label for entry A")
    ap.add_argument("--label-b", required=True, help="Game label for entry B")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=7200, help="Max seconds (default 7200)")
    args = ap.parse_args()

    if not BOLTODDS_API_KEY:
        print("ERROR: BOLTODDS_API_KEY not set")
        return 1

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    label_a = args.label_a
    label_b = args.label_b

    f_a = (out_dir / "entry_a.jsonl").open("w")
    f_b = (out_dir / "entry_b.jsonl").open("w")
    f_shared = (out_dir / "unrouted.jsonl").open("w")

    stop = asyncio.Event()

    def handle_sig(*_):
        print("\n[signal] stopping...")
        stop.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, handle_sig)
    loop.add_signal_handler(signal.SIGTERM, handle_sig)

    uri = f"{BOLTODDS_SCORES_WS}?key={BOLTODDS_API_KEY}"
    count_a = 0
    count_b = 0
    count_shared = 0
    backoff = 2.0

    print(f"  [boltodds] subscribing to:")
    print(f"    A: {label_a}")
    print(f"    B: {label_b}")

    while not stop.is_set():
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20,
                                          max_size=10*1024*1024) as ws:
                # Wait for connection ack
                try:
                    ack = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    ts_ns = time.time_ns()
                    f_shared.write(json.dumps({"ts_ns": ts_ns, "event": "ack", "frame": json.loads(ack)}) + "\n")
                    f_shared.flush()
                except asyncio.TimeoutError:
                    pass

                # Subscribe to both game labels
                sub_payload = {
                    "action": "subscribe",
                    "filters": {"games": [label_a, label_b]},
                }
                await ws.send(json.dumps(sub_payload))
                print(f"  [boltodds] subscribed to both entries")
                backoff = 2.0

                async for raw in ws:
                    if stop.is_set():
                        break
                    ts_ns = time.time_ns()
                    try:
                        frame = json.loads(raw)
                    except Exception:
                        frame = raw

                    # Route: check if frame contains the game label
                    frame_str = json.dumps(frame) if isinstance(frame, dict) else str(frame)
                    routed = False

                    if label_a in frame_str:
                        f_a.write(json.dumps({"ts_ns": ts_ns, "frame": frame}) + "\n")
                        count_a += 1
                        routed = True
                        if count_a % 20 == 0:
                            f_a.flush()

                    if label_b in frame_str:
                        f_b.write(json.dumps({"ts_ns": ts_ns, "frame": frame}) + "\n")
                        count_b += 1
                        routed = True
                        if count_b % 20 == 0:
                            f_b.flush()

                    if not routed:
                        f_shared.write(json.dumps({"ts_ns": ts_ns, "frame": frame}) + "\n")
                        count_shared += 1
                        if count_shared % 20 == 0:
                            f_shared.flush()

        except Exception as e:
            if stop.is_set():
                break
            print(f"  [boltodds] {type(e).__name__}: {e} — reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    # Close files
    f_a.flush(); f_a.close()
    f_b.flush(); f_b.close()
    f_shared.flush(); f_shared.close()

    print(f"\n[+] Done. Output in {out_dir}/")
    print(f"    entry_a.jsonl: {count_a} frames (label ending '01')")
    print(f"    entry_b.jsonl: {count_b} frames (label ending '02')")
    print(f"    unrouted.jsonl: {count_shared} frames")

    # Quick comparison
    if count_a == 0 and count_b == 0:
        print("\n[!] No frames received for either entry. Game may not be live yet.")
    elif count_a > 0 and count_b == 0:
        print("\n[!] Only entry A received frames. Entry B may be invalid/inactive.")
    elif count_a == 0 and count_b > 0:
        print("\n[!] Only entry B received frames. Entry A may be invalid/inactive.")
    else:
        print(f"\n[!] Both entries received frames. Compare the files to check for differences.")


if __name__ == "__main__":
    asyncio.run(main())
