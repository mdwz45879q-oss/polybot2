#!/usr/bin/env python3
"""
Dual-capture: record V1 score updates AND odds updates for the same fixtures.

Subscribes to both `sportsMatchStateUpdatedV2` (scores) and
`sportsMatchOddsUpdated` (all market odds) on a single GraphQL WS
connection. Writes to separate JSONL files for comparison.

Single-fixture:
    python capture_odds.py --fixture-id abc-123 --out ./captures/test --duration 3600

Multi-game:
    python capture_odds.py --games-file games.json --out ./captures/test --duration 3600

games.json format (only `name` and `v1_fixture_id` are required):
    [{"name": "team_a_vs_team_b", "v1_fixture_id": "abc-123-..."}]

Output:
    {out}/{name}/v1_scores.jsonl
    {out}/{name}/v1_odds.jsonl
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

try:
    import requests as _requests
except ImportError:
    _requests = None

V1_BASE = "https://sportsapi.kalstropservice.com/odds_v1/v1"
V1_WS = "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"

# Keepalive / silent-disconnect detection (V1 docs Section 8.2)
PING_INTERVAL = 15   # send a client ping if idle this long (seconds)
RECV_TIMEOUT  = 45   # declare connection dead after this long with zero frames
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


def v1_auth_headers() -> dict[str, str]:
    """Build auth headers for V1 REST calls."""
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


def fetch_market_catalog(fixture_id: str, out_dir: Path, game_name: str) -> dict[str, dict]:
    """Fetch market details for all available groups and build a market ID → info mapping.

    Saves the raw catalog to {out_dir}/{game_name}/v1_market_catalog.json.
    Returns {market_uuid: {"name": ..., "selections": [...], "group": ...}}.
    """
    if _requests is None:
        print(f"  [catalog/{game_name}] requests not installed — skipping")
        return {}

    # Step 1: Get available market groups via SSR endpoint
    # We need the fixture slug, but we only have the UUID. Try fetching details
    # with common market groups directly.
    groups_to_try = [
        "TOP_MARKETS", "WIN_MARKETS", "HANDICAP_MARKETS", "TOTAL_MARKETS",
        "SET_MARKETS", "GAMES_MARKETS", "OTHER_MARKETS",
    ]

    catalog: dict[str, dict] = {}
    raw_markets: list[dict] = []

    for group in groups_to_try:
        try:
            headers = v1_auth_headers()
            url = f"{V1_BASE}/fixture/{fixture_id}/details"
            resp = _requests.get(url, params={"group": group}, headers=headers, timeout=15)
            if resp.status_code != 200:
                continue
            data = resp.json()
            # Response shape: {"top_markets": {"display": [...]}} or similar
            group_key = group.lower()
            group_data = data.get(group_key, {})
            if not isinstance(group_data, dict):
                continue
            display_groups = group_data.get("display", [])
            for dg in display_groups:
                group_name = str(dg.get("groupName") or "").strip()
                # Markets are nested inside display groups
                markets = dg.get("markets", [])
                for m in markets:
                    mid = str(m.get("id") or "").strip()
                    if not mid or mid in catalog:
                        continue
                    mname = str(m.get("name") or "").strip()
                    status = str(m.get("status") or "").strip()
                    selections = []
                    for s in (m.get("selections") or []):
                        selections.append({
                            "id": str(s.get("id") or "").strip(),
                            "name": str(s.get("name") or s.get("fullName") or "").strip(),
                        })
                    entry = {
                        "id": mid,
                        "name": mname or group_name,
                        "group_name": group_name,
                        "group": group,
                        "selections": selections,
                        "status": status,
                    }
                    catalog[mid] = entry
                    raw_markets.append(entry)
                # Collect ALL selections per market from selectionGroups
                # (each group is one side: HOME/AWAY, Over/Under, etc.)
                market_sels: dict[str, list[dict]] = {}  # marketId → [selections]
                for sg in (dg.get("selectionGroups") or []):
                    team = sg.get("team")  # "HOME", "AWAY", or None
                    for s in (sg.get("selections") or []):
                        mid = str(s.get("marketId") or "").strip()
                        if not mid:
                            continue
                        sel_entry = {
                            "id": str(s.get("id") or "").strip(),
                            "name": str(s.get("fullName") or s.get("name") or "").strip(),
                        }
                        if team:
                            sel_entry["team"] = team
                        market_sels.setdefault(mid, []).append(sel_entry)
                for mid, sels in market_sels.items():
                    if mid in catalog:
                        continue
                    entry = {
                        "id": mid,
                        "name": group_name,
                        "group_name": group_name,
                        "group": group,
                        "selections": sels,
                        "status": "",
                    }
                    catalog[mid] = entry
                    raw_markets.append(entry)
        except Exception as e:
            print(f"  [catalog/{game_name}] {group}: {type(e).__name__}: {e}")

    if catalog:
        game_dir = out_dir / game_name
        game_dir.mkdir(parents=True, exist_ok=True)
        catalog_path = game_dir / "v1_market_catalog.json"
        with open(catalog_path, "w") as f:
            json.dump(raw_markets, f, indent=2)
        print(f"  [catalog/{game_name}] {len(catalog)} markets across {len(groups_to_try)} groups → {catalog_path.name}")
    else:
        print(f"  [catalog/{game_name}] no markets found (fixture may be prematch)")

    return catalog


async def _v1_stream(
    label: str,
    sub_id: str,
    operation: str,
    query: str,
    data_key: str,
    fixture_ids: list[str],
    fid_to_name: dict[str, str],
    files: dict[str, any],
    stop: asyncio.Event,
    on_frame=None,
):
    """Generic V1 GraphQL WS subscription loop on its own connection.

    Reconnects independently. Used for both scores and odds streams.
    """
    count = 0
    backoff = 2.0
    while not stop.is_set():
        try:
            qs = v1_auth_qs()
            uri = f"{V1_WS}?{qs}"
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20,
                                          max_size=10*1024*1024) as ws:
                await ws.send(json.dumps({"type": "connection_init", "payload": {}}))
                await ws.send(json.dumps({
                    "id": sub_id,
                    "type": "subscribe",
                    "payload": {
                        "operationName": operation,
                        "query": query,
                        "variables": {"fixtureIds": fixture_ids},
                    },
                }))
                print(f"[{label}] subscribed to {len(fixture_ids)} fixtures")
                backoff = 2.0
                last_activity = time.monotonic()

                try:
                    while not stop.is_set():
                        now = time.monotonic()
                        idle = now - last_activity

                        # Client-initiated keepalive ping (V1 docs Section 8.2)
                        if idle >= PING_INTERVAL:
                            await ws.send(json.dumps({"type": "ping"}))
                            last_activity = now
                            idle = 0.0

                        # Receive with timeout — detect silent disconnects
                        remaining = max(1.0, RECV_TIMEOUT - idle)
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                        except asyncio.TimeoutError:
                            print(f"[{label}] no data for {RECV_TIMEOUT}s — reconnecting")
                            break

                        last_activity = time.monotonic()
                        ts_ns = time.time_ns()
                        try:
                            frame = json.loads(raw)
                        except Exception:
                            continue

                        msg_type = frame.get("type", "")

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                            continue

                        if msg_type == "complete":
                            print(f"[{label}] subscription completed by server — resubscribing")
                            await ws.send(json.dumps({
                                "id": sub_id,
                                "type": "subscribe",
                                "payload": {
                                    "operationName": operation,
                                    "query": query,
                                    "variables": {"fixtureIds": fixture_ids},
                                },
                            }))
                            continue

                        if msg_type != "next":
                            continue

                        update = frame.get("payload", {}).get("data", {}).get(data_key, {})
                        fid = update.get("fixtureId", "")
                        name = fid_to_name.get(fid, fid[:8])
                        f = files.get(name)
                        if f:
                            line = {"ts_ns": ts_ns, "source": f"v1_{label}", "frame": update}
                            # Extract provider timestamps for latency analysis
                            pmt = (update.get("matchSummary") or {}).get("providerMessageTimestamp")
                            if pmt:
                                line["provider_ts"] = pmt
                            mt = update.get("messageTime")
                            pa = update.get("publishedAt")
                            if mt:
                                line["message_time"] = mt
                            if pa:
                                line["published_at"] = pa
                            f.write(json.dumps(line) + "\n")
                            f.flush()
                            count += 1

                        if on_frame:
                            on_frame(name, update, ts_ns)
                finally:
                    # Clean unsubscribe per V1 docs Section 8.2
                    try:
                        await ws.send(json.dumps({"id": sub_id, "type": "complete"}))
                    except Exception:
                        pass  # connection may already be dead

        except Exception as e:
            if stop.is_set():
                break
            print(f"[{label}] {type(e).__name__}: {e} — reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    print(f"[{label}] done — {count} frames")


async def capture(fixture_ids: list[str], game_names: list[str], out_dir: Path,
                  duration: int, stop: asyncio.Event):
    if not CLIENT_ID or not SECRET_RAW:
        print("[!] No credentials — set KALSTROP_CLIENT_ID and KALSTROP_SHARED_SECRET_RAW")
        return

    # Fetch market catalogs (one REST call per group per fixture)
    market_catalogs: dict[str, dict[str, dict]] = {}
    for fid, name in zip(fixture_ids, game_names):
        catalog = fetch_market_catalog(fid, out_dir, name)
        if catalog:
            market_catalogs[name] = catalog

    # Open output files
    score_files: dict[str, any] = {}
    odds_files: dict[str, any] = {}
    fid_to_name: dict[str, str] = {}
    for fid, name in zip(fixture_ids, game_names):
        game_dir = out_dir / name
        game_dir.mkdir(parents=True, exist_ok=True)
        score_files[name] = (game_dir / "v1_scores.jsonl").open("a")
        odds_files[name] = (game_dir / "v1_odds.jsonl").open("a")
        fid_to_name[fid] = name

    # Score summary printer
    def on_score(name, update, ts_ns):
        summary = update.get("matchSummary", {})
        ft = (summary.get("matchStatusDisplay") or [{}])[0].get("freeText", "")
        hs = summary.get("homeScore", "?")
        aws = summary.get("awayScore", "?")
        cp = summary.get("currentPhase") or {}
        rh = cp.get("homeScore", "")
        ra = cp.get("awayScore", "")
        rounds_str = f" rounds={rh}-{ra}" if rh else ""
        print(f"  [score] {name}: maps={hs}-{aws}{rounds_str} ({ft})")

    # Odds summary printer
    def on_odds(name, update, ts_ns):
        markets = update.get("markets", [])
        game_catalog = market_catalogs.get(name, {})
        status_counts: dict[str, int] = {}
        market_names: list[str] = []
        for m in markets:
            s = m.get("status", "?")
            status_counts[s] = status_counts.get(s, 0) + 1
            mid = str(m.get("id") or "")
            cat_entry = game_catalog.get(mid)
            mname = cat_entry["name"] if cat_entry else mid[:8]
            n_sel = len(m.get("selections") or [])
            market_names.append(f"{mname}({s},{n_sel}sel)")
        status_str = ", ".join(f"{c} {s}" for s, c in sorted(status_counts.items()))
        fixture_status = update.get("fixtureStatus", "?")
        markets_detail = " | ".join(market_names[:6])
        if len(market_names) > 6:
            markets_detail += f" +{len(market_names)-6} more"
        print(f"  [odds]  {name}: {fixture_status} {len(markets)}mkts ({status_str})")
        print(f"          {markets_detail}")

    # Run scores and odds on SEPARATE WS connections (independent reconnect)
    scores_task = asyncio.create_task(_v1_stream(
        label="scores",
        sub_id="scores_sub",
        operation="sportsMatchStateUpdatedV2",
        query=("subscription sportsMatchStateUpdatedV2($fixtureIds: [String!]!)"
               " { sportsMatchStateUpdatedV2(fixtureIds: $fixtureIds) }"),
        data_key="sportsMatchStateUpdatedV2",
        fixture_ids=fixture_ids,
        fid_to_name=fid_to_name,
        files=score_files,
        stop=stop,
        on_frame=on_score,
    ))
    odds_task = asyncio.create_task(_v1_stream(
        label="odds",
        sub_id="odds_sub",
        operation="sportsMatchOddsUpdated",
        query=("subscription sportsMatchOddsUpdated($fixtureIds: [String!])"
               " { sportsMatchOddsUpdated(fixtureIds: $fixtureIds) }"),
        data_key="sportsMatchOddsUpdated",
        fixture_ids=fixture_ids,
        fid_to_name=fid_to_name,
        files=odds_files,
        stop=stop,
        on_frame=on_odds,
    ))

    try:
        await asyncio.gather(scores_task, odds_task)
    except asyncio.CancelledError:
        pass
    finally:
        for f in score_files.values():
            f.flush(); f.close()
        for f in odds_files.values():
            f.flush(); f.close()


def main():
    ap = argparse.ArgumentParser(description="Dual V1 score + odds capture")
    ap.add_argument("--fixture-id", default="", help="Single fixture UUID")
    ap.add_argument("--name", default="", help="Game name for output dir (single fixture mode)")
    ap.add_argument("--games-file", default="", help="Path to games.json for multi-game capture")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=7200, help="Max seconds (default 7200)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    stop = asyncio.Event()
    def handle_sig(*_):
        print("\n[signal] stopping...")
        stop.set()
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    # Build fixture list
    fixture_ids: list[str] = []
    game_names: list[str] = []

    if args.games_file:
        with open(args.games_file) as f:
            games = json.load(f)
        for g in games:
            fid = str(g.get("v1_fixture_id") or "").strip()
            name = str(g.get("name") or fid[:12]).strip()
            if fid:
                fixture_ids.append(fid)
                game_names.append(name)
        print(f"[+] Loaded {len(fixture_ids)} fixtures from {args.games_file}")
    elif args.fixture_id:
        fixture_ids.append(args.fixture_id)
        game_names.append(args.name or args.fixture_id[:12])
    else:
        print("ERROR: provide --games-file or --fixture-id")
        return 1

    if not fixture_ids:
        print("ERROR: no fixtures to capture")
        return 1

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
    for game_dir in sorted(out_dir.iterdir()):
        if game_dir.is_dir():
            for p in sorted(game_dir.glob("*.jsonl")):
                lines = sum(1 for _ in p.open())
                print(f"  {game_dir.name}/{p.name}: {lines} lines")


if __name__ == "__main__":
    main()
