#!/usr/bin/env python3
"""
Multi-source capture: LiveStats Socket.IO + V1 GraphQL WS + BoltOdds WS.

Records raw frames from all three providers to JSONL for latency comparison
and data quality analysis.

Usage:
    python scripts/capture_livestats.py \
        --games-file captures/mlb/games.json \
        --out ./captures/mlb \
        --duration 14400 \
        --endpoints match_info,match_timeline,match_timelinedelta

Prerequisites:
    pip install 'python-socketio[asyncio_client]' websockets
    export KALSTROP_CLIENT_ID=...
    export KALSTROP_SHARED_SECRET_RAW=...

    games.json entries should have:
      - livestats_match_id: for LiveStats WS (from build_capture_plan.py --resolve-livestats)
      - v1_fixture_id: for V1 GraphQL WS
      - boltodds_game_label: for BoltOdds WS
"""

import argparse
import asyncio
import hashlib
import hmac as _hmac
import json
import os
import re
import signal
import sys
import time
from pathlib import Path


import unicodedata
import urllib.request as _urllib_request

CLIENT_ID = os.environ.get("KALSTROP_CLIENT_ID") or os.environ.get("CLIENT_ID", "")
SECRET_RAW = os.environ.get("KALSTROP_SHARED_SECRET_RAW") or os.environ.get("SHARED_SECRET_RAW", "")
BOLTODDS_API_KEY = os.environ.get("BOLTODDS_API_KEY", "")

LIVESTATS_BASE = "https://sportsapi.kalstropservice.com"
V1_WS = "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"
BOLTODDS_LIVESCORES_WS = "wss://spro.agency/api/livescores"
BOLTODDS_PBP_WS = "wss://spro.agency/api/playbyplay"
BOLTODDS_PBP_ESPORTS_URL = "https://spro.agency/api/playbyplay/esports"

DEFAULT_ENDPOINTS = ["match_info", "match_timeline", "match_timelinedelta"]
SUBSCRIBE_LEAD_SECONDS = 120  # subscribe 2 min before kickoff
SUBSCRIPTION_CHECK_INTERVAL = 30  # check for new games every 30s


def sanitize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def v1_auth_qs() -> str:
    ts = str(int(time.time()))
    hashed = hashlib.sha256(SECRET_RAW.encode()).hexdigest()
    sig = _hmac.new(hashed.encode(), f"{CLIENT_ID}:{ts}".encode(), hashlib.sha256).hexdigest()
    from urllib.parse import urlencode
    return urlencode({
        "X-Client-ID": CLIENT_ID,
        "X-Timestamp": ts,
        "Authorization": f"Bearer {sig}",
    })


def livestats_auth_qs() -> str:
    ts = str(int(time.time()))
    hashed = hashlib.sha256(SECRET_RAW.encode()).hexdigest()
    sig = _hmac.new(hashed.encode(), f"{CLIENT_ID}:{ts}".encode(), hashlib.sha256).hexdigest()
    from urllib.parse import urlencode
    return urlencode({
        "X-Client-ID": CLIENT_ID,
        "X-Timestamp": ts,
        "Authorization": sig,  # no Bearer prefix for LiveStats
    })


# ---------------------------------------------------------------------------
# LiveStats Socket.IO capture
# ---------------------------------------------------------------------------

async def livestats_capture(
    games: list[dict],
    endpoints: list[str],
    out_dir: Path,
    stop: asyncio.Event,
):
    try:
        import socketio
    except ImportError:
        print("[livestats] ERROR: pip install 'python-socketio[asyncio_client]'")
        return

    # Build queryUrl → (game_name, endpoint_key) routing map and file handles.
    # radar_update frames carry queryUrl like "match_info/63299767".
    query_to_route: dict[str, tuple[str, str]] = {}  # "match_info/12345" → (game_name, endpoint_key)
    file_handles: dict[str, any] = {}                 # "game_name:endpoint_key" → file handle
    subs: list[dict] = []

    for game in games:
        mid = str(game.get("livestats_match_id") or "").strip()
        if not mid:
            continue
        game_name = game.get("name", sanitize_name(mid))
        game_dir = out_dir / game_name
        game_dir.mkdir(parents=True, exist_ok=True)

        for ep in endpoints:
            query_url = f"{ep}/{mid}"
            file_key = f"{game_name}:{ep}"
            query_to_route[query_url] = (game_name, ep)
            fpath = game_dir / f"livestats_{ep}.jsonl"
            file_handles[file_key] = fpath.open("a")
            subs.append({
                "site": "kalstrop",
                "endpoint_key": ep,
                "resource_path": mid,
            })

    if not subs:
        print("[livestats] no games with livestats_match_id, skipping")
        return

    print(f"[livestats] {len(subs)} subscriptions ({len(subs) // len(endpoints)} games × {len(endpoints)} endpoints)")
    total = 0
    backoff = 2.0
    sio = socketio.AsyncClient(reconnection=False)

    @sio.event
    async def connect():
        nonlocal backoff
        backoff = 2.0
        print(f"[livestats] connected, subscribing...")
        for sub in subs:
            await sio.emit("subscribe", sub)
            await asyncio.sleep(0.05)
        print(f"[livestats] {len(subs)} subscriptions sent")

    @sio.on("subscribed")
    async def on_subscribed(data):
        room = data.get("room", "")
        status = data.get("status", "")
        has_initial = "initial_data" in data
        print(f"  [ls:subscribed] {room} status={status} initial={'yes' if has_initial else 'no'}")
        # Record initial snapshot by routing via room format "kalstrop:endpoint:matchid"
        if has_initial:
            ts_ns = time.time_ns()
            parts = room.split(":", 2)
            if len(parts) == 3:
                query_url = f"{parts[1]}/{parts[2]}"
                route = query_to_route.get(query_url)
                if route:
                    file_key = f"{route[0]}:{route[1]}"
                    fh = file_handles.get(file_key)
                    if fh:
                        record = {"ts_ns": ts_ns, "source": "livestats", "event": "initial_snapshot",
                                  "endpoint_key": route[1], "frame": data.get("initial_data")}
                        fh.write(json.dumps(record, separators=(",", ":")) + "\n")
                        fh.flush()

    @sio.on("radar_update")
    async def on_radar_update(data):
        nonlocal total
        ts_ns = time.time_ns()
        total += 1
        # Route by queryUrl field: "match_info/63299767" → (game_name, endpoint_key)
        query_url = data.get("queryUrl", "") if isinstance(data, dict) else ""
        route = query_to_route.get(query_url)
        if route:
            file_key = f"{route[0]}:{route[1]}"
            fh = file_handles.get(file_key)
            if fh:
                record = {"ts_ns": ts_ns, "source": "livestats", "endpoint_key": route[1], "frame": data}
                fh.write(json.dumps(record, separators=(",", ":")) + "\n")
                fh.flush()
                return
        # Unrouted fallback
        catch_all = out_dir / "livestats_unrouted.jsonl"
        with catch_all.open("a") as f:
            f.write(json.dumps({"ts_ns": ts_ns, "source": "livestats", "queryUrl": query_url, "frame": data},
                               separators=(",", ":")) + "\n")

    @sio.on("error")
    async def on_error(data):
        print(f"  [ls:error] {data}")

    @sio.event
    async def disconnect():
        print("[livestats] disconnected")

    while not stop.is_set():
        try:
            url = f"{LIVESTATS_BASE}?{livestats_auth_qs()}"
            await sio.connect(url, socketio_path="/socket.io", transports=["websocket"])
            while sio.connected and not stop.is_set():
                await asyncio.sleep(1)
            if stop.is_set():
                break
        except Exception as e:
            if stop.is_set():
                break
            print(f"[livestats] {type(e).__name__}: {e} -- reconnecting in {backoff:.0f}s")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)

    if sio.connected:
        try:
            await sio.disconnect()
        except Exception:
            pass
    for fh in file_handles.values():
        try:
            fh.close()
        except Exception:
            pass
    print(f"[livestats] captured {total} frames")


# ---------------------------------------------------------------------------
# V1 GraphQL WS capture (same as capture_kalstrop_v1.py)
# ---------------------------------------------------------------------------

async def v1_capture(games: list[dict], out_dir: Path, stop: asyncio.Event):
    try:
        import websockets
    except ImportError:
        print("[v1] ERROR: pip install websockets")
        return

    fixture_ids = [str(g["v1_fixture_id"]) for g in games if g.get("v1_fixture_id")]
    if not fixture_ids:
        print("[v1] no games with v1_fixture_id, skipping")
        return

    # Route by fixture ID → per-game file
    id_to_game: dict[str, str] = {}
    file_handles: dict[str, any] = {}
    for g in games:
        fid = str(g.get("v1_fixture_id") or "").strip()
        if not fid:
            continue
        game_name = g.get("name", sanitize_name(fid))
        game_dir = out_dir / game_name
        game_dir.mkdir(parents=True, exist_ok=True)
        id_to_game[fid] = game_name
        fpath = game_dir / "v1_raw.jsonl"
        file_handles[game_name] = fpath.open("a")

    count = 0
    backoff = 2.0
    print(f"[v1] subscribing to {len(fixture_ids)} fixture(s)")

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
                print(f"[v1] connected, {len(fixture_ids)} fixture(s) subscribed")
                backoff = 2.0
                async for raw in ws:
                    if stop.is_set():
                        break
                    ts_ns = time.time_ns()
                    try:
                        frame = json.loads(raw)
                    except Exception:
                        frame = raw
                    # Route by fixtureId in the frame
                    game_name = None
                    if isinstance(frame, dict) and frame.get("type") == "next":
                        fid = (frame.get("payload", {}).get("data", {})
                               .get("sportsMatchStateUpdatedV2", {}).get("fixtureId", ""))
                        game_name = id_to_game.get(fid)
                    if isinstance(frame, dict) and frame.get("type") == "ping":
                        await ws.send(json.dumps({"type": "pong"}))

                    fh = file_handles.get(game_name) if game_name else None
                    if fh:
                        fh.write(json.dumps({"ts_ns": ts_ns, "source": "v1", "frame": frame},
                                            separators=(",", ":")) + "\n")
                        fh.flush()
                    else:
                        # Write to shared file for unrouted/system frames
                        shared = out_dir / "v1_system.jsonl"
                        with shared.open("a") as f:
                            f.write(json.dumps({"ts_ns": ts_ns, "source": "v1", "frame": frame},
                                               separators=(",", ":")) + "\n")
                    count += 1
        except Exception as e:
            if stop.is_set():
                break
            print(f"[v1] {type(e).__name__}: {e} -- reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    for fh in file_handles.values():
        try:
            fh.close()
        except Exception:
            pass
    print(f"[v1] captured {count} frames")


# ---------------------------------------------------------------------------
# BoltOdds esports label resolution
# ---------------------------------------------------------------------------

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _normalize_team(s: str) -> str:
    s = _strip_accents(s.strip().lower())
    return re.sub(r"[^a-z0-9]", "", s)


def resolve_esports_pbp_labels(game_labels: list[str]) -> dict[str, str]:
    """Fetch PBP esports labels and match to standard labels.

    BoltOdds uses different game labels for esports on /api/get_games vs
    the WS endpoints. The correct labels come from /api/playbyplay/esports.
    Returns dict mapping standard_label → correct_pbp_label.
    """
    url = f"{BOLTODDS_PBP_ESPORTS_URL}?key={BOLTODDS_API_KEY}"
    try:
        with _urllib_request.urlopen(url, timeout=15) as resp:
            pbp_games = json.loads(resp.read())
    except Exception as e:
        print(f"[resolve-esports] failed: {e}")
        return {}

    print(f"[resolve-esports] fetched {len(pbp_games)} PBP esports games")

    def parse_label(label):
        parts = label.rsplit(", ", 2)
        if len(parts) >= 3:
            teams, date_suffix = parts[0], f"{parts[1]}, {parts[2]}"
        elif len(parts) == 2:
            teams, date_suffix = parts[0], parts[1]
        else:
            teams, date_suffix = label, ""
        team_parts = teams.split(" vs ", 1)
        if len(team_parts) == 2:
            return (_normalize_team(team_parts[0]), _normalize_team(team_parts[1]), date_suffix)
        return (_normalize_team(teams), "", date_suffix)

    pbp_index: dict[tuple, str] = {}
    for pbp_label in pbp_games:
        h, a, ds = parse_label(pbp_label)
        pbp_index[(h, a, ds)] = pbp_label
        if a:
            pbp_index[(a, h, ds)] = pbp_label

    resolved: dict[str, str] = {}
    for std_label in game_labels:
        h, a, ds = parse_label(std_label)
        if (h, a, ds) in pbp_index:
            resolved[std_label] = pbp_index[(h, a, ds)]
        else:
            for (ph, pa, _), plabel in pbp_index.items():
                if h == ph and a == pa:
                    resolved[std_label] = plabel
                    break

    n = sum(1 for s, p in resolved.items() if s != p)
    print(f"[resolve-esports] matched {len(resolved)}/{len(game_labels)} ({n} renamed)")
    for std, pbp in resolved.items():
        if std != pbp:
            print(f"  {std}  →  {pbp}")

    return resolved


# ---------------------------------------------------------------------------
# BoltOdds WS capture (dynamic subscription)
# ---------------------------------------------------------------------------

async def boltodds_ws_capture(
    games: list[dict],
    out_dir: Path,
    stop: asyncio.Event,
    *,
    ws_url: str,
    tag: str,
    filename: str,
    label_to_kickoff: dict[str, int | None] | None = None,
):
    """Generic BoltOdds WS capture — works for both livescores and play-by-play.

    Subscribes to games only when they approach kickoff (2 min lead) and
    tracks finished games to avoid resubscribing. This keeps the active
    subscription list small, avoiding Code 1 errors and rate-limit issues.
    """
    try:
        import websockets
    except ImportError:
        print(f"[{tag}] ERROR: pip install websockets")
        return

    if not BOLTODDS_API_KEY:
        print(f"[{tag}] no BOLTODDS_API_KEY set, skipping")
        return

    all_labels = []
    label_to_game: dict[str, str] = {}
    file_handles: dict[str, any] = {}
    for g in games:
        label = str(g.get("boltodds_game_label") or "").strip()
        if not label:
            continue
        game_name = g.get("name", sanitize_name(label))
        all_labels.append(label)
        label_to_game[label] = game_name
        game_dir = out_dir / game_name
        game_dir.mkdir(parents=True, exist_ok=True)
        fpath = game_dir / filename
        file_handles[game_name] = fpath.open("a")

    if not all_labels:
        print(f"[{tag}] no games with boltodds_game_label, skipping")
        return

    count = 0
    backoff = 30.0
    subscribed: set[str] = set()
    finished: set[str] = set()
    _kickoffs = label_to_kickoff or {}

    def _games_to_subscribe_now() -> list[str]:
        now = int(time.time())
        ready = []
        for label in all_labels:
            if label in subscribed or label in finished:
                continue
            kickoff = _kickoffs.get(label)
            if kickoff is None or now >= kickoff - SUBSCRIBE_LEAD_SECONDS:
                ready.append(label)
        return ready

    now_ts = int(time.time())
    n_live = sum(1 for l in all_labels
                 if _kickoffs.get(l) is None or _kickoffs[l] <= now_ts + SUBSCRIBE_LEAD_SECONDS)
    print(f"[{tag}] {len(all_labels)} game(s): {n_live} live/imminent, {len(all_labels) - n_live} upcoming")

    while not stop.is_set():
        try:
            uri = f"{ws_url}?key={BOLTODDS_API_KEY}"
            async with websockets.connect(uri, max_size=None) as ws:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                print(f"[{tag}] connected")
                backoff = 30.0
                subscribed.clear()

                # Initial subscribe — only live/imminent games
                initial = _games_to_subscribe_now()
                if initial:
                    sub = json.dumps({"action": "subscribe", "filters": {"games": initial}})
                    await ws.send(sub)
                    subscribed.update(initial)
                    print(f"[{tag}] subscribed to {len(initial)} game(s)")

                last_sub_check = time.time()

                async for raw in ws:
                    if stop.is_set():
                        break
                    ts_ns = time.time_ns()
                    try:
                        frame = json.loads(raw)
                    except Exception:
                        frame = raw

                    action = frame.get("action", "") if isinstance(frame, dict) else ""

                    # Track finished games
                    if action == "game_removed":
                        gl = frame.get("game") or frame.get("event") or ""
                        if gl:
                            finished.add(gl)
                            print(f"[{tag}] game finished: {gl[:50]}")

                    # Route to per-game file
                    game_name = None
                    if isinstance(frame, dict):
                        gl = frame.get("game") or frame.get("event") or ""
                        game_name = label_to_game.get(gl)

                    fh = file_handles.get(game_name) if game_name else None
                    if fh:
                        fh.write(json.dumps({"ts_ns": ts_ns, "source": tag, "frame": frame},
                                            separators=(",", ":")) + "\n")
                        fh.flush()
                    else:
                        shared = out_dir / f"{tag}_events.jsonl"
                        with shared.open("a") as f:
                            f.write(json.dumps({"ts_ns": ts_ns, "source": tag, "frame": frame},
                                               separators=(",", ":")) + "\n")
                    count += 1

                    # Periodically check for new games to subscribe
                    now = time.time()
                    if now - last_sub_check >= SUBSCRIPTION_CHECK_INTERVAL:
                        last_sub_check = now
                        new_games = _games_to_subscribe_now()
                        if new_games:
                            all_active = [l for l in subscribed | set(new_games) if l not in finished]
                            sub = json.dumps({"action": "subscribe", "filters": {"games": all_active}})
                            await ws.send(sub)
                            subscribed.update(new_games)
                            print(f"[{tag}] added {len(new_games)} game(s), total active: {len(all_active)}")

        except Exception as e:
            if stop.is_set():
                break
            print(f"[{tag}] {type(e).__name__}: {e} -- reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    for fh in file_handles.values():
        try:
            fh.close()
        except Exception:
            pass
    print(f"[{tag}] captured {count} frames")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Multi-source capture: LiveStats + V1 + BoltOdds")
    ap.add_argument("--games-file", required=True, help="Path to games.json")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=14400, help="Max seconds (default 14400 = 4h)")
    ap.add_argument("--endpoints", default=",".join(DEFAULT_ENDPOINTS),
                    help=f"LiveStats endpoint keys (default: {','.join(DEFAULT_ENDPOINTS)})")
    ap.add_argument("--no-livestats", action="store_true", help="Skip LiveStats capture")
    ap.add_argument("--no-v1", action="store_true", help="Skip V1 odds WS capture")
    ap.add_argument("--no-boltodds", action="store_true", help="Skip BoltOdds capture entirely")
    ap.add_argument("--bo-livescores-only", action="store_true",
                    help="BoltOdds: only use livescores endpoint (skip play-by-play)")
    ap.add_argument("--bo-pbp-only", action="store_true",
                    help="BoltOdds: only use play-by-play endpoint (skip livescores)")
    ap.add_argument("--resolve-esports", action="store_true",
                    help="Fetch correct esports labels from /api/playbyplay/esports "
                         "(required for esports — standard labels cause Code 1 errors)")
    args = ap.parse_args()

    with open(args.games_file) as f:
        games = json.load(f)

    if not games:
        print("ERROR: no games in games file")
        return 1

    # Build BoltOdds label → kickoff mapping for dynamic subscription
    bo_label_to_kickoff: dict[str, int | None] = {}
    for g in games:
        label = str(g.get("boltodds_game_label") or "").strip()
        if label:
            kickoff = g.get("start_ts_utc")
            bo_label_to_kickoff[label] = int(kickoff) if kickoff is not None else None

    # Resolve esports labels if requested
    if args.resolve_esports and BOLTODDS_API_KEY:
        bo_labels = [str(g.get("boltodds_game_label") or "").strip()
                     for g in games if g.get("boltodds_game_label")]
        resolved = resolve_esports_pbp_labels(bo_labels)
        if resolved:
            # Replace labels in games list and kickoff map
            new_kickoff: dict[str, int | None] = {}
            for g in games:
                old_label = str(g.get("boltodds_game_label") or "").strip()
                if old_label and old_label in resolved:
                    new_label = resolved[old_label]
                    g["boltodds_game_label"] = new_label
                    new_kickoff[new_label] = bo_label_to_kickoff.get(old_label)
                elif old_label:
                    new_kickoff[old_label] = bo_label_to_kickoff.get(old_label)
            bo_label_to_kickoff = new_kickoff

    endpoints = [e.strip() for e in args.endpoints.split(",") if e.strip()]
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    n_ls = sum(1 for g in games if g.get("livestats_match_id"))
    n_v1 = sum(1 for g in games if g.get("v1_fixture_id"))
    n_bo = sum(1 for g in games if g.get("boltodds_game_label"))
    print(f"[capture] {len(games)} game(s): {n_ls} livestats, {n_v1} v1, {n_bo} boltodds")
    print(f"[capture] endpoints: {endpoints}")
    print(f"[capture] duration: {args.duration}s, output: {out_dir}/")

    stop_event = asyncio.Event()

    def handle_sig(*_):
        print("\n[signal] stopping all captures...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    async def run():
        tasks = []
        if not args.no_livestats and n_ls > 0 and CLIENT_ID and SECRET_RAW:
            tasks.append(asyncio.create_task(
                livestats_capture(games, endpoints, out_dir, stop_event)))
        if not args.no_v1 and n_v1 > 0 and CLIENT_ID and SECRET_RAW:
            tasks.append(asyncio.create_task(
                v1_capture(games, out_dir, stop_event)))
        if not args.no_boltodds and n_bo > 0 and BOLTODDS_API_KEY:
            if not args.bo_pbp_only:
                tasks.append(asyncio.create_task(
                    boltodds_ws_capture(games, out_dir, stop_event,
                                       ws_url=BOLTODDS_LIVESCORES_WS,
                                       tag="bo:livescores",
                                       filename="boltodds_raw.jsonl",
                                       label_to_kickoff=bo_label_to_kickoff)))
            if not args.bo_livescores_only:
                tasks.append(asyncio.create_task(
                    boltodds_ws_capture(games, out_dir, stop_event,
                                       ws_url=BOLTODDS_PBP_WS,
                                       tag="bo:pbp",
                                       filename="boltodds_pbp.jsonl",
                                       label_to_kickoff=bo_label_to_kickoff)))

        if not tasks:
            print("ERROR: no capture tasks started (check credentials and games.json fields)")
            return

        print(f"[capture] started {len(tasks)} capture task(s)")

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

    print(f"\n[+] Done. Output in {out_dir}/")


if __name__ == "__main__":
    sys.exit(main() or 0)
