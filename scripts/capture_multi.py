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
        "v2_category_slug": "international-clubs",
        "v2_tournament_slug": "uefa-champions-league",
        "v2_home_team": "Man Utd",
        "v2_away_team": "Liverpool",
        "v2_scheduled_date": "2026-05-08",
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
      v2_category_slug  -- V2 API category slug for re-fetching fixtures (optional)
      v2_tournament_slug -- V2 API tournament slug (optional)
      v2_home_team      -- V2 home team name for matching after re-fetch (optional)
      v2_away_team      -- V2 away team name for matching (optional)
      v2_scheduled_date -- ISO date "YYYY-MM-DD" for matching (optional)
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
V2_API = f"{V2_BASE}/api/v2/genius"
V2_SIO_PATH = "/socket.io"
OPTA_API = f"{V2_BASE}/api/v2/opta"
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

def v2_auth_headers():
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


def resolve_v2_provider(event_id: str, max_wait: int = 1800, interval: int = 30):
    deadline = time.time() + max(max_wait, 0)
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.get(f"{V2_API}/fixtures/{event_id}/providers",
                             params={"sport": "football"}, headers=v2_auth_headers(), timeout=15)
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


def resolve_v2_live_event_id(
    category_slug: str,
    tournament_slug: str,
    home_team: str,
    away_team: str,
    scheduled_date: str,
    original_event_id: str,
    max_wait: int = 1800,
    interval: int = 30,
) -> str | None:
    """Re-fetch tournament fixtures and find the live event_id by team match.

    V2 event_ids can change when a game transitions from prematch to live.
    This function polls the fixtures endpoint, matching by team names and date
    to discover the current event_id.

    Returns the live event_id (may differ from original_event_id), or None.
    """
    deadline = time.time() + max(max_wait, 0)
    attempt = 0
    home_norm = home_team.strip().lower()
    away_norm = away_team.strip().lower()
    date_norm = scheduled_date.strip()

    while True:
        attempt += 1
        try:
            url = f"{V2_API}/sports/football/competitions/{category_slug}/{tournament_slug}/fixtures"
            r = requests.get(url, headers=v2_auth_headers(), timeout=15)
            if r.status_code == 200:
                data = r.json()
                fixtures = data.get("fixtures", []) if isinstance(data, dict) else []
                for f in fixtures:
                    competitors = f.get("competitors", {})
                    if not isinstance(competitors, dict):
                        continue
                    home = competitors.get("home", {})
                    away = competitors.get("away", {})
                    f_home = str(home.get("name") or "").strip().lower()
                    f_away = str(away.get("name") or "").strip().lower()
                    f_date = str(f.get("scheduled_date") or "").strip()
                    f_eid = str(f.get("event_id") or "").strip()

                    if not f_eid:
                        continue
                    # Match by teams (case-insensitive)
                    if f_home != home_norm or f_away != away_norm:
                        continue
                    # Match by date if provided
                    if date_norm and f_date and f_date != date_norm:
                        continue
                    # Found it
                    return f_eid
                msg = f"game not found in {len(fixtures)} fixtures"
            else:
                msg = f"HTTP {r.status_code}"
        except Exception as e:
            msg = str(e)

        if time.time() >= deadline:
            print(f"  [v2] gave up finding live event_id after {attempt} attempts")
            return None
        print(f"  [v2] {original_event_id}: {msg} — retry in {interval}s (attempt {attempt})")
        time.sleep(interval)


# ─── V1 capture (async websockets) ──────────────────────────────────────

async def v1_capture(fixture_id: str, out_path: Path, stop: asyncio.Event):
    if not CLIENT_ID or not SECRET_RAW:
        print("  [v1] no credentials — skipping")
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
    f = out_path.open("a")

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
        from urllib.parse import urlencode as _ue
        _sio_qs = _ue({"product": "genius-stats", **v2_auth_headers()})
        sio.connect(f"{V2_BASE}?{_sio_qs}", socketio_path=V2_SIO_PATH, transports=["websocket"])
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


# ─── Opta capture (python-socketio, runs in a thread) ──────────────────

def resolve_opta_provider(event_id: str, sport: str = "football",
                           max_wait: int = 1800, interval: int = 30):
    """Resolve Opta event_id → running_ball fixture_id."""
    from urllib.parse import quote as url_quote
    encoded_eid = url_quote(str(event_id), safe="")
    deadline = time.time() + max(max_wait, 0)
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.get(
                f"{OPTA_API}/fixtures/{encoded_eid}/providers",
                params={"sport": sport},
                headers=v2_auth_headers(),
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                opta = data.get("providers", {}).get("opta", {})
                rb = opta.get("running_ball", {})
                fid = rb.get("fixture_id") if isinstance(rb, dict) else None
                if fid:
                    print(f"  [opta] resolved event_id={event_id} → fixture_id={fid}")
                    return {"fixture_id": str(fid)}
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
            headers=v2_auth_headers(),
            timeout=15,
        )
        if r.status_code != 200:
            print(f"  [opta] /match HTTP {r.status_code} for fixture_id={fixture_id}")
            return {}
        data = r.json()
        contestants = data.get("matchInfo", {}).get("contestant", [])
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

    @sio.on("error")
    def on_error(data):
        print(f"  [opta] error: {data}")

    try:
        from urllib.parse import urlencode as _ue
        _sio_qs = _ue({"product": "opta-stats", **v2_auth_headers()})
        sio.connect(f"{V2_BASE}?{_sio_qs}", socketio_path=V2_SIO_PATH, transports=["websocket"])
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


# ─── BoltOdds capture (async websockets, single shared connection) ────────

async def boltodds_capture_multi(
    game_entries: list[tuple[str, Path]],
    stop: asyncio.Event,
):
    """Single BoltOdds WS connection subscribing to all games at once.

    game_entries: list of (game_label, out_path) tuples.
    All games share one connection; frames are routed to per-game files.
    System/event frames (no game field) are written to a shared _events.jsonl file.
    """
    if not BOLTODDS_API_KEY:
        print("  [boltodds] no BOLTODDS_API_KEY — skipping")
        return
    if not game_entries:
        return

    all_labels = [label for label, _ in game_entries]
    # Open per-game output files
    files: dict[str, Any] = {}
    for label, out_path in game_entries:
        files[label] = out_path.open("a")

    # Shared events file for system messages (ack, connected, errors)
    events_dir = game_entries[0][1].parent.parent if game_entries else Path(".")
    events_file = (events_dir / "boltodds_events.jsonl").open("a")

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
                        events_file.write(json.dumps({"ts_ns": ts_ns, "source": "boltodds", "frame": frame}) + "\n")
                        events_file.flush()
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

                        # Detect server-side disconnect message — reconnect immediately
                        if isinstance(frame, dict) and "error" in frame and "connection closed" in str(frame.get("error", "")).lower():
                            events_file.write(json.dumps({"ts_ns": ts_ns, "source": "boltodds", "frame": frame}) + "\n")
                            events_file.flush()
                            print(f"  [boltodds] server disconnect: {frame.get('error', '')[:60]}")
                            break

                        # Route frame to the right game file based on content
                        target_f = _route_boltodds_frame(frame, game_entries, files)
                        if target_f:
                            target_f.write(json.dumps({"ts_ns": ts_ns, "source": "boltodds", "frame": frame}) + "\n")
                            target_f.flush()
                        else:
                            # System/event frame — write to shared events file
                            events_file.write(json.dumps({"ts_ns": ts_ns, "source": "boltodds", "frame": frame}) + "\n")
                            events_file.flush()
                        count += 1

                    # Reconnect after loop exit (server closed or disconnect detected)
                    if not stop.is_set():
                        print(f"  [boltodds] reconnecting in {backoff:.0f}s")
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 1.5, 15)
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
        try:
            events_file.flush()
            events_file.close()
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
    v2_category_slug: str = "",
    v2_tournament_slug: str = "",
    v2_home_team: str = "",
    v2_away_team: str = "",
    v2_scheduled_date: str = "",
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
            resolve_eid = v2_event_id
            if v2_category_slug and v2_tournament_slug and v2_home_team and v2_away_team:
                live_eid = resolve_v2_live_event_id(
                    category_slug=v2_category_slug,
                    tournament_slug=v2_tournament_slug,
                    home_team=v2_home_team,
                    away_team=v2_away_team,
                    scheduled_date=v2_scheduled_date,
                    original_event_id=v2_event_id,
                    max_wait=resolve_timeout,
                )
                if live_eid:
                    if live_eid != v2_event_id:
                        print(f"  [v2] event_id rotated: {v2_event_id} → {live_eid}")
                    resolve_eid = live_eid
                else:
                    print(f"  [v2] could not find live event_id, trying original...")

            provider = resolve_v2_provider(resolve_eid, max_wait=resolve_timeout)
            if not provider:
                print(f"  [v2] resolution failed for {resolve_eid}")
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
    ap.add_argument("--v2-category-slug", type=str, default="",
                    help="V2 category slug for fixture re-fetch (single-game mode)")
    ap.add_argument("--v2-tournament-slug", type=str, default="",
                    help="V2 tournament slug for fixture re-fetch (single-game mode)")
    ap.add_argument("--v2-home-team", type=str, default="",
                    help="V2 home team name for matching (single-game mode)")
    ap.add_argument("--v2-away-team", type=str, default="",
                    help="V2 away team name for matching (single-game mode)")
    ap.add_argument("--v2-scheduled-date", type=str, default="",
                    help="V2 scheduled date YYYY-MM-DD for matching (single-game mode)")
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
        opta_threads: list[threading.Thread] = []
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
            v2_category_slug = str(game.get("v2_category_slug") or "").strip()
            v2_tournament_slug = str(game.get("v2_tournament_slug") or "").strip()
            v2_home_team = str(game.get("v2_home_team") or "").strip()
            v2_away_team = str(game.get("v2_away_team") or "").strip()
            v2_scheduled_date = str(game.get("v2_scheduled_date") or "").strip()
            game_dir = out_dir / name
            game_dir.mkdir(parents=True, exist_ok=True)

            # V2/BetGenius covers soccer only -- skip for baseball and other sports
            if v2_event_id and sport in ("soccer", "football"):
                kickoff_ts = _parse_kickoff_ts(game)

                def _v2_worker(eid=v2_event_id, gdir=game_dir, gname=name, kts=kickoff_ts,
                               cat_slug=v2_category_slug, tourn_slug=v2_tournament_slug,
                               home=v2_home_team, away=v2_away_team, sdate=v2_scheduled_date):
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

                    # Step 1: Re-discover live event_id if metadata provided
                    resolve_eid = eid
                    if cat_slug and tourn_slug and home and away:
                        print(f"  [v2/{gname}] re-fetching fixtures to find live event_id...")
                        live_eid = resolve_v2_live_event_id(
                            category_slug=cat_slug,
                            tournament_slug=tourn_slug,
                            home_team=home,
                            away_team=away,
                            scheduled_date=sdate,
                            original_event_id=eid,
                            max_wait=args.resolve_timeout,
                            interval=15,
                        )
                        if live_eid:
                            if live_eid != eid:
                                print(f"  [v2/{gname}] event_id rotated: {eid} → {live_eid}")
                            resolve_eid = live_eid
                        else:
                            print(f"  [v2/{gname}] could not find live event_id, trying original...")

                    # Step 2: Resolve BetGenius fixture_id (existing logic)
                    print(f"  [v2/{gname}] resolving event_id={resolve_eid}...")
                    provider = resolve_v2_provider(resolve_eid, max_wait=args.resolve_timeout, interval=15)
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

        # Start Opta threads (all sports — not soccer-gated like V2)
        for game in games:
            name = game.get("name", "unknown")
            opta_event_id = str(game.get("opta_event_id") or "").strip()
            opta_sport = str(game.get("opta_sport") or "football").strip().lower()
            game_dir = out_dir / name
            game_dir.mkdir(parents=True, exist_ok=True)

            if opta_event_id:
                kickoff_ts = _parse_kickoff_ts(game)

                def _opta_worker(eid=opta_event_id, sport=opta_sport, gdir=game_dir,
                                  gname=name, kts=kickoff_ts):
                    # Wait until lead time before kickoff
                    if kts is not None:
                        start_resolve_at = kts - (V2_LEAD_MINUTES * 60)
                        wait_seconds = start_resolve_at - time.time()
                        if wait_seconds > 0:
                            print(f"  [opta/{gname}] waiting {wait_seconds:.0f}s until {V2_LEAD_MINUTES}min before kickoff")
                            while wait_seconds > 0 and not stop_flag[0]:
                                time.sleep(min(wait_seconds, 5.0))
                                wait_seconds = start_resolve_at - time.time()
                            if stop_flag[0]:
                                return

                    # Step 1: Resolve running_ball fixture_id
                    print(f"  [opta/{gname}] resolving event_id={eid} (sport={sport})...")
                    provider = resolve_opta_provider(eid, sport=sport,
                                                      max_wait=args.resolve_timeout, interval=15)
                    if not provider:
                        print(f"  [opta/{gname}] resolution failed")
                        return
                    if stop_flag[0]:
                        return

                    fixture_id = provider["fixture_id"]

                    # Step 2: Resolve contestant → home/away mapping
                    print(f"  [opta/{gname}] resolving contestants for fixture_id={fixture_id}...")
                    contestant_map = resolve_opta_contestants(fixture_id)

                    if stop_flag[0]:
                        return
                    print(f"  [opta/{gname}] capturing...")
                    opta_capture_sync(fixture_id, contestant_map, gdir / "opta_raw.jsonl", stop_flag)

                t = threading.Thread(target=_opta_worker, daemon=True)
                t.start()
                opta_threads.append(t)

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
        for t in opta_threads:
            t.join(timeout=5)

    # Single-game mode
    elif args.v1_fixture_id or args.v2_event_id or args.boltodds_game_label:
        _run_single_game(
            v1_fixture_id=args.v1_fixture_id,
            v2_event_id=args.v2_event_id,
            v2_category_slug=args.v2_category_slug,
            v2_tournament_slug=args.v2_tournament_slug,
            v2_home_team=args.v2_home_team,
            v2_away_team=args.v2_away_team,
            v2_scheduled_date=args.v2_scheduled_date,
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
