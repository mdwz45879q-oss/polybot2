#!/usr/bin/env python3
"""
Standalone BoltOdds capture script for debugging subscription issues.

Connects to the livescores and/or play-by-play WebSocket endpoints,
subscribes to the games listed in a games.json file, and logs all
messages (including errors) to stdout and per-game JSONL files.

For esports games, BoltOdds uses different game labels on the PBP
endpoint vs the standard get_games endpoint. Use --resolve-esports
to fetch the correct PBP labels from /api/playbyplay/esports and
match them to your games.json entries.

Usage:
    export BOLTODDS_API_KEY=your_key

    # Standard sports (baseball, soccer, etc.)
    python capture_boltodds_debug.py --games-file games.json --out ./captures

    # Esports (resolves PBP labels automatically)
    python capture_boltodds_debug.py --games-file games.json --out ./captures --resolve-esports

    # PBP only
    python capture_boltodds_debug.py --games-file games.json --out ./captures --pbp-only --resolve-esports

games.json format:
    [
        {"name": "team_a_vs_team_b", "boltodds_game_label": "Team A vs Team B, 2026-05-28, 01"},
        ...
    ]
"""

import argparse
import asyncio
import json
import os
import re
import signal
import sys
import time
import unicodedata
import urllib.request
from pathlib import Path

try:
    import websockets
except ImportError:
    print("pip install websockets")
    sys.exit(2)

API_KEY = os.environ.get("BOLTODDS_API_KEY", "")

LIVESCORES_WS = "wss://spro.agency/api/livescores"
PBP_WS = "wss://spro.agency/api/playbyplay"
PBP_ESPORTS_URL = "https://spro.agency/api/playbyplay/esports"


def sanitize(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _normalize_team(s: str) -> str:
    """Normalize team name for fuzzy matching.

    Strips accents, lowercases, removes all non-alphanumeric characters
    including spaces — so "Game Hunters" and "GameHunters" both become
    "gamehunters", and "1Win" and "1w" won't match (correctly — they're
    genuinely different abbreviations that need the PBP label).
    """
    s = _strip_accents(s.strip().lower())
    s = re.sub(r"[^a-z0-9]", "", s)
    return s


def resolve_esports_pbp_labels(game_labels: list[str]) -> dict[str, str]:
    """Fetch PBP esports labels and match them to standard game labels.

    Returns a dict mapping standard label → PBP esports label for games
    that match by team names + date.
    """
    url = f"{PBP_ESPORTS_URL}?key={API_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            pbp_games = json.loads(resp.read())
    except Exception as e:
        print(f"[resolve-esports] failed to fetch PBP esports games: {e}")
        return {}

    print(f"[resolve-esports] fetched {len(pbp_games)} PBP esports games")

    # Parse each PBP label into (normalized_teams, date_suffix)
    def parse_label(label: str):
        # "Team A vs Team B, 2026-05-28, 01" → (teams_part, date_suffix)
        parts = label.rsplit(", ", 2)
        if len(parts) >= 3:
            teams = parts[0]
            date_suffix = f"{parts[1]}, {parts[2]}"
        elif len(parts) == 2:
            teams = parts[0]
            date_suffix = parts[1]
        else:
            teams = label
            date_suffix = ""
        # Split teams by " vs "
        team_parts = teams.split(" vs ", 1)
        if len(team_parts) == 2:
            return (_normalize_team(team_parts[0]), _normalize_team(team_parts[1]), date_suffix)
        return (_normalize_team(teams), "", date_suffix)

    # Build index of PBP labels by normalized teams + date
    pbp_index: dict[tuple, str] = {}
    for pbp_label in pbp_games:
        h, a, ds = parse_label(pbp_label)
        pbp_index[(h, a, ds)] = pbp_label
        # Also index with swapped team order
        if a:
            pbp_index[(a, h, ds)] = pbp_label

    # Match each standard label to a PBP label
    resolved: dict[str, str] = {}
    for std_label in game_labels:
        h, a, ds = parse_label(std_label)
        key = (h, a, ds)
        if key in pbp_index:
            resolved[std_label] = pbp_index[key]
        else:
            # Try matching by just team names (ignore date suffix)
            for (ph, pa, pds), plabel in pbp_index.items():
                if h == ph and a == pa:
                    resolved[std_label] = plabel
                    break

    matched = len(resolved)
    unmatched = len(game_labels) - matched
    print(f"[resolve-esports] matched {matched}/{len(game_labels)} labels"
          f" ({unmatched} unmatched)")

    for std, pbp in resolved.items():
        if std != pbp:
            print(f"  {std}")
            print(f"    → {pbp}")

    return resolved


SUBSCRIBE_LEAD_SECONDS = 120  # subscribe 2 min before kickoff
SUBSCRIPTION_CHECK_INTERVAL = 30  # check for new games to subscribe every 30s


async def capture_endpoint(
    ws_url: str,
    tag: str,
    game_labels: list[str],
    label_to_name: dict[str, str],
    label_to_kickoff: dict[str, int | None],
    out_dir: Path,
    filename: str,
    stop: asyncio.Event,
):
    # Open per-game output files
    file_handles: dict[str, any] = {}
    for label, game_name in label_to_name.items():
        game_dir = out_dir / game_name
        game_dir.mkdir(parents=True, exist_ok=True)
        file_handles[game_name] = (game_dir / filename).open("a")

    count = 0
    errors = 0
    connected_games = 0
    backoff = 30.0

    # Track subscription state
    subscribed: set[str] = set()
    finished: set[str] = set()

    def _games_to_subscribe_now() -> list[str]:
        """Return labels that should be subscribed right now."""
        now = int(time.time())
        ready = []
        for label in game_labels:
            if label in subscribed or label in finished:
                continue
            kickoff = label_to_kickoff.get(label)
            if kickoff is None:
                # No kickoff time — subscribe immediately
                ready.append(label)
            elif now >= kickoff - SUBSCRIBE_LEAD_SECONDS:
                ready.append(label)
        return ready

    while not stop.is_set():
        try:
            uri = f"{ws_url}?key={API_KEY}"
            print(f"[{tag}] connecting to {ws_url} ...")
            async with websockets.connect(uri, max_size=None) as ws:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                handshake = json.loads(raw) if isinstance(raw, (str, bytes)) else {}
                print(f"[{tag}] handshake: {handshake}")

                backoff = 30.0
                connected_games = 0
                errors = 0
                subscribed.clear()

                # Initial subscribe for games that are live or near kickoff
                initial = _games_to_subscribe_now()
                if initial:
                    sub = {"action": "subscribe", "filters": {"games": initial}}
                    await ws.send(json.dumps(sub))
                    subscribed.update(initial)
                    print(f"[{tag}] initial subscribe: {len(initial)} game(s)")
                else:
                    # Subscribe to empty list to keep connection alive
                    # (will resubscribe when games go live)
                    next_kickoff = None
                    for label in game_labels:
                        k = label_to_kickoff.get(label)
                        if k and (next_kickoff is None or k < next_kickoff):
                            next_kickoff = k
                    if next_kickoff:
                        wait_min = max(0, (next_kickoff - int(time.time()) - SUBSCRIBE_LEAD_SECONDS)) / 60
                        print(f"[{tag}] no games live yet, next kickoff in ~{wait_min:.0f} min")

                last_sub_check = time.time()

                async for raw in ws:
                    if stop.is_set():
                        break
                    ts_ns = time.time_ns()
                    try:
                        frame = json.loads(raw)
                    except Exception:
                        frame = {"_raw": str(raw)[:500]}

                    action = frame.get("action", "") if isinstance(frame, dict) else ""

                    if action == "error":
                        errors += 1
                        msg = frame.get("message", "")
                        print(f"[{tag}] ERROR #{errors}: {msg}")
                    elif action == "connected":
                        connected_games += 1
                        event = frame.get("event", "")
                        stream = frame.get("stream", "")
                        print(f"[{tag}] CONNECTED #{connected_games}: stream={stream} event={event}")
                    elif action == "subscription_updated":
                        print(f"[{tag}] subscription_updated: {frame.get('message', '')}")
                    elif action in ("game_removed",):
                        # Game finished — track it
                        gl = frame.get("game") or frame.get("event") or ""
                        if gl:
                            finished.add(gl)
                            print(f"[{tag}] game finished: {gl[:50]}")
                    elif action == "ping":
                        pass
                    elif action in ("socket_connected",):
                        pass
                    else:
                        count += 1
                        if count <= 3 or count % 100 == 0:
                            event = frame.get("game", "") or frame.get("event", "")
                            print(f"[{tag}] frame #{count}: action={action} game={event[:50]}")

                    # Route to per-game file
                    gl = ""
                    if isinstance(frame, dict):
                        gl = frame.get("game") or frame.get("event") or ""
                    game_name = label_to_name.get(gl)

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

                    # Periodically check for new games to subscribe
                    now = time.time()
                    if now - last_sub_check >= SUBSCRIPTION_CHECK_INTERVAL:
                        last_sub_check = now
                        new_games = _games_to_subscribe_now()
                        if new_games:
                            # Resubscribe with full list (BoltOdds replaces filters)
                            all_active = [l for l in subscribed | set(new_games) if l not in finished]
                            sub = {"action": "subscribe", "filters": {"games": all_active}}
                            await ws.send(json.dumps(sub))
                            subscribed.update(new_games)
                            print(f"[{tag}] added {len(new_games)} game(s), total active: {len(all_active)}")

                print(f"[{tag}] connection closed normally")

        except websockets.exceptions.ConnectionClosedError as e:
            if stop.is_set():
                break
            print(f"[{tag}] ConnectionClosedError: {e} -- reconnecting in {backoff:.0f}s")
        except websockets.exceptions.ConnectionClosedOK:
            if stop.is_set():
                break
            print(f"[{tag}] connection closed OK -- reconnecting in {backoff:.0f}s")
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

    print(f"[{tag}] done: {count} data frames, {connected_games} connected, {errors} errors")


def main():
    ap = argparse.ArgumentParser(description="BoltOdds capture (livescores + play-by-play)")
    ap.add_argument("--games-file", required=True, help="Path to games.json")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--duration", type=int, default=300, help="Max seconds (default 300)")
    ap.add_argument("--livescores-only", action="store_true", help="Only use livescores endpoint")
    ap.add_argument("--pbp-only", action="store_true", help="Only use play-by-play endpoint")
    ap.add_argument("--resolve-esports", action="store_true",
                    help="Fetch PBP esports labels from /api/playbyplay/esports "
                         "and use them for PBP subscriptions (team names differ between endpoints)")
    args = ap.parse_args()

    if not API_KEY:
        print("ERROR: set BOLTODDS_API_KEY env var")
        return 1

    with open(args.games_file) as f:
        games = json.load(f)

    # Build standard label → game name + kickoff mapping
    std_labels = []
    label_to_name: dict[str, str] = {}
    label_to_kickoff: dict[str, int | None] = {}
    for g in games:
        label = str(g.get("boltodds_game_label") or "").strip()
        if not label:
            continue
        name = g.get("name", sanitize(label))
        std_labels.append(label)
        label_to_name[label] = name
        kickoff = g.get("start_ts_utc")
        label_to_kickoff[label] = int(kickoff) if kickoff is not None else None

    if not std_labels:
        print("ERROR: no games with 'boltodds_game_label' found in games.json")
        return 1

    # Resolve esports labels if requested. The /api/playbyplay/esports
    # endpoint returns the correct game labels for esports — these work
    # on BOTH the livescores and PBP endpoints. The standard /api/get_games
    # labels do NOT work for esports (cause Code 1 errors).
    resolved: dict[str, str] = {}
    if args.resolve_esports:
        resolved = resolve_esports_pbp_labels(std_labels)
        if resolved:
            # Replace labels for ALL endpoints (livescores + PBP)
            new_labels = []
            new_label_to_name = {}
            new_label_to_kickoff = {}
            for std_label in std_labels:
                correct_label = resolved.get(std_label, std_label)
                game_name = label_to_name[std_label]
                new_labels.append(correct_label)
                new_label_to_name[correct_label] = game_name
                new_label_to_kickoff[correct_label] = label_to_kickoff.get(std_label)
            std_labels = new_labels
            label_to_name = new_label_to_name
            label_to_kickoff = new_label_to_kickoff

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nBoltOdds capture: {len(std_labels)} game(s), duration={args.duration}s")
    if args.resolve_esports and resolved:
        print(f"  Esports labels resolved: {len(resolved)}/{len(std_labels)}")
    print()

    stop = asyncio.Event()

    def handle_sig(*_):
        print("\n[signal] stopping...")
        stop.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    # Show subscription schedule
    now_ts = int(time.time())
    n_live_now = sum(1 for l in std_labels
                     if label_to_kickoff.get(l) is None or label_to_kickoff[l] <= now_ts + SUBSCRIBE_LEAD_SECONDS)
    n_upcoming = len(std_labels) - n_live_now
    print(f"  Live/imminent: {n_live_now}, upcoming: {n_upcoming}")
    print()

    async def run():
        tasks = []
        if not args.pbp_only:
            tasks.append(asyncio.create_task(
                capture_endpoint(LIVESCORES_WS, "livescores", std_labels,
                                 label_to_name, label_to_kickoff,
                                 out_dir, "boltodds_livescores.jsonl", stop)))
        if not args.livescores_only:
            tasks.append(asyncio.create_task(
                capture_endpoint(PBP_WS, "pbp", std_labels,
                                 label_to_name, label_to_kickoff,
                                 out_dir, "boltodds_pbp.jsonl", stop)))

        if not tasks:
            print("ERROR: no endpoints selected")
            return

        print(f"Started {len(tasks)} endpoint(s)\n")

        try:
            await asyncio.wait_for(stop.wait(), timeout=args.duration)
        except asyncio.TimeoutError:
            print(f"\n[+] duration ({args.duration}s) elapsed")
        finally:
            stop.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(run())
    print(f"\nDone. Output in {out_dir}/")


if __name__ == "__main__":
    sys.exit(main() or 0)
