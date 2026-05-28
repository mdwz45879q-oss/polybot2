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


async def capture_endpoint(
    ws_url: str,
    tag: str,
    game_labels: list[str],
    label_to_name: dict[str, str],
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
    backoff = 10.0

    while not stop.is_set():
        try:
            uri = f"{ws_url}?key={API_KEY}"
            print(f"[{tag}] connecting to {ws_url} ...")
            async with websockets.connect(uri, max_size=None) as ws:
                # Wait for socket_connected handshake
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                handshake = json.loads(raw) if isinstance(raw, (str, bytes)) else {}
                print(f"[{tag}] handshake: {handshake}")

                # Subscribe
                sub = {"action": "subscribe", "filters": {"games": game_labels}}
                await ws.send(json.dumps(sub))
                print(f"[{tag}] subscribed to {len(game_labels)} game(s)")

                backoff = 10.0
                connected_games = 0
                errors = 0

                async for raw in ws:
                    if stop.is_set():
                        break
                    ts_ns = time.time_ns()
                    try:
                        frame = json.loads(raw)
                    except Exception:
                        frame = {"_raw": str(raw)[:500]}

                    action = frame.get("action", "") if isinstance(frame, dict) else ""

                    # Log errors and connections to stdout
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

    # Build standard label → game name mapping
    std_labels = []
    label_to_name: dict[str, str] = {}
    for g in games:
        label = str(g.get("boltodds_game_label") or "").strip()
        if not label:
            continue
        name = g.get("name", sanitize(label))
        std_labels.append(label)
        label_to_name[label] = name

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
            for std_label in std_labels:
                correct_label = resolved.get(std_label, std_label)
                game_name = label_to_name[std_label]
                new_labels.append(correct_label)
                new_label_to_name[correct_label] = game_name
            std_labels = new_labels
            label_to_name = new_label_to_name

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

    async def run():
        tasks = []
        if not args.pbp_only:
            tasks.append(asyncio.create_task(
                capture_endpoint(LIVESCORES_WS, "livescores", std_labels,
                                 label_to_name, out_dir, "boltodds_livescores.jsonl", stop)))
        if not args.livescores_only:
            tasks.append(asyncio.create_task(
                capture_endpoint(PBP_WS, "pbp", std_labels,
                                 label_to_name, out_dir, "boltodds_pbp.jsonl", stop)))

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
