#!/usr/bin/env python3
"""
Compare baseball latency between Kalstrop V1 and BoltOdds across multiple games.

Reads a games.json file, identifies baseball games, parses event timelines from
both sources, and produces aggregate latency statistics.

Usage:
    # Single game:
    python scripts/compare_baseball_latency.py --game-dir captures/2026_05_04/rays_jays

    # All baseball games from a capture session:
    python scripts/compare_baseball_latency.py \
        --captures-dir captures/2026_05_04 \
        --games-file captures/2026_05_04/games_05_04.json

Output:
    Per-event-type statistics:
    - Proportion of times each source is faster
    - Average absolute timing difference (ms)
    - Per-game breakdown
"""

import argparse
import json
from pathlib import Path

from parse_baseball_events import parse_v1_events, parse_boltodds_events


def load_baseball_game_dirs(captures_dir: str, games_file: str) -> list[tuple[str, Path]]:
    """Load game directories for baseball games from games.json."""
    captures = Path(captures_dir)
    with open(games_file) as f:
        games = json.load(f)

    dirs: list[tuple[str, Path]] = []
    for game in games:
        sport = str(game.get("sport", "baseball")).strip().lower()
        if sport != "baseball":
            continue
        name = str(game.get("name", "")).strip()
        if not name:
            continue
        game_dir = captures / name
        if game_dir.exists():
            dirs.append((name, game_dir))
    return dirs


def compute_deltas(
    v1_events: list[tuple[str, int]],
    bo_events: list[tuple[str, int]],
) -> list[tuple[str, float]]:
    """Compute per-event deltas. Returns (event_type, delta_ms) where positive = V1 faster."""
    v1_map = {e: t for e, t in v1_events}
    bo_map = {e: t for e, t in bo_events}
    common = sorted(set(v1_map.keys()) & set(bo_map.keys()))

    deltas: list[tuple[str, float]] = []
    for event in common:
        delta_ms = (bo_map[event] - v1_map[event]) / 1_000_000
        deltas.append((event, delta_ms))
    return deltas


def categorize_event(event_type: str) -> str:
    if event_type.startswith("nrfi_"):
        return "nrfi"
    elif event_type.startswith("total_over:"):
        return "total_over"
    elif event_type == "game_end":
        return "game_end"
    return "other"


def print_per_game(game_name: str, deltas: list[tuple[str, float]]) -> None:
    if not deltas:
        print(f"  {game_name}: no comparable events")
        return
    print(f"\n  {game_name} ({len(deltas)} events):")
    for event, delta_ms in deltas:
        faster = "V1" if delta_ms > 0 else "BO" if delta_ms < 0 else "TIE"
        print(f"    {event:<22} {delta_ms:>+8.1f} ms  ({faster})")


def print_aggregate(all_deltas: list[tuple[str, float, str]]) -> None:
    """Print aggregate stats. Each entry is (event_type, delta_ms, game_name)."""
    if not all_deltas:
        print("\nNo comparable events found.")
        return

    # Group by category
    by_category: dict[str, list[float]] = {}
    for event_type, delta_ms, _ in all_deltas:
        cat = categorize_event(event_type)
        by_category.setdefault(cat, []).append(delta_ms)

    # Overall
    all_ms = [d for _, d, _ in all_deltas]
    v1_faster = sum(1 for d in all_ms if d > 0)
    bo_faster = sum(1 for d in all_ms if d < 0)
    ties = sum(1 for d in all_ms if d == 0)
    total = len(all_ms)
    avg_abs = sum(abs(d) for d in all_ms) / total if total else 0
    avg_signed = sum(all_ms) / total if total else 0

    print("\n" + "=" * 70)
    print("AGGREGATE RESULTS")
    print("=" * 70)
    print(f"\n  Total comparable events: {total}")
    print(f"  V1 faster:  {v1_faster}/{total} ({100*v1_faster/total:.0f}%)")
    print(f"  BO faster:  {bo_faster}/{total} ({100*bo_faster/total:.0f}%)")
    if ties:
        print(f"  Ties:       {ties}/{total}")
    print(f"\n  Avg absolute difference: {avg_abs:.1f} ms")
    print(f"  Avg signed difference:   {avg_signed:+.1f} ms (positive = V1 faster)")

    # Per category
    print(f"\n  {'Category':<15} {'Count':>6} {'V1 faster':>10} {'BO faster':>10} {'Avg abs (ms)':>13} {'Avg signed':>12}")
    print(f"  {'-'*15} {'-'*6} {'-'*10} {'-'*10} {'-'*13} {'-'*12}")
    for cat in ["nrfi", "total_over", "game_end", "other"]:
        deltas = by_category.get(cat)
        if not deltas:
            continue
        n = len(deltas)
        v1 = sum(1 for d in deltas if d > 0)
        bo = sum(1 for d in deltas if d < 0)
        avg_a = sum(abs(d) for d in deltas) / n
        avg_s = sum(deltas) / n
        print(f"  {cat:<15} {n:>6} {v1:>10} {bo:>10} {avg_a:>13.1f} {avg_s:>+12.1f}")


def main():
    ap = argparse.ArgumentParser(description="Compare baseball latency: V1 vs BoltOdds")
    ap.add_argument("--game-dir", type=str, default="",
                    help="Single game directory (has v1_raw.jsonl + boltodds_raw.jsonl)")
    ap.add_argument("--captures-dir", type=str, default="",
                    help="Parent captures directory (contains per-game subdirs)")
    ap.add_argument("--games-file", type=str, default="",
                    help="games.json file to identify baseball games")
    args = ap.parse_args()

    game_dirs: list[tuple[str, Path]] = []

    if args.game_dir:
        gd = Path(args.game_dir)
        game_dirs.append((gd.name, gd))
    elif args.captures_dir and args.games_file:
        game_dirs = load_baseball_game_dirs(args.captures_dir, args.games_file)
    else:
        ap.error("Provide either --game-dir or both --captures-dir and --games-file")

    if not game_dirs:
        print("No baseball game directories found.")
        return

    print(f"Found {len(game_dirs)} baseball game(s) to analyze.\n")

    all_deltas: list[tuple[str, float, str]] = []

    for game_name, game_dir in game_dirs:
        v1_path = game_dir / "v1_raw.jsonl"
        bo_path = game_dir / "boltodds_raw.jsonl"

        if not v1_path.exists() or not bo_path.exists():
            print(f"  {game_name}: skipped (missing {'v1' if not v1_path.exists() else 'boltodds'} file)")
            continue

        v1_events = parse_v1_events(str(v1_path))
        bo_events = parse_boltodds_events(str(bo_path))

        if not v1_events or not bo_events:
            print(f"  {game_name}: skipped (no events from {'v1' if not v1_events else 'boltodds'})")
            continue

        deltas = compute_deltas(v1_events, bo_events)
        print_per_game(game_name, deltas)

        for event_type, delta_ms in deltas:
            all_deltas.append((event_type, delta_ms, game_name))

    print_aggregate(all_deltas)


if __name__ == "__main__":
    main()
