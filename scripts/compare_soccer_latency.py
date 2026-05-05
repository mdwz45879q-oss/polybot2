#!/usr/bin/env python3
"""
Compare soccer latency between Kalstrop V1, Kalstrop V2, and BoltOdds.

Usage:
    # Single game:
    python scripts/compare_soccer_latency.py --game-dir captures/2026_05_04/chelsea_nottingham

    # All soccer games from a capture session:
    python scripts/compare_soccer_latency.py \
        --captures-dir captures/2026_05_04 \
        --games-file captures/2026_05_04/games_05_04.json
"""

import argparse
import json
from pathlib import Path

from parse_soccer_events import parse_v1_soccer_events, parse_boltodds_soccer_events, parse_v2_soccer_events


def load_soccer_game_dirs(captures_dir: str, games_file: str) -> list[tuple[str, Path]]:
    captures = Path(captures_dir)
    with open(games_file) as f:
        games = json.load(f)
    dirs: list[tuple[str, Path]] = []
    for game in games:
        sport = str(game.get("sport", "soccer")).strip().lower()
        if sport not in ("soccer", "football"):
            continue
        name = str(game.get("name", "")).strip()
        if not name:
            continue
        game_dir = captures / name
        if game_dir.exists():
            dirs.append((name, game_dir))
    return dirs


def categorize_event(event_type: str) -> str:
    if event_type.startswith("total_over:"):
        return "total_over"
    elif event_type.startswith("corner:"):
        return "corner"
    elif event_type == "halftime":
        return "halftime"
    elif event_type == "game_end":
        return "game_end"
    return "other"


def compute_pairwise(
    events_a: list[tuple[str, int]],
    events_b: list[tuple[str, int]],
) -> list[tuple[str, float]]:
    map_a = {e: t for e, t in events_a}
    map_b = {e: t for e, t in events_b}
    common = sorted(set(map_a.keys()) & set(map_b.keys()))
    return [(event, (map_b[event] - map_a[event]) / 1_000_000) for event in common]


def print_pair_summary(
    label_a: str,
    label_b: str,
    all_deltas: list[tuple[str, float, str]],
) -> None:
    if not all_deltas:
        print(f"\n  {label_a} vs {label_b}: no comparable events")
        return

    all_ms = [d for _, d, _ in all_deltas]
    a_faster = sum(1 for d in all_ms if d > 0)
    b_faster = sum(1 for d in all_ms if d < 0)
    total = len(all_ms)
    avg_abs = sum(abs(d) for d in all_ms) / total
    avg_signed = sum(all_ms) / total

    print(f"\n  {label_a} vs {label_b}  ({total} events)")
    print(f"    {label_a} faster: {a_faster}/{total} ({100*a_faster/total:.0f}%)")
    print(f"    {label_b} faster: {b_faster}/{total} ({100*b_faster/total:.0f}%)")
    print(f"    Avg absolute: {avg_abs:.1f} ms")
    print(f"    Avg signed:   {avg_signed:+.1f} ms (positive = {label_a} faster)")

    by_cat: dict[str, list[float]] = {}
    for event_type, delta_ms, _ in all_deltas:
        cat = categorize_event(event_type)
        by_cat.setdefault(cat, []).append(delta_ms)

    print(f"    {'Category':<15} {'Count':>6} {label_a+' faster':>12} {label_b+' faster':>12} {'Avg abs':>10} {'Avg signed':>12}")
    print(f"    {'-'*15} {'-'*6} {'-'*12} {'-'*12} {'-'*10} {'-'*12}")
    for cat in ["total_over", "halftime", "corner", "game_end"]:
        deltas = by_cat.get(cat)
        if not deltas:
            continue
        n = len(deltas)
        af = sum(1 for d in deltas if d > 0)
        bf = sum(1 for d in deltas if d < 0)
        print(f"    {cat:<15} {n:>6} {af:>12} {bf:>12} {sum(abs(d) for d in deltas)/n:>10.1f} {sum(deltas)/n:>+12.1f}")


def print_per_game_detail(
    game_name: str,
    pairs: dict[str, list[tuple[str, float]]],
) -> None:
    print(f"\n  {game_name}:")
    all_events = set()
    for deltas in pairs.values():
        all_events.update(e for e, _ in deltas)

    if not all_events:
        print("    no comparable events")
        return

    header_parts = [f"{'Event':<22}"]
    pair_labels = sorted(pairs.keys())
    for label in pair_labels:
        header_parts.append(f"{label:>14}")
    print(f"    {''.join(header_parts)}")
    print(f"    {'-' * (22 + 14 * len(pair_labels))}")

    for event in sorted(all_events):
        parts = [f"{event:<22}"]
        for label in pair_labels:
            delta_map = {e: d for e, d in pairs[label]}
            d = delta_map.get(event)
            if d is not None:
                parts.append(f"{d:>+12.1f}ms")
            else:
                parts.append(f"{'—':>14}")
        print(f"    {''.join(parts)}")


def main():
    ap = argparse.ArgumentParser(description="Compare soccer latency: V1 vs V2 vs BoltOdds")
    ap.add_argument("--game-dir", type=str, default="")
    ap.add_argument("--captures-dir", type=str, default="")
    ap.add_argument("--games-file", type=str, default="")
    args = ap.parse_args()

    game_dirs: list[tuple[str, Path]] = []
    if args.game_dir:
        gd = Path(args.game_dir)
        game_dirs.append((gd.name, gd))
    elif args.captures_dir and args.games_file:
        game_dirs = load_soccer_game_dirs(args.captures_dir, args.games_file)
    else:
        ap.error("Provide either --game-dir or both --captures-dir and --games-file")

    if not game_dirs:
        print("No soccer game directories found.")
        return

    print(f"Found {len(game_dirs)} soccer game(s) to analyze.")

    # Accumulate all pairwise deltas: pair_key -> [(event, delta_ms, game_name)]
    agg: dict[str, list[tuple[str, float, str]]] = {
        "V1 vs BO": [],
        "V1 vs V2": [],
        "BO vs V2": [],
    }

    sources = [
        ("v1_raw.jsonl", parse_v1_soccer_events, "V1"),
        ("v2_raw.jsonl", parse_v2_soccer_events, "V2"),
        ("boltodds_raw.jsonl", parse_boltodds_soccer_events, "BO"),
    ]

    for game_name, game_dir in game_dirs:
        parsed: dict[str, list[tuple[str, int]]] = {}
        for filename, parser, label in sources:
            path = game_dir / filename
            if path.exists():
                events = parser(str(path))
                if events:
                    parsed[label] = events

        if len(parsed) < 2:
            print(f"  {game_name}: skipped (need at least 2 sources, have {list(parsed.keys())})")
            continue

        game_pairs: dict[str, list[tuple[str, float]]] = {}
        pair_combos = [("V1", "BO", "V1 vs BO"), ("V1", "V2", "V1 vs V2"), ("BO", "V2", "BO vs V2")]
        for la, lb, pair_key in pair_combos:
            if la in parsed and lb in parsed:
                deltas = compute_pairwise(parsed[la], parsed[lb])
                game_pairs[pair_key] = deltas
                for event_type, delta_ms in deltas:
                    agg[pair_key].append((event_type, delta_ms, game_name))

        print_per_game_detail(game_name, game_pairs)

    # Aggregate
    print("\n" + "=" * 70)
    print("AGGREGATE RESULTS")
    print("=" * 70)

    for pair_key in ["V1 vs BO", "V1 vs V2", "BO vs V2"]:
        print_pair_summary(pair_key.split(" vs ")[0], pair_key.split(" vs ")[1], agg[pair_key])


if __name__ == "__main__":
    main()
