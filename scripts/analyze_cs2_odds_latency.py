#!/usr/bin/env python3
"""
CS2 Odds vs Scores Latency Analysis.

Reads captured V1 scores + odds data for CS2 games and computes latency
deltas between score-based map winner detection (round-13) and odds-based
signals (CLOSED, RESULTED).

Usage:
    python scripts/analyze_cs2_odds_latency.py --data-dir captures/cs2_06_06
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path


# ─── map_winner() condition from the Rust evaluator ────────────────────

def map_winner(rh: int, ra: int) -> bool:
    big, small = max(rh, ra), min(rh, ra)
    d = big - small
    return (big == 13 and small <= 11) or (big >= 16 and big % 3 == 1 and d >= 2 and d <= 4)


# ─── Data structures ──────────────────────────────────────────────────

@dataclass
class OddsSignals:
    market_name: str = ""
    market_id: str = ""
    last_open_ts: int | None = None
    last_open_prob: float = 0.0      # max prob at last open
    last_open_favored: str = ""      # which selection was favored
    closed_ts: int | None = None
    resulted_ts: int | None = None
    resulted_winner: str = ""


@dataclass
class MapEvent:
    game: str = ""
    map_number: int = 0
    winner_side: str = ""            # "home" or "away"
    bo_format: str = ""              # "BO1" or "BO3"
    is_match_deciding: bool = False  # this map decided the match

    # Score signals (ts in ms)
    score_round13_ts: int | None = None
    score_round13_rounds: str = ""
    score_maps_update_ts: int | None = None
    score_closed_ts: int | None = None

    # Odds signals per market type
    odds_match_winner: OddsSignals = field(default_factory=OddsSignals)
    odds_map_winner_2way: OddsSignals = field(default_factory=OddsSignals)
    odds_map_winner_3way: OddsSignals = field(default_factory=OddsSignals)


# ─── Parsing helpers ──────────────────────────────────────────────────

def load_catalog(game_dir: Path) -> dict[str, dict]:
    path = game_dir / "v1_market_catalog.json"
    if not path.exists():
        return {}
    with open(path) as f:
        markets = json.load(f)
    by_id = {}
    for m in markets:
        by_id[m["id"]] = m
    return by_id


def find_market_id(catalog: dict, group_name: str) -> str | None:
    for mid, m in catalog.items():
        gn = m.get("group_name", "") or m.get("name", "")
        if gn == group_name:
            return mid
    return None


def parse_scores(game_dir: Path) -> list[dict]:
    path = game_dir / "v1_scores.jsonl"
    if not path.exists():
        return []
    entries = []
    with open(path) as f:
        for line in f:
            entry = json.loads(line)
            ts_ms = entry["ts_ns"] // 1_000_000
            frame = entry.get("frame", {})
            summary = frame.get("matchSummary", {})
            hs = int(summary.get("homeScore", "0") or "0")
            aws = int(summary.get("awayScore", "0") or "0")
            ft = (summary.get("matchStatusDisplay") or [{}])[0].get("freeText", "")
            cp = summary.get("currentPhase") or {}
            rh = int(cp.get("homeScore", "0") or "0")
            ra = int(cp.get("awayScore", "0") or "0")
            phase = int(cp.get("phase", "0") or "0")
            entries.append({
                "ts_ms": ts_ms,
                "maps_home": hs, "maps_away": aws,
                "rounds_home": rh, "rounds_away": ra,
                "phase": phase, "freetext": ft,
            })
    return entries


def parse_odds(game_dir: Path, market_ids: set[str]) -> list[dict]:
    """Parse odds frames, keeping only markets we care about."""
    path = game_dir / "v1_odds.jsonl"
    if not path.exists():
        return []
    entries = []
    with open(path) as f:
        for line in f:
            entry = json.loads(line)
            ts_ms = entry["ts_ns"] // 1_000_000
            frame = entry.get("frame", {})
            for m in frame.get("markets", []):
                mid = m.get("id", "")
                if mid not in market_ids:
                    continue
                status = m.get("status", "")
                sels = m.get("selections", [])
                sel_data = []
                for s in sels:
                    sel_data.append({
                        "id": s.get("id", ""),
                        "prob": s.get("probability"),
                        "status": s.get("status", ""),
                        "result": s.get("resultStatus", ""),
                    })
                entries.append({
                    "ts_ms": ts_ms,
                    "market_id": mid,
                    "status": status,
                    "selections": sel_data,
                })
    return entries


def extract_odds_signals(odds_entries: list[dict], market_id: str, market_name: str) -> OddsSignals:
    """Extract key timestamps for a specific market."""
    sig = OddsSignals(market_name=market_name, market_id=market_id)
    if not market_id:
        return sig

    for e in odds_entries:
        if e["market_id"] != market_id:
            continue
        ts = e["ts_ms"]
        status = e["status"]
        sels = e["selections"]

        if status == "OPEN":
            # Track last open with probability
            max_prob = 0.0
            favored = ""
            for s in sels:
                p = s.get("prob")
                if p:
                    pf = float(p)
                    if pf > max_prob:
                        max_prob = pf
                        favored = s.get("id", "")
            if max_prob > 0:
                sig.last_open_ts = ts
                sig.last_open_prob = max_prob
                sig.last_open_favored = favored

        if status == "CLOSED" and sig.closed_ts is None:
            sig.closed_ts = ts

        # RESULTED can come with market status CLOSED, RESULTED, or SUSPENDED
        for s in sels:
            if s.get("result") and sig.resulted_ts is None:
                sig.resulted_ts = ts
                sig.resulted_winner = s["result"]  # "WINNER" or "LOSER"

    return sig


# ─── Main analysis ────────────────────────────────────────────────────

def analyze_game(game_dir: Path) -> list[MapEvent]:
    name = game_dir.name
    catalog = load_catalog(game_dir)
    scores = parse_scores(game_dir)

    if len(scores) < 3:
        return []

    # Find market IDs
    match_winner_id = find_market_id(catalog, "Match Winner - Twoway") or ""
    map_winner_ids = {}
    map_winner_3way_ids = {}
    for n in range(1, 6):
        map_winner_ids[n] = find_market_id(catalog, f"Map {n} Winner - Twoway") or ""
        map_winner_3way_ids[n] = find_market_id(catalog, f"Map {n} Winner - Threeway") or ""

    all_market_ids = {match_winner_id} | set(map_winner_ids.values()) | set(map_winner_3way_ids.values())
    all_market_ids.discard("")
    odds_entries = parse_odds(game_dir, all_market_ids)

    # Determine BO format
    final_maps = scores[-1]
    total_maps = final_maps["maps_home"] + final_maps["maps_away"]
    first_maps = scores[0]
    first_total = first_maps["maps_home"] + first_maps["maps_away"]
    has_bo3_markets = any(map_winner_ids.get(n) for n in [2, 3])
    bo = "BO3" if has_bo3_markets or total_maps > 1 else "BO1"

    # Walk through scores to find map completions
    events: list[MapEvent] = []
    prev_maps_home = scores[0]["maps_home"]
    prev_maps_away = scores[0]["maps_away"]

    for i, s in enumerate(scores):
        phase = s["phase"]
        rh, ra = s["rounds_home"], s["rounds_away"]
        mh, ma = s["maps_home"], s["maps_away"]
        ft = s["freetext"]
        ts = s["ts_ms"]

        # Detect map completion via round-13 (or OT win)
        if phase > 0 and map_winner(rh, ra):
            # Check if we already recorded this map
            already = any(e.map_number == phase and e.score_round13_ts is not None for e in events)
            if not already:
                winner = "home" if rh > ra else "away"
                evt = MapEvent(
                    game=name,
                    map_number=phase,
                    winner_side=winner,
                    bo_format=bo,
                    score_round13_ts=ts,
                    score_round13_rounds=f"{rh}-{ra}",
                )
                events.append(evt)

        # Detect maps-won update
        if mh > prev_maps_home or ma > prev_maps_away:
            # Find which map this corresponds to
            map_num = mh + ma  # total maps completed
            # Find the event for this map, or create one
            matching = [e for e in events if e.map_number == map_num or
                        (e.score_maps_update_ts is None and e.map_number <= map_num)]
            if matching:
                evt = matching[-1]
                if evt.score_maps_update_ts is None:
                    evt.score_maps_update_ts = ts
            else:
                # No round-13 detected (score dropped it) — create event from maps update
                winner = "home" if mh > prev_maps_home else "away"
                evt = MapEvent(
                    game=name,
                    map_number=map_num,
                    winner_side=winner,
                    bo_format=bo,
                    score_maps_update_ts=ts,
                )
                events.append(evt)

            prev_maps_home = mh
            prev_maps_away = ma

        # Detect "Closed"
        if "closed" in ft.lower():
            for evt in events:
                if evt.score_closed_ts is None:
                    evt.score_closed_ts = ts

    # Determine which maps are match-deciding
    final_home = scores[-1]["maps_home"]
    final_away = scores[-1]["maps_away"]
    if bo == "BO1":
        for evt in events:
            evt.is_match_deciding = True
    else:
        maps_to_win = 2  # BO3
        for evt in events:
            # The deciding map is the one where someone reaches maps_to_win
            if evt.map_number == final_home + final_away:
                evt.is_match_deciding = True

    # Extract odds signals for each event
    for evt in events:
        evt.odds_match_winner = extract_odds_signals(odds_entries, match_winner_id, "Match Winner")
        mid_2way = map_winner_ids.get(evt.map_number, "")
        mid_3way = map_winner_3way_ids.get(evt.map_number, "")
        evt.odds_map_winner_2way = extract_odds_signals(odds_entries, mid_2way, f"Map {evt.map_number} Winner (2way)")
        evt.odds_map_winner_3way = extract_odds_signals(odds_entries, mid_3way, f"Map {evt.map_number} Winner (3way)")

    return events


def fmt_delta(a: int | None, b: int | None) -> str:
    if a is None or b is None:
        return "N/A"
    d = b - a  # positive = b is later (a is faster)
    sign = "+" if d >= 0 else ""
    return f"{sign}{d / 1000:.1f}s"


def fmt_ts(ts: int | None) -> str:
    if ts is None:
        return "—"
    return str(ts)


def main():
    ap = argparse.ArgumentParser(description="CS2 Odds vs Scores Latency Analysis")
    ap.add_argument("--data-dir", required=True, help="Directory containing game capture subdirs")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    all_events: list[MapEvent] = []

    for game_dir in sorted(data_dir.iterdir()):
        if not game_dir.is_dir():
            continue
        events = analyze_game(game_dir)
        all_events.extend(events)

    if not all_events:
        print("No map completion events found.")
        return

    # ─── Per-event table ──────────────────────────────────────────────
    print("=" * 130)
    print("CS2 ODDS vs SCORES LATENCY ANALYSIS")
    print("=" * 130)
    print()
    print(f"{'Game':<35s} {'Map':>3s} {'BO':>3s} {'Win':>4s} {'Dec':>3s} "
          f"{'R13→MW RESULTED':>16s} {'R13→MapW CLOSED':>16s} {'R13→MapW RESULTED':>18s} "
          f"{'MW→MapW RESULTED':>17s} {'R13→MapsUpd':>12s}")
    print("-" * 130)

    for evt in all_events:
        r13 = evt.score_round13_ts
        mw_res = evt.odds_match_winner.resulted_ts
        mapw_closed = evt.odds_map_winner_2way.closed_ts or evt.odds_map_winner_3way.closed_ts
        mapw_res = evt.odds_map_winner_2way.resulted_ts or evt.odds_map_winner_3way.resulted_ts
        maps_upd = evt.score_maps_update_ts

        # Only show MW→RESULTED delta for match-deciding maps
        r13_mw = fmt_delta(r13, mw_res) if evt.is_match_deciding else "n/a"
        r13_mapw_closed = fmt_delta(r13, mapw_closed)
        r13_mapw_res = fmt_delta(r13, mapw_res)
        mw_mapw = fmt_delta(mw_res, mapw_res) if evt.is_match_deciding and mw_res and mapw_res else "n/a"
        r13_maps = fmt_delta(r13, maps_upd)

        dec = "Y" if evt.is_match_deciding else ""

        print(f"{evt.game:<35s} {evt.map_number:>3d} {evt.bo_format:>3s} {evt.winner_side:>4s} {dec:>3s} "
              f"{r13_mw:>16s} {r13_mapw_closed:>16s} {r13_mapw_res:>18s} "
              f"{mw_mapw:>17s} {r13_maps:>12s}")

    # ─── Detail per event ─────────────────────────────────────────────
    print()
    print("=" * 100)
    print("DETAILED TIMESTAMPS (ms)")
    print("=" * 100)

    for evt in all_events:
        print(f"\n  {evt.game} Map {evt.map_number} ({evt.bo_format}, winner={evt.winner_side}"
              f"{', DECIDING' if evt.is_match_deciding else ''})")
        print(f"    Score round-13:     {fmt_ts(evt.score_round13_ts):>16s}  ({evt.score_round13_rounds})")
        print(f"    Score maps update:  {fmt_ts(evt.score_maps_update_ts):>16s}")
        print(f"    Score 'Closed':     {fmt_ts(evt.score_closed_ts):>16s}")

        for label, sig in [
            ("Match Winner", evt.odds_match_winner),
            ("Map Winner 2way", evt.odds_map_winner_2way),
            ("Map Winner 3way", evt.odds_map_winner_3way),
        ]:
            if not sig.market_id:
                continue
            streamed = sig.last_open_ts is not None or sig.closed_ts is not None or sig.resulted_ts is not None
            if not streamed:
                print(f"    {label:20s} (in catalog but NOT streamed)")
                continue
            print(f"    {label:20s} last_open={fmt_ts(sig.last_open_ts)} (prob={sig.last_open_prob:.3f})"
                  f"  closed={fmt_ts(sig.closed_ts)}  resulted={fmt_ts(sig.resulted_ts)}")

    # ─── Summary statistics ───────────────────────────────────────────
    print()
    print("=" * 100)
    print("SUMMARY STATISTICS")
    print("=" * 100)

    # Collect deltas
    r13_vs_mw_resulted = []
    r13_vs_mapw_resulted = []
    r13_vs_mapw_closed = []
    mw_vs_mapw_resulted = []
    r13_vs_maps_update = []
    closed_vs_resulted = []

    for evt in all_events:
        r13 = evt.score_round13_ts
        mw_res = evt.odds_match_winner.resulted_ts
        mapw_closed = evt.odds_map_winner_2way.closed_ts or evt.odds_map_winner_3way.closed_ts
        mapw_res = evt.odds_map_winner_2way.resulted_ts or evt.odds_map_winner_3way.resulted_ts
        maps_upd = evt.score_maps_update_ts

        if r13 and mw_res and evt.is_match_deciding:
            r13_vs_mw_resulted.append((mw_res - r13) / 1000)
        if r13 and mapw_res:
            r13_vs_mapw_resulted.append((mapw_res - r13) / 1000)
        if r13 and mapw_closed:
            r13_vs_mapw_closed.append((mapw_closed - r13) / 1000)
        if mw_res and mapw_res and evt.is_match_deciding:
            mw_vs_mapw_resulted.append((mapw_res - mw_res) / 1000)
        if r13 and maps_upd:
            r13_vs_maps_update.append((maps_upd - r13) / 1000)
        if mapw_closed and mapw_res:
            closed_vs_resulted.append((mapw_res - mapw_closed) / 1000)

    def print_stats(label: str, values: list[float], unit: str = "s"):
        if not values:
            print(f"\n  {label}: no data")
            return
        values.sort()
        n = len(values)
        mean = sum(values) / n
        median = values[n // 2]
        print(f"\n  {label} (n={n}):")
        print(f"    Mean:   {mean:+.1f}{unit}")
        print(f"    Median: {median:+.1f}{unit}")
        print(f"    Min:    {min(values):+.1f}{unit}")
        print(f"    Max:    {max(values):+.1f}{unit}")
        faster_score = sum(1 for v in values if v > 0)
        faster_odds = sum(1 for v in values if v < 0)
        print(f"    Score faster: {faster_score}/{n}  |  Odds faster: {faster_odds}/{n}")

    print_stats("Score round-13 vs Odds RESULTED (Match Winner) [deciding maps only]",
                r13_vs_mw_resulted)
    print_stats("Score round-13 vs Odds RESULTED (Map Winner)",
                r13_vs_mapw_resulted)
    print_stats("Score round-13 vs Odds CLOSED (Map Winner)",
                r13_vs_mapw_closed)
    print_stats("Match Winner RESULTED vs Map Winner RESULTED [deciding maps]",
                mw_vs_mapw_resulted)
    print_stats("Score round-13 vs Score maps-won update (Signal 1 vs Signal 2)",
                r13_vs_maps_update)
    print_stats("Map Winner CLOSED vs Map Winner RESULTED (gap)",
                closed_vs_resulted)

    # ─── Conclusion ───────────────────────────────────────────────────
    print()
    print("=" * 100)
    print("CONCLUSION")
    print("=" * 100)

    if r13_vs_mw_resulted:
        mean_r13_mw = sum(r13_vs_mw_resulted) / len(r13_vs_mw_resulted)
        score_wins = sum(1 for v in r13_vs_mw_resulted if v > 0)
        total = len(r13_vs_mw_resulted)
        if score_wins == total:
            print(f"\n  Score round-13 is FASTER than odds RESULTED in ALL {total} deciding maps.")
            print(f"  Average advantage: {mean_r13_mw:.1f}s")
            print(f"  → Odds stream does NOT provide a latency advantage for CS2 map winner detection.")
        elif score_wins > total / 2:
            print(f"\n  Score round-13 is faster in {score_wins}/{total} deciding maps (avg {mean_r13_mw:.1f}s).")
            print(f"  → Odds stream is occasionally faster but score-based detection is generally better.")
        else:
            odds_wins = total - score_wins
            print(f"\n  Odds RESULTED is faster in {odds_wins}/{total} deciding maps (avg {-mean_r13_mw:.1f}s).")
            print(f"  → Odds stream provides a latency advantage for CS2!")

    if r13_vs_maps_update:
        mean_sig2 = sum(r13_vs_maps_update) / len(r13_vs_maps_update)
        print(f"\n  Score Signal 2 (maps-won update) arrives {mean_sig2:.1f}s after round-13 on average.")
        if r13_vs_mw_resulted:
            mean_mw = sum(r13_vs_mw_resulted) / len(r13_vs_mw_resulted)
            if mean_mw < mean_sig2:
                print(f"  Odds RESULTED ({mean_mw:.1f}s after R13) is faster than Signal 2 ({mean_sig2:.1f}s after R13).")
                print(f"  → Odds could serve as a better FALLBACK than the maps-won counter.")


if __name__ == "__main__":
    main()
