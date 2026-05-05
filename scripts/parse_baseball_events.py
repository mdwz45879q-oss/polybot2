#!/usr/bin/env python3
"""
Parse baseball capture files and extract betting-relevant event timelines.

Reads V1 and BoltOdds JSONL files for a baseball game and outputs a normalized
list of events with nanosecond timestamps for latency comparison.

Usage:
    python scripts/parse_baseball_events.py --game-dir captures/2026_05_04/rays_jays
"""

import argparse
import json
from pathlib import Path


def parse_v1_events(jsonl_path: str) -> list[tuple[str, int]]:
    events: list[tuple[str, int]] = []
    prev_total = 0
    first_inning = True
    game_ended = False

    with open(jsonl_path) as f:
        for line in f:
            row = json.loads(line)
            ts_ns = row.get("ts_ns")
            if not ts_ns:
                continue
            frame = row.get("frame", {})
            if frame.get("type") != "next":
                continue
            ms = (
                frame.get("payload", {})
                .get("data", {})
                .get("sportsMatchStateUpdatedV2", {})
                .get("matchSummary", {})
            )
            if not ms:
                continue
            home_str = ms.get("homeScore", "0")
            away_str = ms.get("awayScore", "0")
            try:
                home = int(home_str)
                away = int(away_str)
            except (ValueError, TypeError):
                continue
            total = home + away
            free_text = ""
            msd = ms.get("matchStatusDisplay")
            if isinstance(msd, list) and msd:
                free_text = str(msd[0].get("freeText", ""))

            # NRFI
            if first_inning:
                if total > 0 and "1st inning" in free_text.lower():
                    events.append(("nrfi_run", ts_ns))
                    first_inning = False
                elif free_text == "Break top 2 bottom 1" and total == 0:
                    events.append(("nrfi_inning_end", ts_ns))
                    first_inning = False

            # Total overs
            if total > prev_total:
                for n in range(prev_total + 1, total + 1):
                    events.append((f"total_over:{n}", ts_ns))
                prev_total = total

            # Game end
            if free_text == "Ended" and not game_ended:
                events.append(("game_end", ts_ns))
                game_ended = True

    return events


def parse_boltodds_events(jsonl_path: str) -> list[tuple[str, int]]:
    events: list[tuple[str, int]] = []
    prev_total = 0
    first_inning = True
    game_ended = False

    with open(jsonl_path) as f:
        for line in f:
            row = json.loads(line)
            ts_ns = row.get("ts_ns")
            if not ts_ns:
                continue
            frame = row.get("frame", {})
            state = frame.get("state")
            if not isinstance(state, dict):
                continue
            runs = state.get("runs", {})
            if not isinstance(runs, dict):
                continue
            try:
                runs_a = int(runs.get("A", 0))
                runs_b = int(runs.get("B", 0))
            except (ValueError, TypeError):
                continue
            total = runs_a + runs_b
            match_period = state.get("matchPeriod", [])
            period_detail = match_period[1] if isinstance(match_period, list) and len(match_period) > 1 else ""
            inning = state.get("inning")
            match_completed = bool(state.get("matchCompleted"))

            # NRFI
            if first_inning:
                if total > 0 and inning == 1:
                    events.append(("nrfi_run", ts_ns))
                    first_inning = False
                elif period_detail == "AT_END_1ST_INNING" and total == 0:
                    events.append(("nrfi_inning_end", ts_ns))
                    first_inning = False

            # Total overs
            if total > prev_total:
                for n in range(prev_total + 1, total + 1):
                    events.append((f"total_over:{n}", ts_ns))
                prev_total = total

            # Game end
            if (match_completed or period_detail == "MATCH_COMPLETED") and not game_ended:
                events.append(("game_end", ts_ns))
                game_ended = True

    return events


def format_timeline(events: list[tuple[str, int]], label: str) -> list[str]:
    lines = [f"  {label} ({len(events)} events):"]
    for event_type, ts_ns in events:
        lines.append(f"    {event_type:20s}  ts_ns={ts_ns}")
    return lines


def compare_timelines(v1: list[tuple[str, int]], bo: list[tuple[str, int]]) -> None:
    v1_map = {e: t for e, t in v1}
    bo_map = {e: t for e, t in bo}
    all_events = sorted(set(v1_map.keys()) | set(bo_map.keys()))

    print(f"\n{'Event':<22} {'V1 ts_ns':<22} {'BO ts_ns':<22} {'Delta (ms)':>12} {'Faster'}")
    print("-" * 90)
    for event in all_events:
        v1_ts = v1_map.get(event)
        bo_ts = bo_map.get(event)
        if v1_ts and bo_ts:
            delta_ms = (bo_ts - v1_ts) / 1_000_000
            faster = "V1" if delta_ms > 0 else "BO" if delta_ms < 0 else "TIE"
            print(f"{event:<22} {v1_ts:<22} {bo_ts:<22} {delta_ms:>+12.1f} {faster}")
        elif v1_ts:
            print(f"{event:<22} {v1_ts:<22} {'—':<22} {'—':>12} V1 only")
        else:
            print(f"{event:<22} {'—':<22} {bo_ts:<22} {'—':>12} BO only")


def main():
    ap = argparse.ArgumentParser(description="Parse baseball captures into event timelines")
    ap.add_argument("--game-dir", required=True, help="Directory with v1_raw.jsonl and boltodds_raw.jsonl")
    args = ap.parse_args()

    game_dir = Path(args.game_dir)
    v1_path = game_dir / "v1_raw.jsonl"
    bo_path = game_dir / "boltodds_raw.jsonl"

    if v1_path.exists():
        v1_events = parse_v1_events(str(v1_path))
        print("\n".join(format_timeline(v1_events, "Kalstrop V1")))
    else:
        v1_events = []
        print("  Kalstrop V1: no file")

    if bo_path.exists():
        bo_events = parse_boltodds_events(str(bo_path))
        print("\n".join(format_timeline(bo_events, "BoltOdds")))
    else:
        bo_events = []
        print("  BoltOdds: no file")

    if v1_events and bo_events:
        compare_timelines(v1_events, bo_events)


if __name__ == "__main__":
    main()
