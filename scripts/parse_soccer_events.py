#!/usr/bin/env python3
"""
Parse soccer capture files and extract betting-relevant event timelines.

Reads V1, V2, and BoltOdds JSONL files for a soccer game and outputs normalized
event lists with nanosecond timestamps for latency comparison.

Usage:
    python scripts/parse_soccer_events.py --game-dir captures/2026_05_04/chelsea_nottingham
"""

import argparse
import json
from pathlib import Path


def parse_v1_soccer_events(jsonl_path: str) -> list[tuple[str, int]]:
    events: list[tuple[str, int]] = []
    emitted: set[str] = set()
    prev_goals = 0
    prev_corners = 0

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

            try:
                home = int(ms.get("homeScore", 0))
                away = int(ms.get("awayScore", 0))
            except (ValueError, TypeError):
                continue
            total_goals = home + away

            free_text = ""
            msd = ms.get("matchStatusDisplay")
            if isinstance(msd, list) and msd:
                free_text = str(msd[0].get("freeText", ""))

            stats = ms.get("statistics")
            total_corners = 0
            if isinstance(stats, dict):
                corners = stats.get("corners")
                if isinstance(corners, dict):
                    try:
                        total_corners = int(corners.get("home", 0)) + int(corners.get("away", 0))
                    except (ValueError, TypeError):
                        pass

            # Total overs (goals)
            if total_goals > prev_goals:
                for n in range(prev_goals + 1, total_goals + 1):
                    key = f"total_over:{n}"
                    if key not in emitted:
                        events.append((key, ts_ns))
                        emitted.add(key)
                prev_goals = total_goals

            # Corners
            if total_corners > prev_corners:
                for n in range(prev_corners + 1, total_corners + 1):
                    key = f"corner:{n}"
                    if key not in emitted:
                        events.append((key, ts_ns))
                        emitted.add(key)
                prev_corners = total_corners

            # Halftime
            if free_text == "Halftime" and "halftime" not in emitted:
                events.append(("halftime", ts_ns))
                emitted.add("halftime")

            # Game end
            if free_text == "Ended" and "game_end" not in emitted:
                events.append(("game_end", ts_ns))
                emitted.add("game_end")

    return events


def parse_boltodds_soccer_events(jsonl_path: str) -> list[tuple[str, int]]:
    events: list[tuple[str, int]] = []
    emitted: set[str] = set()
    prev_goals = 0
    prev_corners = 0

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

            try:
                goals_a = int(state.get("goalsA", 0) or 0)
                goals_b = int(state.get("goalsB", 0) or 0)
            except (ValueError, TypeError):
                continue
            total_goals = goals_a + goals_b

            try:
                corners_a = int(state.get("cornersA", 0) or 0)
                corners_b = int(state.get("cornersB", 0) or 0)
            except (ValueError, TypeError):
                corners_a, corners_b = 0, 0
            total_corners = corners_a + corners_b

            match_period = state.get("matchPeriod", [])
            period_detail = match_period[1] if isinstance(match_period, list) and len(match_period) > 1 else ""

            # Total overs (goals)
            if total_goals > prev_goals:
                for n in range(prev_goals + 1, total_goals + 1):
                    key = f"total_over:{n}"
                    if key not in emitted:
                        events.append((key, ts_ns))
                        emitted.add(key)
                prev_goals = total_goals

            # Corners
            if total_corners > prev_corners:
                for n in range(prev_corners + 1, total_corners + 1):
                    key = f"corner:{n}"
                    if key not in emitted:
                        events.append((key, ts_ns))
                        emitted.add(key)
                prev_corners = total_corners

            # Halftime
            if period_detail == "AT_HALF_TIME" and "halftime" not in emitted:
                events.append(("halftime", ts_ns))
                emitted.add("halftime")

            # Game end
            if period_detail == "MATCH_COMPLETED" and "game_end" not in emitted:
                events.append(("game_end", ts_ns))
                emitted.add("game_end")

    return events


def parse_v2_soccer_events(jsonl_path: str) -> list[tuple[str, int]]:
    events: list[tuple[str, int]] = []
    emitted: set[str] = set()
    prev_goals = 0

    with open(jsonl_path) as f:
        for line in f:
            row = json.loads(line)
            ts_ns = row.get("ts_ns")
            if not ts_ns:
                continue
            outer_data = row.get("data", {})
            if not isinstance(outer_data, dict):
                continue
            inner = outer_data.get("data", {})
            if not isinstance(inner, dict):
                continue

            sb = inner.get("scoreboardInfo", {})
            if not isinstance(sb, dict):
                continue

            hs = sb.get("homeScore")
            aws = sb.get("awayScore")
            if hs is None or aws is None:
                continue
            try:
                home = int(hs)
                away = int(aws)
            except (ValueError, TypeError):
                continue
            total_goals = home + away

            phase = str(sb.get("currentPhase", ""))

            # V2 does not provide reliable corner data (matchActions is a
            # sliding window that misses corners between updates).

            # Total overs (goals)
            if total_goals > prev_goals:
                for n in range(prev_goals + 1, total_goals + 1):
                    key = f"total_over:{n}"
                    if key not in emitted:
                        events.append((key, ts_ns))
                        emitted.add(key)
                prev_goals = total_goals

            # Halftime
            if phase == "HalfTime" and "halftime" not in emitted:
                events.append(("halftime", ts_ns))
                emitted.add("halftime")

            # Game end
            if phase == "FullTimeNormalTime" and "game_end" not in emitted:
                events.append(("game_end", ts_ns))
                emitted.add("game_end")

    return events


def format_timeline(events: list[tuple[str, int]], label: str) -> list[str]:
    lines = [f"  {label} ({len(events)} events):"]
    for event_type, ts_ns in events:
        lines.append(f"    {event_type:20s}  ts_ns={ts_ns}")
    return lines


def main():
    ap = argparse.ArgumentParser(description="Parse soccer captures into event timelines")
    ap.add_argument("--game-dir", required=True, help="Directory with v1_raw.jsonl, v2_raw.jsonl, boltodds_raw.jsonl")
    args = ap.parse_args()

    game_dir = Path(args.game_dir)

    for filename, parser, label in [
        ("v1_raw.jsonl", parse_v1_soccer_events, "Kalstrop V1"),
        ("v2_raw.jsonl", parse_v2_soccer_events, "Kalstrop V2"),
        ("boltodds_raw.jsonl", parse_boltodds_soccer_events, "BoltOdds"),
    ]:
        path = game_dir / filename
        if path.exists():
            events = parser(str(path))
            print("\n".join(format_timeline(events, label)))
        else:
            print(f"  {label}: no file")


if __name__ == "__main__":
    main()
