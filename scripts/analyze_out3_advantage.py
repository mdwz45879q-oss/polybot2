#!/usr/bin/env python3
"""
Analyze latency advantage of BoltOdds out=3 signal vs Kalstrop V1 inning transitions.

BoltOdds reports pitch-by-pitch data including the out count (0→1→2→3). When out=3,
the half-inning is definitively over — but V1 only reports this later via freeText
transitions ("Break top 2 bottom 1", "Ended"). This script quantifies the timing
advantage for three betting scenarios:

1. NRFI (No Run First Inning): end of 1st inning with 0 runs
2. Game end (home wins): top of 9th+ ends with home leading
3. Game end (away wins): bottom of 9th+ ends with away leading

Usage:
    python scripts/analyze_out3_advantage.py --captures-dir captures/2026_05_21
"""

import argparse
import json
import statistics
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Out3Signal:
    """A BoltOdds out=3 event relevant to betting."""
    scenario: str       # "nrfi", "game_end_top9", "game_end_bot9"
    ts_ns: int
    inning: int
    top_of_inning: bool
    home_score: int
    away_score: int


@dataclass
class V1Signal:
    """A V1 inning transition or game end relevant to betting."""
    scenario: str       # "nrfi", "game_end"
    ts_ns: int
    home_score: int
    away_score: int
    free_text: str


@dataclass
class Measurement:
    """A paired measurement of the same event from both sources."""
    game_name: str
    scenario: str
    bo_ts_ns: int
    v1_ts_ns: int
    delta_ms: float     # positive = BO faster (BO arrived first)
    inning: int
    home_score: int
    away_score: int


def extract_bo_signals(jsonl_path: str) -> tuple[list[Out3Signal], list[str]]:
    """Extract out=3 signals from BoltOdds JSONL. Returns (signals, warnings)."""
    signals: list[Out3Signal] = []
    warnings: list[str] = []

    found_nrfi = False
    found_game_end = False
    # Track if we ever see inning=1 bottom to know if NRFI was possible
    saw_bot_1 = False
    # Track if game completed (to know if game-end was possible)
    game_completed = False

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

            out = state.get("out")
            inning = state.get("inning")
            top_of_inning = state.get("topOfInning")
            runs = state.get("runs", {})
            total_a = state.get("totalRunsForTeamA") or 0  # home
            total_b = state.get("totalRunsForTeamB") or 0  # away
            match_completed = bool(state.get("matchCompleted"))

            if match_completed:
                game_completed = True

            if inning == 1 and top_of_inning is False:
                saw_bot_1 = True

            # Only look at out=3 frames before matchCompleted
            if out != 3 or match_completed:
                continue

            total_runs = total_a + total_b

            # NRFI: 3rd out in bottom of 1st with 0 runs
            if not found_nrfi and inning == 1 and top_of_inning is False and total_runs == 0:
                signals.append(Out3Signal(
                    scenario="nrfi",
                    ts_ns=ts_ns,
                    inning=inning,
                    top_of_inning=top_of_inning,
                    home_score=total_a,
                    away_score=total_b,
                ))
                found_nrfi = True

            # Game end: top of 9th+ ends with home leading
            if not found_game_end and inning >= 9 and top_of_inning is True and total_a > total_b:
                signals.append(Out3Signal(
                    scenario="game_end_top9",
                    ts_ns=ts_ns,
                    inning=inning,
                    top_of_inning=top_of_inning,
                    home_score=total_a,
                    away_score=total_b,
                ))
                found_game_end = True

            # Game end: bottom of 9th+ ends with away leading
            if not found_game_end and inning >= 9 and top_of_inning is False and total_b > total_a:
                signals.append(Out3Signal(
                    scenario="game_end_bot9",
                    ts_ns=ts_ns,
                    inning=inning,
                    top_of_inning=top_of_inning,
                    home_score=total_a,
                    away_score=total_b,
                ))
                found_game_end = True

    # Generate warnings for missing signals
    if saw_bot_1 and not found_nrfi:
        # Check if runs were scored (NRFI busted) vs missing frame
        # Re-scan to check if there's a frame with inning=1, bot, runs>0
        nrfi_busted = False
        with open(jsonl_path) as f:
            for line in f:
                row = json.loads(line)
                frame = row.get("frame", {})
                state = frame.get("state")
                if not isinstance(state, dict):
                    continue
                if state.get("inning") == 1 and state.get("topOfInning") is False:
                    ta = state.get("totalRunsForTeamA") or 0
                    tb = state.get("totalRunsForTeamB") or 0
                    if ta + tb > 0:
                        nrfi_busted = True
                        break
        if not nrfi_busted:
            warnings.append("no out=3 frame for bottom of 1st (NRFI detection gap)")

    if game_completed and not found_game_end:
        # Check if it was a walkoff (no 3rd-out game end expected)
        pass  # Walkoffs are expected to have no out=3 game-end signal

    return signals, warnings


def extract_v1_signals(jsonl_path: str) -> list[V1Signal]:
    """Extract inning transition and game-end signals from V1 JSONL."""
    signals: list[V1Signal] = []
    found_nrfi = False
    found_ended = False

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
            msd = ms.get("matchStatusDisplay")
            free_text = ""
            if isinstance(msd, list) and msd:
                free_text = str(msd[0].get("freeText", ""))

            # NRFI: "Break top 2 bottom 1" means full 1st inning completed
            if not found_nrfi and free_text == "Break top 2 bottom 1" and home + away == 0:
                signals.append(V1Signal(
                    scenario="nrfi",
                    ts_ns=ts_ns,
                    home_score=home,
                    away_score=away,
                    free_text=free_text,
                ))
                found_nrfi = True

            # Game end
            if not found_ended and free_text == "Ended":
                signals.append(V1Signal(
                    scenario="game_end",
                    ts_ns=ts_ns,
                    home_score=home,
                    away_score=away,
                    free_text=free_text,
                ))
                found_ended = True

    return signals


def match_signals(
    bo_signals: list[Out3Signal],
    v1_signals: list[V1Signal],
    game_name: str,
) -> list[Measurement]:
    """Match BO and V1 signals into latency measurements."""
    measurements: list[Measurement] = []

    bo_nrfi = next((s for s in bo_signals if s.scenario == "nrfi"), None)
    v1_nrfi = next((s for s in v1_signals if s.scenario == "nrfi"), None)

    if bo_nrfi and v1_nrfi:
        delta_ms = (v1_nrfi.ts_ns - bo_nrfi.ts_ns) / 1_000_000
        measurements.append(Measurement(
            game_name=game_name,
            scenario="nrfi",
            bo_ts_ns=bo_nrfi.ts_ns,
            v1_ts_ns=v1_nrfi.ts_ns,
            delta_ms=delta_ms,
            inning=bo_nrfi.inning,
            home_score=bo_nrfi.home_score,
            away_score=bo_nrfi.away_score,
        ))

    # Game end: match either top9 or bot9 BO signal with V1 "Ended"
    bo_game_end = next(
        (s for s in bo_signals if s.scenario in ("game_end_top9", "game_end_bot9")),
        None,
    )
    v1_game_end = next((s for s in v1_signals if s.scenario == "game_end"), None)

    if bo_game_end and v1_game_end:
        delta_ms = (v1_game_end.ts_ns - bo_game_end.ts_ns) / 1_000_000
        measurements.append(Measurement(
            game_name=game_name,
            scenario=bo_game_end.scenario,
            bo_ts_ns=bo_game_end.ts_ns,
            v1_ts_ns=v1_game_end.ts_ns,
            delta_ms=delta_ms,
            inning=bo_game_end.inning,
            home_score=bo_game_end.home_score,
            away_score=bo_game_end.away_score,
        ))

    return measurements


def discover_game_dirs(captures_dir: str) -> list[tuple[str, Path]]:
    """Find all game directories with both v1_raw.jsonl and boltodds_raw.jsonl."""
    base = Path(captures_dir)
    dirs: list[tuple[str, Path]] = []
    for d in sorted(base.iterdir()):
        if not d.is_dir():
            continue
        v1 = d / "v1_raw.jsonl"
        bo = d / "boltodds_raw.jsonl"
        if v1.exists() and bo.exists():
            dirs.append((d.name, d))
    return dirs


def print_scenario_section(
    measurements: list[Measurement],
    scenario_filter: list[str],
    title: str,
) -> None:
    """Print results for a specific scenario group."""
    filtered = [m for m in measurements if m.scenario in scenario_filter]

    print(f"\n--- {title} ---\n")
    if not filtered:
        print("  (No measurements in this dataset)\n")
        return

    for m in filtered:
        half = "top" if "top" in m.scenario else "bot"
        print(f"  {m.game_name}:")
        print(f"    BO out=3 @ inn={m.inning} {half} (H={m.home_score}, A={m.away_score}):")
        print(f"      ts_ns = {m.bo_ts_ns}")
        if "nrfi" in m.scenario:
            print(f"    V1 \"Break top 2 bottom 1\" ({m.home_score}-{m.away_score}):")
        else:
            print(f"    V1 \"Ended\" ({m.home_score}-{m.away_score}):")
        print(f"      ts_ns = {m.v1_ts_ns}")
        print(f"    Delta: {m.delta_ms:+.1f} ms ({m.delta_ms/1000:+.2f} s) — {'BO faster' if m.delta_ms > 0 else 'V1 faster'}")
        print()

    deltas = [m.delta_ms for m in filtered]
    print(f"  Summary ({len(deltas)} measurement{'s' if len(deltas) != 1 else ''}):")
    print(f"    Mean:   {statistics.mean(deltas)/1000:+.2f} s")
    if len(deltas) > 1:
        print(f"    Median: {statistics.median(deltas)/1000:+.2f} s")
    print(f"    Min:    {min(deltas)/1000:+.2f} s")
    print(f"    Max:    {max(deltas)/1000:+.2f} s")
    print()


def print_aggregate(measurements: list[Measurement]) -> None:
    """Print overall aggregate statistics."""
    if not measurements:
        print("\nNo measurements to aggregate.")
        return

    deltas = [m.delta_ms for m in measurements]
    all_bo_faster = all(d > 0 for d in deltas)

    print("=" * 70)
    print("AGGREGATE ACROSS ALL SCENARIOS")
    print("=" * 70)
    print(f"\n  Total measurements: {len(deltas)}")
    print(f"  Mean advantage:     {statistics.mean(deltas)/1000:+.2f} s")
    if len(deltas) > 1:
        print(f"  Median advantage:   {statistics.median(deltas)/1000:+.2f} s")
    print(f"  Range:              {min(deltas)/1000:.2f} s - {max(deltas)/1000:.2f} s")
    print(f"  All BO faster:      {'Yes' if all_bo_faster else 'No'}")
    print()


def main():
    ap = argparse.ArgumentParser(
        description="Analyze BoltOdds out=3 latency advantage vs Kalstrop V1"
    )
    ap.add_argument(
        "--captures-dir",
        required=True,
        help="Directory containing game subdirectories with v1_raw.jsonl + boltodds_raw.jsonl",
    )
    args = ap.parse_args()

    game_dirs = discover_game_dirs(args.captures_dir)
    if not game_dirs:
        print("No game directories found with both v1_raw.jsonl and boltodds_raw.jsonl.")
        return

    print("=" * 70)
    print("OUT=3 LATENCY ADVANTAGE ANALYSIS")
    print("BoltOdds out=3 signal vs Kalstrop V1 inning transitions")
    print(f"{len(game_dirs)} games from {Path(args.captures_dir).name}")
    print("=" * 70)

    all_measurements: list[Measurement] = []
    all_warnings: list[tuple[str, str]] = []

    for game_name, game_dir in game_dirs:
        bo_path = game_dir / "boltodds_raw.jsonl"
        v1_path = game_dir / "v1_raw.jsonl"

        bo_signals, warnings = extract_bo_signals(str(bo_path))
        v1_signals = extract_v1_signals(str(v1_path))

        measurements = match_signals(bo_signals, v1_signals, game_name)
        all_measurements.extend(measurements)

        for w in warnings:
            all_warnings.append((game_name, w))

    # Print per-scenario results
    print_scenario_section(all_measurements, ["nrfi"], "NRFI (No Run First Inning)")
    print_scenario_section(all_measurements, ["game_end_top9"], "Game End (home wins, top 9+ outs)")
    print_scenario_section(all_measurements, ["game_end_bot9"], "Game End (away wins, bottom 9+ outs)")

    # Print aggregate
    print_aggregate(all_measurements)

    # Print warnings
    if all_warnings:
        print("WARNINGS:")
        for game_name, warning in all_warnings:
            print(f"  - {game_name}: {warning}")
        print()


if __name__ == "__main__":
    main()
