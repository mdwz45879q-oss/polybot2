"""CLI argument builders for polybot2."""

from __future__ import annotations

import argparse


def add_subcommands(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:  # type: ignore[type-arg]
    data_p = sub.add_parser("data", help="Data ingestion commands")
    data_sub = data_p.add_subparsers(dest="data_command", required=True)
    data_sync = data_sub.add_parser("sync", help="Sync Polymarket metadata")
    data_sync.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    data_sync.add_argument("--markets", action="store_true", dest="sync_markets", help="Sync market metadata")
    data_sync.add_argument("--batch-size", type=int, default=None)
    data_sync.add_argument("--concurrency", type=int, default=None)
    data_sync.add_argument("--max-rps", type=int, default=None)
    data_sync.add_argument("--resolved-max-pages", type=int, default=None)
    data_sync.add_argument("--open-max-pages", type=int, default=None)
    data_sync.add_argument("--open-only", action="store_true", default=False)
    data_sync.add_argument("--skip-reference-sync", action="store_true", default=False)
    data_sync.add_argument("--enable-payload-artifacts", action="store_true", default=False)
    data_sync.add_argument("--disable-payload-artifacts", action="store_true", default=False)
    data_sync.add_argument("--fast-mode", action="store_true", default=False)

    data_bench = data_sub.add_parser("benchmark-markets", help="Benchmark market sync for concurrency/rate tuning")
    data_bench.add_argument("--concurrency-values", type=str, default="20,30,40,60")
    data_bench.add_argument("--max-rps-values", type=str, default="24,48,96,160,220")
    data_bench.add_argument("--repeats", type=int, default=3)
    data_bench.add_argument("--batch-size", type=int, default=500)
    data_bench.add_argument("--resolved-max-pages", type=int, default=20)
    data_bench.add_argument("--open-max-pages", type=int, default=20)
    data_bench.add_argument("--request-delay", type=float, default=0.0)
    data_bench.add_argument("--fetch-max-retries", type=int, default=3)
    data_bench.add_argument("--output-dir", type=str, default="artifacts/polybot2_market_benchmarks")
    data_bench.add_argument("--enable-payload-artifacts", action="store_true", default=False)
    data_bench.add_argument("--skip-reference-sync", action="store_true", default=False)
    data_bench.add_argument("--disable-payload-artifacts", action="store_true", default=False)
    data_bench.add_argument("--fast-mode", action="store_true", default=False)

    provider_p = sub.add_parser("provider", help="Provider catalog commands")
    provider_sub = provider_p.add_subparsers(dest="provider_command", required=True)
    provider_sync = provider_sub.add_parser("sync", help="Sync provider games")
    provider_sync.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    provider_sync.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")
    provider_capture = provider_sub.add_parser("capture", help="Record raw score frames to disk")
    provider_capture.add_argument("--provider", type=str, choices=["kalstrop"], default="kalstrop")
    provider_capture.add_argument("--league", type=str, required=True)
    provider_capture.add_argument("--out", type=str, required=True, help="Output directory")
    capture_sel = provider_capture.add_mutually_exclusive_group(required=True)
    capture_sel.add_argument("--universal-id", type=str, action="append", dest="universal_ids",
                             help="Fixture ID (repeatable)")
    capture_sel.add_argument("--today", action="store_true", default=False,
                             help="Capture all of today's games (Eastern Time)")
    provider_capture.add_argument("--date-et", type=str, default="",
                                  help="Override date for --today (YYYY-MM-DD, Eastern Time)")
    provider_capture.add_argument("--max-duration-seconds", type=float, default=21600.0,
                                  help="Auto-stop after N seconds (default: 6h)")

    mapping_p = sub.add_parser("mapping", help="Mapping validation commands")
    mapping_sub = mapping_p.add_subparsers(dest="mapping_command", required=True)
    mapping_validate = mapping_sub.add_parser("validate", help="Validate mappings.py")

    link_p = sub.add_parser("link", help="Deterministic linking commands")
    link_sub = link_p.add_subparsers(dest="link_command", required=True)

    link_build = link_sub.add_parser("build", help="Build provider->Polymarket deterministic links")
    link_build.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    link_build.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")
    link_build.add_argument("--league-scope", type=str, choices=["live", "all"], default="live")

    link_report = link_sub.add_parser("report", help="Show link quality report")
    link_report.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    link_report.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")

    link_review = link_sub.add_parser("review", help="Review provider->Polymarket link mappings")
    link_review_sub = link_review.add_subparsers(dest="link_review_command", required=True)

    link_review_card = link_review_sub.add_parser("card", help="Show review card for one provider game")
    link_review_card.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    link_review_card.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")
    link_review_card.add_argument("--run-id", type=int, required=True)
    link_review_card.add_argument("--provider-game-id", type=str, required=True)
    link_review_card.add_argument("--format", type=str, choices=["table", "json"], default="table")

    link_review_candidates = link_review_sub.add_parser("candidates", help="Show candidate comparison for one game")
    link_review_candidates.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    link_review_candidates.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")
    link_review_candidates.add_argument("--run-id", type=int, required=True)
    link_review_candidates.add_argument("--provider-game-id", type=str, required=True)
    link_review_candidates.add_argument("--format", type=str, choices=["table", "json"], default="table")

    link_review_decide = link_review_sub.add_parser("decide", help="Record review decision")
    link_review_decide.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    link_review_decide.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")
    link_review_decide.add_argument("--run-id", type=int, required=True)
    link_review_decide.add_argument("--provider-game-id", type=str, required=True)
    link_review_decide.add_argument("--decision", type=str, choices=["approve", "reject", "skip"], required=True)
    link_review_decide.add_argument("--note", type=str, default="")
    link_review_decide.add_argument("--actor", type=str, default="cli")
    link_review_decide.add_argument("--format", type=str, choices=["table", "json"], default="table")

    link_review_session = link_review_sub.add_parser("session", help="Interactive operator review session")
    link_review_session.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    link_review_session.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")
    link_review_session.add_argument("--run-id", type=int, required=True)
    link_review_session.add_argument(
        "--scope",
        type=str,
        choices=["all", "mapped_pending", "mapped", "unresolved"],
        default="mapped_pending",
    )
    link_review_session.add_argument("--decision", type=str, default="")
    link_review_session.add_argument("--resolution", type=str, default="")
    link_review_session.add_argument("--parse-status", type=str, default="ok")
    link_review_session.add_argument("--limit", type=int, default=500)
    link_review_session.add_argument("--include-inactive", action="store_true", default=False)

    hotpath_p = sub.add_parser("hotpath", help="Low-latency hotpath runtime")
    hotpath_sub = hotpath_p.add_subparsers(dest="hotpath_command", required=True)
    hotpath_run = hotpath_sub.add_parser("run", help="Run hotpath runtime")
    hotpath_run.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    hotpath_run.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")
    hotpath_run.add_argument("--league", type=str, required=True, help="Canonical league key (e.g. mlb)")
    hotpath_run.add_argument("--scores-only", action="store_true", default=False)
    hotpath_run.add_argument("--read-timeout-seconds", type=float, default=0.05)
    hotpath_run.add_argument("--profile-latency", action="store_true", default=False)
    hotpath_run.add_argument(
        "--execution-mode",
        type=str,
        choices=["live", "paper"],
        default="live",
        help="Execution mode for hotpath run (paper mode is non-trading/noop dispatch).",
    )
    run_group = hotpath_run.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--link-run-id", type=int, default=None)
    run_group.add_argument("--approve-link-run", type=int, default=None, help="Deprecated alias for --link-run-id")
    hotpath_run.add_argument("--force-launch", action="store_true", default=False)
    hotpath_run.add_argument("--with-observe", action="store_true", default=False)

    hotpath_replay = hotpath_sub.add_parser("replay", help="Replay captured score stream through hotpath triggers")
    hotpath_replay.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    hotpath_replay.add_argument("--provider", type=str, choices=["boltodds", "kalstrop"], default="")
    hotpath_replay.add_argument("--league", type=str, required=True, help="Canonical league key (v1 supports mlb)")
    hotpath_replay.add_argument("--link-run-id", type=int, required=True)
    hotpath_replay.add_argument("--capture-manifest", type=str, required=True)
    hotpath_replay.add_argument("--universal-id", type=str, action="append", dest="universal_ids")
    hotpath_replay.add_argument("--mode", type=str, choices=["as_fast", "timed"], default="as_fast")
    hotpath_replay.add_argument("--speed-multiplier", type=float, default=1.0)
    hotpath_replay.add_argument("--out", type=str, default="")
    hotpath_replay.add_argument("--format", type=str, choices=["table", "json"], default="table")

    hotpath_observe = hotpath_sub.add_parser("observe", help="Live terminal scoreboard (reads JSONL log file)")
    hotpath_observe.add_argument("--log-file", type=str, default="", help="Path to hotpath JSONL log file (auto-discovers latest if omitted)")
    hotpath_observe.add_argument("--log-dir", type=str, default="", help="Directory to search for log files")
    hotpath_observe.add_argument("--run-id", type=int, default=None, help="Filter log files by run ID")
    hotpath_observe.add_argument("--db", type=str, default="")
    hotpath_observe.add_argument("--league", type=str, default="mlb")
    hotpath_observe.add_argument("--link-run-id", type=int, default=None)
