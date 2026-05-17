"""CLI argument builders for polybot2."""

from __future__ import annotations

import argparse


def add_subcommands(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:  # type: ignore[type-arg]
    market_p = sub.add_parser("market", help="Polymarket data commands")
    market_sub = market_p.add_subparsers(dest="market_command", required=True)
    market_sync = market_sub.add_parser("sync", help="Sync Polymarket market metadata")
    market_sync.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    market_sync.add_argument("--all", action="store_true", default=False, help="Include resolved/closed markets (default: open only)")
    market_sync.add_argument("--batch-size", type=int, default=None)
    market_sync.add_argument("--concurrency", type=int, default=None)
    market_sync.add_argument("--max-rps", type=int, default=None)
    market_sync.add_argument("--open-max-pages", type=int, default=None)
    market_sync.add_argument("--fast-mode", action="store_true", default=False)

    provider_p = sub.add_parser("provider", help="Provider catalog commands")
    provider_sub = provider_p.add_subparsers(dest="provider_command", required=True)
    provider_sync = provider_sub.add_parser("sync", help="Sync provider games")
    provider_sync.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    provider_sync.add_argument("--provider", type=str, choices=["boltodds", "kalstrop_v1", "kalstrop_v2", "kalstrop_opta"], default="")
    link_p = sub.add_parser("link", help="Deterministic linking commands")
    link_sub = link_p.add_subparsers(dest="link_command", required=True)

    link_build = link_sub.add_parser("build", help="Build provider->Polymarket deterministic links")
    link_build.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    link_build.add_argument("--league-scope", type=str, choices=["live", "all"], default="live")
    link_build.add_argument(
        "--horizon-hours", type=float, default=None,
        help="Only link games starting within this many hours from now "
             "(default: per-league plan_horizon_hours from config)",
    )

    link_review = link_sub.add_parser("review", help="Interactive link review session")
    link_review.add_argument("--db", type=str, default="", help="Override SQLite DB path")
    link_review.add_argument("--run-id", type=int, required=True)
    link_review.add_argument(
        "--scope",
        type=str,
        choices=["all", "mapped_pending", "mapped", "unresolved"],
        default="mapped_pending",
    )
    link_review.add_argument("--decision", type=str, default="")
    link_review.add_argument("--resolution", type=str, default="")
    link_review.add_argument("--parse-status", type=str, default="ok")
    link_review.add_argument("--limit", type=int, default=500)
    link_review.add_argument("--include-inactive", action="store_true", default=False)

    hotpath_p = sub.add_parser("hotpath", help="Low-latency hotpath runtime")
    hotpath_sub = hotpath_p.add_subparsers(dest="hotpath_command", required=True)

    hotpath_live = hotpath_sub.add_parser("live", help="Run hotpath with periodic plan refresh")
    hotpath_live.add_argument("--league", type=str, nargs="+", default=None,
                              help="One or more league keys (e.g., epl laliga ucl)")
    hotpath_live.add_argument("--sport", type=str, default=None,
                              choices=["soccer", "baseball"],
                              help="Run all live leagues for a sport")
    hotpath_live.add_argument("--link-run-id", type=int, default=None,
                              help="Link run ID (default: latest for the league)")
    hotpath_live.add_argument("--execution-mode", type=str, choices=["live", "paper"], required=True)
    hotpath_live.add_argument("--refresh-interval", type=int, default=None,
                              help="Seconds between plan refreshes (default: from config/live_trading.py, or 300)")
    hotpath_live.add_argument("--db", type=str, default="")

    hotpath_observe = hotpath_sub.add_parser("observe", help="Live terminal scoreboard (reads JSONL log file)")
    hotpath_observe.add_argument("--log-file", type=str, default="", help="Path to hotpath JSONL log file (auto-discovers latest if omitted)")
    hotpath_observe.add_argument("--log-dir", type=str, default="", help="Directory to search for log files")
    hotpath_observe.add_argument("--run-id", type=int, default=None, help="Filter log files by run ID")
    hotpath_observe.add_argument("--db", type=str, default="")
    hotpath_observe.add_argument("--league", type=str, default="mlb")
    hotpath_observe.add_argument("--link-run-id", type=int, default=None)

    hotpath_compile = hotpath_sub.add_parser("compile", help="Compile hotpath plan and print summary (dry run)")
    hotpath_compile.add_argument("--league", type=str, required=True)
    hotpath_compile.add_argument("--link-run-id", type=int, default=None,
                                  help="Link run ID (default: latest for the league)")
    hotpath_compile.add_argument("--db", type=str, default="", help="Override SQLite DB path")
