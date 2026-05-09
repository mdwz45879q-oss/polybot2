from __future__ import annotations

import pytest

from polybot2._cli.parser import build_parser


def test_cli_parser_smoke() -> None:
    parser = build_parser()

    # market sync
    market_sync_args = parser.parse_args(["market", "sync"])
    assert market_sync_args.command == "market"
    assert market_sync_args.market_command == "sync"

    market_sync_all_args = parser.parse_args(["market", "sync", "--all", "--fast-mode"])
    assert bool(getattr(market_sync_all_args, "all", False)) is True
    assert bool(market_sync_all_args.fast_mode) is True

    # provider sync
    provider_sync_args = parser.parse_args(["provider", "sync", "--provider", "kalstrop_v1"])
    assert provider_sync_args.command == "provider"
    assert provider_sync_args.provider_command == "sync"
    assert provider_sync_args.provider == "kalstrop_v1"

    # link build (no --provider; provider comes from league config)
    link_build_args = parser.parse_args(["link", "build"])
    assert link_build_args.command == "link"
    assert link_build_args.link_command == "build"

    link_build_scope_args = parser.parse_args(["link", "build", "--league-scope", "all"])
    assert link_build_scope_args.league_scope == "all"

    # --auto-approve was removed
    with pytest.raises(SystemExit):
        parser.parse_args(["link", "build", "--auto-approve"])

    # link review (flat — no sub-subcommand, no --provider; provider from run_id lookup)
    link_review_args = parser.parse_args(["link", "review", "--run-id", "42", "--scope", "all"])
    assert link_review_args.command == "link"
    assert link_review_args.link_command == "review"
    assert link_review_args.run_id == 42
    assert link_review_args.scope == "all"

    # hotpath live (no --provider; provider from league config)
    hotpath_live_args = parser.parse_args(
        ["hotpath", "live", "--league", "mlb", "--link-run-id", "1", "--execution-mode", "paper"]
    )
    assert hotpath_live_args.command == "hotpath"
    assert hotpath_live_args.hotpath_command == "live"
    assert hotpath_live_args.execution_mode == "paper"

    # hotpath observe
    hotpath_observe_args = parser.parse_args(["hotpath", "observe", "--run-id", "42"])
    assert hotpath_observe_args.command == "hotpath"
    assert hotpath_observe_args.hotpath_command == "observe"
    assert hotpath_observe_args.run_id == 42

    # removed commands should fail
    with pytest.raises(SystemExit):
        parser.parse_args(["mapping", "validate"])
    with pytest.raises(SystemExit):
        parser.parse_args(["data", "sync", "--markets"])
    with pytest.raises(SystemExit):
        parser.parse_args(["data", "benchmark-markets"])
    with pytest.raises(SystemExit):
        parser.parse_args(["hotpath", "run", "--league", "mlb", "--link-run-id", "1"])
    with pytest.raises(SystemExit):
        parser.parse_args(["link", "report"])
    with pytest.raises(SystemExit):
        parser.parse_args(["link", "review", "card", "--run-id", "1", "--provider-game-id", "x"])
    with pytest.raises(SystemExit):
        parser.parse_args(["provider", "capture", "--league", "mlb", "--out", "/tmp/cap", "--today"])
