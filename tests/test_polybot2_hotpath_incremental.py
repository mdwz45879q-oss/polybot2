"""Tests for incremental market discovery (hotpath/incremental.py)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.hotpath import compile_hotpath_plan
from polybot2.hotpath.incremental import (
    IncrementalRefreshResult,
    _build_event_to_game_map,
    _extract_new_targets,
    _extract_strategy_keys,
    _find_new_condition_ids,
    _gather_event_ids,
    _insert_new_market_targets,
    discover_new_markets,
)
from polybot2.linking import LinkReviewService, LinkService, load_mapping
from polybot2.linking.mapping_loader import load_live_trading_policy


_NOW_TS = 1_777_000_100
_KICKOFF_TS = 1_776_553_200

_BASE_EVENT = {
    "id": "evt_mlb",
    "title": "Philadelphia Phillies vs Atlanta Braves",
    "slug": "mlb-phi-atl-2026-04-18",
    "startTime": "2026-04-18T23:00:00Z",
    "teams": [
        {"id": 10, "name": "philadelphia phillies", "abbreviation": "phi"},
        {"id": 11, "name": "atlanta braves", "abbreviation": "atl"},
    ],
    "markets": [
        {
            "id": "mkt_moneyline",
            "conditionId": "cond_moneyline",
            "question": "Who will win?",
            "slug": "mlb-phi-atl-moneyline",
            "sportsMarketType": "moneyline",
            "line": None,
            "closed": False,
            "resolved": False,
            "volume": 1000,
            "outcomes": ["Atlanta Braves", "Philadelphia Phillies"],
            "clobTokenIds": ["tok_ml_home", "tok_ml_away"],
        },
        {
            "id": "mkt_tot_8_5",
            "conditionId": "cond_tot_8_5",
            "question": "O/U 8.5",
            "slug": "mlb-phi-atl-ou-8.5",
            "sportsMarketType": "totals",
            "line": 8.5,
            "closed": False,
            "resolved": False,
            "volume": 1000,
            "outcomes": ["Over", "Under"],
            "clobTokenIds": ["tok_over_8_5", "tok_under_8_5"],
        },
        {
            "id": "mkt_nrfi",
            "conditionId": "cond_nrfi",
            "question": "NRFI?",
            "slug": "mlb-phi-atl-nrfi",
            "sportsMarketType": "nrfi",
            "line": None,
            "closed": False,
            "resolved": False,
            "volume": 1000,
            "outcomes": ["Yes", "No"],
            "clobTokenIds": ["tok_nrfi_yes", "tok_nrfi_no"],
        },
    ],
}

_PROVIDER_ROW = (
    "kalstrop_v1", "gid_mlb",
    "Atlanta Braves vs Philadelphia Phillies, 2026-04-18", "",
    "baseball", "Major League Baseball",
    "", "",
    "2026-04-18, 07:00 PM",
    _KICKOFF_TS, "2026-04-18",
    "Atlanta Braves", "Philadelphia Phillies",
    "ok", "", _NOW_TS,
)


def _seed(runtime: DataRuntimeConfig, event_data: dict[str, Any] | None = None) -> int:
    """Seed DB with one MLB game and return the link run_id."""
    event = dict(event_data or _BASE_EVENT)
    mapping = load_mapping()
    with open_database(runtime) as db:
        db.markets.upsert_from_gamma_events(
            events_data=[event], updated_ts=_NOW_TS,
        )
        db.linking.upsert_provider_games([_PROVIDER_ROW])
        result = LinkService(db=db).build_links(
            provider="kalstrop_v1", mapping=mapping, league_scope="all",
        )
        LinkReviewService(db=db).record_decision(
            provider="kalstrop_v1", run_id=int(result.run_id),
            provider_game_id="gid_mlb", decision="approve", actor="test",
        )
    return int(result.run_id)


def _compile(runtime: DataRuntimeConfig, run_id: int):
    with open_database(runtime) as db:
        return compile_hotpath_plan(
            db=db, provider="kalstrop_v1", league="mlb", run_id=run_id,
            now_ts_utc=_KICKOFF_TS - 3600,
        )


def _add_market_to_event(event: dict, *, cond_id: str, market_type: str, line: float | None = None):
    """Return a copy of the event with an extra market appended."""
    event = dict(event)
    event["markets"] = list(event["markets"]) + [
        {
            "id": f"mkt_{cond_id}",
            "conditionId": cond_id,
            "question": f"New market {cond_id}",
            "slug": f"mlb-phi-atl-{cond_id}",
            "sportsMarketType": market_type,
            "line": line,
            "closed": False,
            "resolved": False,
            "volume": 0,
            "outcomes": ["Over", "Under"] if market_type == "totals" else ["Yes", "No"],
            "clobTokenIds": [f"tok_{cond_id}_0", f"tok_{cond_id}_1"],
        }
    ]
    return event


async def _run_discover(
    runtime: DataRuntimeConfig,
    run_id: int,
    current_plan,
    gamma_response: list[dict],
):
    """Run discover_new_markets with a mocked Gamma API response."""
    with open_database(runtime) as db:
        with patch(
            "polybot2.hotpath.incremental._fetch_events_by_ids",
            new_callable=AsyncMock,
            return_value=gamma_response,
        ):
            return await discover_new_markets(
                current_plan=current_plan,
                db=db,
                now_ts_utc=_KICKOFF_TS - 3600,
            )


# ---------------------------------------------------------------
# Tests
# ---------------------------------------------------------------


def test_no_new_markets(tmp_path: Path) -> None:
    """Gamma returns exactly the same markets — no delta."""
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed(runtime)
    plan = _compile(runtime, run_id)
    assert len(plan.games) == 1

    result = asyncio.run(_run_discover(runtime, run_id, plan, [_BASE_EVENT]))
    assert result.new_plan is None
    assert result.new_targets == ()
    assert result.markets_discovered == 0


def test_one_new_totals_market(tmp_path: Path) -> None:
    """A new totals line appears mid-game — should show up in delta."""
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed(runtime)
    plan = _compile(runtime, run_id)
    old_keys = _extract_strategy_keys(plan)

    updated_event = _add_market_to_event(
        _BASE_EVENT, cond_id="cond_tot_11_5", market_type="totals", line=11.5,
    )
    result = asyncio.run(_run_discover(runtime, run_id, plan, [updated_event]))

    assert result.new_plan is not None
    assert len(result.new_targets) > 0
    assert "cond_tot_11_5" in result.new_condition_ids
    assert result.targets_inserted > 0
    new_keys = _extract_strategy_keys(result.new_plan)
    assert new_keys > old_keys


def test_new_market_outside_allowed_types(tmp_path: Path) -> None:
    """New market has a type not in LIVE_BETTING_MARKET_TYPES — ignored."""
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed(runtime)
    plan = _compile(runtime, run_id)

    updated_event = _add_market_to_event(
        _BASE_EVENT, cond_id="cond_exotic", market_type="player_props",
    )
    result = asyncio.run(_run_discover(runtime, run_id, plan, [updated_event]))

    assert result.new_plan is None
    assert result.new_targets == ()
    assert result.targets_inserted == 0


def test_new_market_already_resolved(tmp_path: Path) -> None:
    """New market exists but is already resolved — ignored."""
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed(runtime)
    plan = _compile(runtime, run_id)

    resolved_event = _add_market_to_event(
        _BASE_EVENT, cond_id="cond_resolved_tot", market_type="totals", line=12.5,
    )
    resolved_event["markets"][-1]["resolved"] = True
    result = asyncio.run(_run_discover(runtime, run_id, plan, [resolved_event]))

    assert result.new_plan is None
    assert result.new_targets == ()


def test_extract_helpers() -> None:
    """Sanity-check the pure extraction helpers."""
    from polybot2.hotpath.contracts import CompiledGamePlan, CompiledMarket, CompiledPlan, CompiledTarget

    t1 = CompiledTarget(
        condition_id="c1", outcome_index=0, token_id="tok1",
        sports_market_type="totals", line=8.5, outcome_label="Over",
        outcome_semantic="over", strategy_key="gid:TOTAL:OVER:8.5",
    )
    t2 = CompiledTarget(
        condition_id="c2", outcome_index=0, token_id="tok2",
        sports_market_type="totals", line=9.5, outcome_label="Over",
        outcome_semantic="over", strategy_key="gid:TOTAL:OVER:9.5",
    )
    m1 = CompiledMarket(
        condition_id="c1", market_id="m1", event_id="e1",
        sports_market_type="totals", line=8.5, question="O/U 8.5",
        targets=(t1,),
    )
    m2 = CompiledMarket(
        condition_id="c2", market_id="m2", event_id="e1",
        sports_market_type="totals", line=9.5, question="O/U 9.5",
        targets=(t2,),
    )
    g = CompiledGamePlan(
        provider_game_id="gid", canonical_league="mlb",
        canonical_home_team="ATL", canonical_away_team="PHI",
        kickoff_ts_utc=None, markets=(m1,),
    )
    old_plan = CompiledPlan(
        provider="kalstrop_v1", league="mlb", run_id=1,
        plan_hash="h1", compiled_at=0, games=(g,),
    )
    g_new = CompiledGamePlan(
        provider_game_id="gid", canonical_league="mlb",
        canonical_home_team="ATL", canonical_away_team="PHI",
        kickoff_ts_utc=None, markets=(m1, m2),
    )
    new_plan = CompiledPlan(
        provider="kalstrop_v1", league="mlb", run_id=1,
        plan_hash="h2", compiled_at=0, games=(g_new,),
    )
    assert _extract_strategy_keys(old_plan) == {"gid:TOTAL:OVER:8.5"}
    delta = _extract_new_targets(old_plan, new_plan)
    assert len(delta) == 1
    assert delta[0].strategy_key == "gid:TOTAL:OVER:9.5"
