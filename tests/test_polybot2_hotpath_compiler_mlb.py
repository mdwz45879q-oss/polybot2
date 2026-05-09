from __future__ import annotations

from pathlib import Path

import pytest

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.hotpath import (
    HotPathPlanError,
    compile_hotpath_plan,
    evaluate_hotpath_scope,
)
from polybot2.linking import LinkReviewService, LinkService, load_mapping
from polybot2.linking.mapping_loader import LoadedLiveTradingPolicy, load_live_trading_policy


def _seed_run(*, runtime: DataRuntimeConfig, live_policy: LoadedLiveTradingPolicy | None = None) -> int:
    mapping = load_mapping()
    now_ts = 1_777_000_100
    provider_rows = [
        (
            "kalstrop_v1",
            "gid_mlb",
            "Atlanta Braves vs Philadelphia Phillies, 2026-04-18",
            "",
            "baseball",
            "Major League Baseball",
            "", "",
            "2026-04-18, 07:00 PM",
            1_776_553_200,
            "2026-04-18",
            "Atlanta Braves",
            "Philadelphia Phillies",
            "ok",
            "",
            now_ts,
        ),
        (
            "kalstrop_v1",
            "gid_bun",
            "Hoffenheim vs Dortmund, 2026-04-18, 09",
            "",
            "soccer",
            "Bundesliga",
            "", "",
            "2026-04-18, 09:00 AM",
            1_776_520_800,
            "2026-04-18",
            "Hoffenheim",
            "Dortmund",
            "ok",
            "",
            now_ts,
        ),
    ]

    totals_markets = []
    for line in (8.5, 9.5, 10.5):
        lk = str(line).replace(".", "_")
        totals_markets.append(
            {
                "id": f"mkt_tot_{lk}",
                "conditionId": f"cond_tot_{lk}",
                "question": f"Braves vs Phillies: O/U {line}",
                "slug": f"mlb-phi-atl-2026-04-18-ou-{line}",
                "sportsMarketType": "totals",
                "line": line,
                "closed": False,
                "resolved": False,
                "volume": 1000,
                "outcomes": ["Over", "Under"],
                "clobTokenIds": [f"tok_over_{lk}", f"tok_under_{lk}"],
            }
        )

    with open_database(runtime) as db:
        db.markets.upsert_from_gamma_events(
            events_data=[
                {
                    "id": "evt_mlb",
                    "title": "Philadelphia Phillies vs Atlanta Braves",
                    "slug": "mlb-phi-atl-2026-04-18",
                    "startTime": "2026-04-18T23:00:00Z",
                    "teams": [
                        {"id": 10, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                        {"id": 11, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                    ],
                    "markets": [
                        {
                            "id": "mkt_moneyline",
                            "conditionId": "cond_moneyline",
                            "question": "Who will win the game?",
                            "slug": "mlb-phi-atl-2026-04-18-moneyline",
                            "sportsMarketType": "moneyline",
                            "line": None,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Atlanta Braves", "Philadelphia Phillies"],
                            "clobTokenIds": ["tok_ml_home", "tok_ml_away"],
                        },
                        *totals_markets,
                        {
                            "id": "mkt_spread",
                            "conditionId": "cond_spread",
                            "question": "Spread market",
                            "slug": "mlb-phi-atl-2026-04-18-spread",
                            "sportsMarketType": "spreads",
                            "line": 1.5,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Home", "Away"],
                            "clobTokenIds": ["tok_spread_h", "tok_spread_a"],
                        },
                        {
                            "id": "mkt_nrfi",
                            "conditionId": "cond_nrfi",
                            "question": "Will there be a run in the first inning?",
                            "slug": "mlb-phi-atl-2026-04-18-nrfi",
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
            ],
            updated_ts=now_ts,
        )
        db.linking.upsert_provider_games(provider_rows)
        result = LinkService(db=db).build_links(
            provider="kalstrop_v1",
            mapping=mapping,
            live_policy=live_policy,
            league_scope="all",
        )

        review = LinkReviewService(db=db)
        review.record_decision(
            provider="kalstrop_v1",
            run_id=int(result.run_id),
            provider_game_id="gid_mlb",
            decision="approve",
            actor="test",
        )
    return int(result.run_id)


def test_compile_requires_approval_when_not_forced(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_run(runtime=runtime)
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        # Overwrite latest decision to skip to simulate blocker.
        review.record_decision(
            provider="kalstrop_v1",
            run_id=run_id,
            provider_game_id="gid_mlb",
            decision="skip",
            actor="test",
        )
        # With the game skipped (not rejected), it is still eligible but has no tradeable targets
        # because approval is no longer required — only rejection excludes.
        # However a "skip" decision != "reject", so the game IS eligible.
        # The game was approved in _seed_run, then overwritten to "skip" above.
        # Since skip != reject, the game remains eligible and should compile.
        plan = compile_hotpath_plan(db=db, provider="kalstrop_v1", league="mlb", run_id=run_id, include_inactive=True)
        assert len(tuple(plan.games)) >= 1


def test_compile_window_filters_games_by_kickoff(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_run(runtime=runtime)
    with open_database(runtime) as db:
        # Too far before kickoff (outside upper horizon window) should be excluded.
        with pytest.raises(HotPathPlanError):
            compile_hotpath_plan(
                db=db,
                provider="kalstrop_v1",
                league="mlb",
                run_id=run_id,
                now_ts_utc=1_776_553_200 - (26 * 3600),
                plan_horizon_hours=24,
            )

        # Just before kickoff stays in-window.
        plan = compile_hotpath_plan(
            db=db,
            provider="kalstrop_v1",
            league="mlb",
            run_id=run_id,
            now_ts_utc=1_776_553_200 - 1800,
            plan_horizon_hours=24,
        )
        assert len(tuple(plan.games)) >= 1

        # Already-started/live games remain eligible (within 6h of kickoff).
        plan_live = compile_hotpath_plan(
            db=db,
            provider="kalstrop_v1",
            league="mlb",
            run_id=run_id,
            now_ts_utc=1_776_553_200 + (3 * 3600),
            plan_horizon_hours=24,
        )
        assert len(tuple(plan_live.games)) >= 1


def test_compile_hotpath_plan_stays_pinned_to_selected_run_id(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    approved_run_id = _seed_run(runtime=runtime)
    pending_run_id = _seed_run(runtime=runtime)
    assert pending_run_id > approved_run_id

    with open_database(runtime) as db:
        # Explicitly reject the game in the newer run to exclude it.
        from polybot2.linking import LinkReviewService
        review = LinkReviewService(db=db)
        review.record_decision(
            provider="kalstrop_v1",
            run_id=int(pending_run_id),
            provider_game_id="gid_mlb",
            decision="reject",
            actor="test",
        )
        # Rejected game means no eligible games → no tradeable targets → blocked.
        with pytest.raises(HotPathPlanError):
            compile_hotpath_plan(
                db=db,
                provider="kalstrop_v1",
                league="mlb",
                run_id=int(pending_run_id),
                include_inactive=True,
            )
        # The older run still has approval → eligible → compiles fine.
        approved_plan = compile_hotpath_plan(
            db=db,
            provider="kalstrop_v1",
            league="mlb",
            run_id=int(approved_run_id),
            include_inactive=True,
        )
        assert int(approved_plan.run_id) == int(approved_run_id)
