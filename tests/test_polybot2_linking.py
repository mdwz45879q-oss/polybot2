from __future__ import annotations

from pathlib import Path

import pytest

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.linking import LinkService, load_mapping
from polybot2.linking.mapping_loader import LoadedLiveTradingPolicy, MappingValidationError


def test_deterministic_linking_flow(tmp_path: Path) -> None:
    db_path = tmp_path / "prediction_markets.db"
    runtime = DataRuntimeConfig(db_path=str(db_path))
    mapping = load_mapping()

    with open_database(runtime) as db:
        # One Polymarket event+market+token for MLB
        now_ts = 1_777_000_000
        db.markets.upsert_pm_events(
            [
                (
                    "246254",
                    "Will Arizona Diamondbacks win on 2026-04-18?",
                    "",
                    "mlb-ari-atl-2026-04-18",
                    "mlb-ari-atl-2026-04-18",
                    "",
                    "mlb",
                    None,
                    "2026-04-18",
                    None,
                    None,
                    None,
                    "open",
                    now_ts,
                )
            ]
        )
        db.markets.upsert_pm_markets(
            [
                (
                    "c1",
                    "m1",
                    "246254",
                    "Will Arizona Diamondbacks win on 2026-04-18?",
                    "",
                    "mlb-ari-atl-2026-04-18-ari",
                    "moneyline",
                    None,
                    None,
                    None,
                    0,
                    None,
                    0.0,
                    "",
                    None,
                    now_ts,
                )
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("t_yes", "c1", 0, "Yes", now_ts),
                ("t_no", "c1", 1, "No", now_ts),
            ]
        )

        # Provider game row with kalstrop_v1-style league signal.
        db.linking.upsert_provider_games(
            [
                (
                    "kalstrop_v1",
                    "gid1",
                    "Arizona Diamondbacks vs Atlanta Braves, 2026-04-18, 01",
                    "",
                    "baseball",
                    "Major League Baseball",
                    "", "",
                    "2026-04-18, 01:10 PM",
                    None,
                    "2026-04-18",
                    "Arizona Diamondbacks",
                    "Atlanta Braves",
                    "ok",
                    "",
                    now_ts,
                )
            ]
        )

        svc = LinkService(db=db)
        result = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        assert result.n_games_seen == 1
        assert result.n_games_linked == 1
        assert result.n_targets_tradeable >= 1



def test_league_normalization_fallback_links_kalstrop_without_provider_alias(tmp_path: Path) -> None:
    db_path = tmp_path / "prediction_markets.db"
    runtime = DataRuntimeConfig(db_path=str(db_path))
    mapping = load_mapping()
    now_ts = 1_777_000_000

    with open_database(runtime) as db:
        db.markets.upsert_pm_events(
            [
                (
                    "evt_kal_1",
                    "Arizona Diamondbacks vs Atlanta Braves",
                    "",
                    "mlb-ari-atl-2026-04-18",
                    "mlb-ari-atl-2026-04-18",
                    "",
                    "mlb",
                    None,
                    "2026-04-18",
                    1_776_553_200,
                    1_776_553_200,
                    1_776_639_600,
                    "open",
                    now_ts,
                )
            ]
        )
        db.markets.upsert_pm_event_teams(
            [
                ("evt_kal_1", 0, None, None, "Arizona Diamondbacks", "mlb", "ari", "", "", "", "", now_ts),
                ("evt_kal_1", 1, None, None, "Atlanta Braves", "mlb", "atl", "", "", "", "", now_ts),
            ],
            touched_event_ids=["evt_kal_1"],
        )
        db.markets.upsert_pm_markets(
            [
                (
                    "cond_kal_1",
                    "mkt_kal_1",
                    "evt_kal_1",
                    "Arizona Diamondbacks vs Atlanta Braves",
                    "",
                    "mlb-ari-atl-2026-04-18-moneyline",
                    "moneyline",
                    None,
                    None,
                    None,
                    0,
                    None,
                    0.0,
                    "",
                    None,
                    now_ts,
                )
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("tok_kal_yes", "cond_kal_1", 0, "Yes", now_ts),
                ("tok_kal_no", "cond_kal_1", 1, "No", now_ts),
            ]
        )
        db.linking.upsert_provider_games(
            [
                (
                    "kalstrop_v1",
                    "gid_kal_1",
                    "Arizona Diamondbacks vs Atlanta Braves",
                    "",
                    "baseball",
                    "Major League Baseball",
                    "", "",
                    "2026-04-18T23:00:00Z",
                    1_776_553_200,
                    "2026-04-18",
                    "Arizona Diamondbacks",
                    "Atlanta Braves",
                    "ok",
                    "",
                    now_ts,
                )
            ]
        )
        result = LinkService(db=db).build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")

    assert result.n_games_seen == 1
    assert result.n_games_linked == 1


def test_bundesliga_uses_polymarket_league_code_for_candidate_lookup(tmp_path: Path) -> None:
    db_path = tmp_path / "prediction_markets.db"
    runtime = DataRuntimeConfig(db_path=str(db_path))
    mapping = load_mapping()

    with open_database(runtime) as db:
        now_ts = 1_777_000_000
        kickoff_ts = 1_776_519_000  # 2026-04-18 13:30:00 UTC (example)
        event_id = "345313"
        db.markets.upsert_pm_events(
            [
                (
                    event_id,
                    "TSG 1899 Hoffenheim vs. BV Borussia 09 Dortmund",
                    "bun-hof-dor-2026-04-18",
                    "bun-hof-dor-2026-04-18",
                    "",
                    "bun",
                    "2026-04-18",
                    kickoff_ts,
                    kickoff_ts + 86_400,
                    "open",
                    now_ts,
                )
            ]
        )
        db.markets.upsert_pm_event_teams(
            [
                (event_id, 0, None, None, "TSG 1899 Hoffenheim", "bun", "hof", "", "", "", "", now_ts),
                (event_id, 1, None, None, "BV Borussia 09 Dortmund", "bun", "dor", "", "", "", "", now_ts),
            ],
            touched_event_ids=[event_id],
        )
        db.markets.upsert_pm_markets(
            [
                (
                    "cond_bun_1",
                    "mkt_bun_1",
                    event_id,
                    "TSG 1899 Hoffenheim vs. BV Borussia 09 Dortmund",
                    "",
                    "bun-hof-dor-2026-04-18-moneyline",
                    "moneyline",
                    None,
                    0,
                    None,
                    0.0,
                    "",
                    None,
                    now_ts,
                )
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("tok_bun_yes", "cond_bun_1", 0, "Yes", now_ts),
                ("tok_bun_no", "cond_bun_1", 1, "No", now_ts),
            ]
        )

        db.linking.upsert_provider_games(
            [
                (
                    "kalstrop_v1",
                    "gid_bun_1",
                    "TSG 1899 Hoffenheim vs BV Borussia 09 Dortmund",
                    "",
                    "soccer",
                    "Bundesliga",
                    "", "",
                    "2026-04-18, 09:00 AM",
                    kickoff_ts,
                    "2026-04-18",
                    "TSG 1899 Hoffenheim",
                    "BV Borussia 09 Dortmund",
                    "ok",
                    "",
                    now_ts,
                )
            ]
        )

        svc = LinkService(db=db)
        result = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        row = db.execute(
            """
            SELECT binding_status, reason_code, is_tradeable
            FROM link_game_bindings
            WHERE provider = ? AND provider_game_id = ?
            """,
            ("kalstrop_v1", "gid_bun_1"),
        ).fetchone()
        event_row = db.execute(
            """
            SELECT event_id
            FROM link_event_bindings
            WHERE provider = ? AND provider_game_id = ?
            """,
            ("kalstrop_v1", "gid_bun_1"),
        ).fetchone()

    assert result.n_games_seen == 1
    assert result.n_games_linked == 1
    assert row is not None
    assert row["binding_status"] == "exact"
    assert row["reason_code"] == "ok"
    assert int(row["is_tradeable"] or 0) == 1
    assert event_row is not None
    assert event_row["event_id"] == event_id


def test_live_policy_market_type_filter_applies_per_league(tmp_path: Path) -> None:
    db_path = tmp_path / "prediction_markets.db"
    runtime = DataRuntimeConfig(db_path=str(db_path))
    mapping = load_mapping()

    with open_database(runtime) as db:
        now_ts = 1_777_000_000
        event_id = "evt_mlb_filter"
        kickoff_ts = 1_776_553_200
        db.markets.upsert_pm_events(
            [
                (
                    event_id,
                    "Arizona Diamondbacks vs Atlanta Braves",
                    "mlb-ari-atl-2026-04-18",
                    "mlb-ari-atl-2026-04-18",
                    "",
                    "mlb",
                    "2026-04-18",
                    kickoff_ts,
                    kickoff_ts + 86_400,
                    "open",
                    now_ts,
                )
            ]
        )
        db.markets.upsert_pm_event_teams(
            [
                (event_id, 0, None, None, "Arizona Diamondbacks", "mlb", "ari", "", "", "", "", now_ts),
                (event_id, 1, None, None, "Atlanta Braves", "mlb", "atl", "", "", "", "", now_ts),
            ],
            touched_event_ids=[event_id],
        )
        db.markets.upsert_pm_markets(
            [
                (
                    "cond_allowed",
                    "mkt_allowed",
                    event_id,
                    "Arizona Diamondbacks vs Atlanta Braves",
                    "",
                    "mlb-ari-atl-2026-04-18-moneyline",
                    "moneyline",
                    None,
                    None,
                    None,
                    0,
                    None,
                    0.0,
                    "",
                    None,
                    now_ts,
                ),
                (
                    "cond_blocked",
                    "mkt_blocked",
                    event_id,
                    "Arizona Diamondbacks vs Atlanta Braves (blocked type)",
                    "",
                    "mlb-ari-atl-2026-04-18-exact-score",
                    "exact_score",
                    None,
                    None,
                    None,
                    0,
                    None,
                    0.0,
                    "",
                    None,
                    now_ts,
                ),
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("tok_allowed_yes", "cond_allowed", 0, "Yes", now_ts),
                ("tok_allowed_no", "cond_allowed", 1, "No", now_ts),
                ("tok_blocked_yes", "cond_blocked", 0, "Yes", now_ts),
                ("tok_blocked_no", "cond_blocked", 1, "No", now_ts),
            ]
        )
        db.linking.upsert_provider_games(
            [
                (
                    "kalstrop_v1",
                    "gid_mlb_filter",
                    "Arizona Diamondbacks vs Atlanta Braves, 2026-04-18, 01",
                    "",
                    "baseball",
                    "Major League Baseball",
                    "", "",
                    "2026-04-18, 01:10 PM",
                    kickoff_ts,
                    "2026-04-18",
                    "Arizona Diamondbacks",
                    "Atlanta Braves",
                    "ok",
                    "",
                    now_ts,
                )
            ]
        )

        svc = LinkService(db=db)
        result = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        target_rows = db.execute(
            """
            SELECT condition_id
            FROM link_market_bindings
            WHERE provider = ? AND provider_game_id = ?
            ORDER BY condition_id, outcome_index
            """,
            ("kalstrop_v1", "gid_mlb_filter"),
        ).fetchall()

    assert result.n_games_seen == 1
    assert result.n_games_linked == 1
    assert {str(r["condition_id"]) for r in target_rows} == {"cond_allowed"}


def test_build_links_fails_when_policy_market_type_not_in_reference_table(tmp_path: Path) -> None:
    db_path = tmp_path / "prediction_markets.db"
    runtime = DataRuntimeConfig(db_path=str(db_path))
    mapping = load_mapping()
    bad_policy = LoadedLiveTradingPolicy(
        path="test_policy.py",
        policy_version="v1",
        policy_hash="h",
        default_provider="kalstrop_v1",
        live_betting_leagues={"mlb"},
        live_betting_market_types_by_league={"mlb": {"moneyline", "not_real_type"}},
        live_betting_market_types={"moneyline", "not_real_type"},
    )

    with open_database(runtime) as db:
        db.execute(
            "INSERT INTO pm_sports_market_types_ref (market_type, synced_at) VALUES (?, ?)",
            ("moneyline", 1_777_000_000),
        )
        db.commit()
        db.linking.upsert_provider_games(
            [
                (
                    "kalstrop_v1",
                    "gid_validation",
                    "Arizona Diamondbacks vs Atlanta Braves, 2026-04-18, 01",
                    "",
                    "baseball",
                    "Major League Baseball",
                    "", "",
                    "2026-04-18, 01:10 PM",
                    None,
                    "2026-04-18",
                    "Arizona Diamondbacks",
                    "Atlanta Braves",
                    "ok",
                    "",
                    1_777_000_000,
                )
            ]
        )
        svc = LinkService(db=db)
        with pytest.raises(MappingValidationError, match="unknown market type"):
            svc.build_links(
                provider="kalstrop_v1",
                mapping=mapping,
                live_policy=bad_policy,
                league_scope="all",
            )
