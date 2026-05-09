from __future__ import annotations

from pathlib import Path

import pytest

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.linking import LinkService, load_mapping


def _seed_mlb_fixture(
    db,
    *,
    provider_game_id: str,
    event_id: str = "246254",
    condition_id: str = "c1",
    game_date_et: str = "2026-04-18",
    with_tokens: bool,
    now_ts: int = 1_777_000_000,
) -> None:
    slug_prefix = f"mlb-ari-atl-{game_date_et}"
    db.markets.upsert_pm_events(
        [
            (
                event_id,
                "Will Arizona Diamondbacks win?",
                "",
                slug_prefix,
                slug_prefix,
                "",
                "mlb",
                None,
                game_date_et,
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
                condition_id,
                f"mkt_{condition_id}",
                event_id,
                "Will Arizona Diamondbacks win?",
                "",
                f"{slug_prefix}-ari",
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
    if with_tokens:
        db.markets.upsert_pm_market_tokens(
            [
                (f"{condition_id}_yes", condition_id, 0, "Yes", now_ts),
                (f"{condition_id}_no", condition_id, 1, "No", now_ts),
            ]
        )

    db.linking.upsert_provider_games(
        [
            (
                "kalstrop_v1",
                provider_game_id,
                "Arizona Diamondbacks vs Atlanta Braves, 2026-04-18, 01",
                "",
                "baseball",
                "Major League Baseball",
                "", "",
                "2026-04-18, 01:10 PM",
                None,
                game_date_et,
                "Arizona Diamondbacks",
                "Atlanta Braves",
                "ok",
                "",
                now_ts,
            )
        ]
    )


def test_link_build_requires_tradeable_targets_for_gate_pass(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    with open_database(runtime) as db:
        _seed_mlb_fixture(db, provider_game_id="gid_no_tokens", with_tokens=False)
        svc = LinkService(db=db)
        result = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")

    assert result.n_games_linked == 1
    assert result.n_targets_tradeable == 0
    assert result.gate_result == "fail"


def test_link_build_marks_unresolved_when_no_tradeable_targets(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    with open_database(runtime) as db:
        _seed_mlb_fixture(db, provider_game_id="gid_no_tokens", with_tokens=False)
        svc = LinkService(db=db)
        result = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        row = db.execute(
            """
            SELECT binding_status, reason_code, is_tradeable
            FROM link_game_bindings
            WHERE provider = ? AND provider_game_id = ?
            """,
            ("kalstrop_v1", "gid_no_tokens"),
        ).fetchone()

    assert result.n_games_tradeable == 0
    assert row is not None
    assert row["binding_status"] == "unresolved"
    assert row["reason_code"] == "no_tradeable_targets"
    assert int(row["is_tradeable"]) == 0


def test_link_build_atomic_rollback_on_persist_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    with open_database(runtime) as db:
        db.linking.upsert_game_bindings(
            [
                (
                    "kalstrop_v1",
                    "old_gid",
                    "mlb",
                    "arizona diamondbacks",
                    "atlanta braves",
                    "mlb-ari-atl-2026-04-18",
                    "exact",
                    "ok",
                    1,
                    "v_prev",
                    "h_prev",
                    1,
                    1,
                )
            ]
        )
        db.linking.upsert_market_bindings(
            [
                (
                    "kalstrop_v1",
                    "old_gid",
                    "old_c1",
                    0,
                    "old_t1",
                    "mlb-ari-atl-2026-04-18-ari",
                    "GAME",
                    "exact",
                    "ok",
                    1,
                    "v_prev",
                    "h_prev",
                    1,
                    1,
                )
            ]
        )
        _seed_mlb_fixture(db, provider_game_id="new_gid", condition_id="new_c1", with_tokens=True)

        original = db.linking.upsert_event_bindings

        def _boom(rows, *, commit: bool = True):
            del rows, commit
            raise RuntimeError("simulated_persist_failure")

        monkeypatch.setattr(db.linking, "upsert_event_bindings", _boom)
        svc = LinkService(db=db)
        with pytest.raises(RuntimeError, match="simulated_persist_failure"):
            svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")
        monkeypatch.setattr(db.linking, "upsert_event_bindings", original)

        rows = db.execute(
            """
            SELECT provider_game_id, binding_status
            FROM link_game_bindings
            WHERE provider = ?
            ORDER BY provider_game_id
            """,
            ("kalstrop_v1",),
        ).fetchall()
        market_rows = db.execute(
            """
            SELECT provider_game_id, token_id
            FROM link_market_bindings
            WHERE provider = ?
            ORDER BY provider_game_id, condition_id, outcome_index
            """,
            ("kalstrop_v1",),
        ).fetchall()

    assert [tuple(r) for r in rows] == [("old_gid", "exact")]
    assert [tuple(r) for r in market_rows] == [("old_gid", "old_t1")]


def test_link_build_atomic_success_replaces_provider_state_and_stamps_run_id(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    with open_database(runtime) as db:
        db.linking.upsert_game_bindings(
            [
                (
                    "kalstrop_v1",
                    "old_gid",
                    "mlb",
                    "arizona diamondbacks",
                    "atlanta braves",
                    "mlb-ari-atl-2026-04-18",
                    "exact",
                    "ok",
                    1,
                    "v_prev",
                    "h_prev",
                    1,
                    1,
                )
            ]
        )
        db.linking.upsert_market_bindings(
            [
                (
                    "kalstrop_v1",
                    "old_gid",
                    "old_c1",
                    0,
                    "old_t1",
                    "mlb-ari-atl-2026-04-18-ari",
                    "GAME",
                    "exact",
                    "ok",
                    1,
                    "v_prev",
                    "h_prev",
                    1,
                    1,
                )
            ]
        )
        _seed_mlb_fixture(db, provider_game_id="new_gid", condition_id="new_c1", with_tokens=True)
        svc = LinkService(db=db)
        res = svc.build_links(provider="kalstrop_v1", mapping=mapping, league_scope="all")

        game_rows = db.execute(
            """
            SELECT provider_game_id, run_id, is_tradeable
            FROM link_game_bindings
            WHERE provider = ?
            ORDER BY provider_game_id
            """,
            ("kalstrop_v1",),
        ).fetchall()
        market_rows = db.execute(
            """
            SELECT provider_game_id, run_id
            FROM link_market_bindings
            WHERE provider = ?
            ORDER BY provider_game_id, condition_id, outcome_index
            """,
            ("kalstrop_v1",),
        ).fetchall()
        latest_run = db.linking.load_latest_link_run(provider="kalstrop_v1")

    assert [r["provider_game_id"] for r in game_rows] == ["new_gid"]
    assert all(int(r["run_id"] or 0) == int(res.run_id) for r in game_rows)
    assert all(int(r["run_id"] or 0) == int(res.run_id) for r in market_rows)
    assert latest_run is not None
    assert int(latest_run["run_id"]) == int(res.run_id)
