from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.execution import resolve_token_id
from polybot2.linking import LinkService, load_mapping


def test_market_tokens_enforce_unique_condition_outcome(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    with open_database(runtime) as db:
        db.execute(
            """
            INSERT INTO pm_market_tokens(token_id, condition_id, outcome_index, outcome_label, updated_at)
            VALUES (?,?,?,?,?)
            """,
            ("tok_old", "c1", 0, "Yes", 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO pm_market_tokens(token_id, condition_id, outcome_index, outcome_label, updated_at)
                VALUES (?,?,?,?,?)
                """,
                ("tok_new", "c1", 0, "Yes", 2),
            )


def test_upsert_tokens_replaces_touched_condition_rows(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    with open_database(runtime) as db:
        db.markets.upsert_pm_market_tokens(
            [
                ("old_yes", "c1", 0, "Yes", 1),
                ("old_no", "c1", 1, "No", 1),
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("new_yes", "c1", 0, "Yes", 2),
                ("new_no", "c1", 1, "No", 2),
            ]
        )

        rows = db.execute(
            """
            SELECT token_id, condition_id, outcome_index
            FROM pm_market_tokens
            WHERE condition_id = ?
            ORDER BY outcome_index
            """,
            ("c1",),
        ).fetchall()
        got = [tuple(r) for r in rows]
        assert got == [
            ("new_yes", "c1", 0),
            ("new_no", "c1", 1),
        ]


def test_upsert_tokens_does_not_touch_untouched_conditions(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    with open_database(runtime) as db:
        db.markets.upsert_pm_market_tokens(
            [
                ("old_c1_yes", "c1", 0, "Yes", 1),
                ("old_c1_no", "c1", 1, "No", 1),
                ("keep_c2_yes", "c2", 0, "Yes", 1),
                ("keep_c2_no", "c2", 1, "No", 1),
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("new_c1_yes", "c1", 0, "Yes", 2),
                ("new_c1_no", "c1", 1, "No", 2),
            ]
        )

        c1_rows = db.execute(
            """
            SELECT token_id
            FROM pm_market_tokens
            WHERE condition_id = ?
            ORDER BY outcome_index
            """,
            ("c1",),
        ).fetchall()
        c2_rows = db.execute(
            """
            SELECT token_id
            FROM pm_market_tokens
            WHERE condition_id = ?
            ORDER BY outcome_index
            """,
            ("c2",),
        ).fetchall()

        assert [r["token_id"] for r in c1_rows] == ["new_c1_yes", "new_c1_no"]
        assert [r["token_id"] for r in c2_rows] == ["keep_c2_yes", "keep_c2_no"]


def test_resolve_token_id_returns_expected_after_token_change(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    with open_database(runtime) as db:
        db.markets.upsert_pm_market_tokens(
            [
                ("old_yes", "c1", 0, "Yes", 1),
                ("old_no", "c1", 1, "No", 1),
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("new_yes", "c1", 0, "Yes", 2),
                ("new_no", "c1", 1, "No", 2),
            ]
        )

    yes = resolve_token_id(runtime=runtime, condition_id="c1", outcome_index=0)
    no = resolve_token_id(runtime=runtime, condition_id="c1", outcome_index=1)
    assert yes.token_id == "new_yes"
    assert no.token_id == "new_no"


def test_linking_uses_updated_tokens_after_replacement(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()

    with open_database(runtime) as db:
        now_ts = 1_777_000_000
        db.markets.upsert_pm_events(
            [
                (
                    "246254",
                    "Will Arizona Diamondbacks win on 2026-04-18?",
                    "mlb-ari-atl-2026-04-18",
                    "mlb-ari-atl-2026-04-18",
                    "",
                    "mlb",
                    "2026-04-18",
                    None,
                    None,
                    "open",
                    "",
                    "",
                    0,
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
                    0,
                    None,
                    0.0,
                    "",
                    None,
                    "",
                    "",
                    0,
                    now_ts,
                )
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("old_yes", "c1", 0, "Yes", now_ts - 1),
                ("old_no", "c1", 1, "No", now_ts - 1),
            ]
        )
        db.markets.upsert_pm_market_tokens(
            [
                ("new_yes", "c1", 0, "Yes", now_ts),
                ("new_no", "c1", 1, "No", now_ts),
            ]
        )

        db.linking.upsert_provider_games(
            [
                (
                    "boltodds",
                    "gid1",
                    "ARI Diamondbacks vs ATL Braves, 2026-04-18, 01",
                    "",
                    "MLB",
                    "mlb",
                    "2026-04-18, 01:10 PM",
                    None,
                    "2026-04-18",
                    "ARI Diamondbacks",
                    "ATL Braves",
                    "ok",
                    "",
                    "",
                    "",
                    0,
                    now_ts,
                )
            ]
        )

        svc = LinkService(db=db)
        res = svc.build_links(provider="boltodds", mapping=mapping, league_scope="all")
        assert res.n_targets_tradeable >= 2

        rows = db.execute(
            """
            SELECT outcome_index, token_id
            FROM link_market_bindings
            WHERE provider = ? AND provider_game_id = ? AND condition_id = ?
            ORDER BY outcome_index
            """,
            ("boltodds", "gid1", "c1"),
        ).fetchall()
        assert [tuple(r) for r in rows] == [(0, "new_yes"), (1, "new_no")]
