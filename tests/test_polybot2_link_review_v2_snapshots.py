from __future__ import annotations

from pathlib import Path

import pytest

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.linking import LinkService, load_mapping


def _seed_fixture(db, *, include_unresolved: bool) -> None:
    now_ts = 1_777_000_000
    db.markets.upsert_from_gamma_events(
        events_data=[
            {
                "id": "evt_1",
                "title": "Philadelphia Phillies vs Atlanta Braves",
                "slug": "mlb-phi-atl-2026-04-18",
                "startTime": "2026-04-18T23:00:00Z",
                "teams": [
                    {"id": 10, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                    {"id": 11, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                ],
                "markets": [
                    {
                        "id": "mkt_1",
                        "conditionId": "cond_1",
                        "question": "Philadelphia Phillies vs Atlanta Braves",
                        "slug": "mlb-phi-atl-2026-04-18-moneyline",
                        "sportsMarketType": "moneyline",
                        "line": None,
                        "closed": False,
                        "resolved": False,
                        "volume": 1000,
                        "outcomes": ["Yes", "No"],
                        "clobTokenIds": ["tok_yes", "tok_no"],
                    }
                ],
            }
        ],
        updated_ts=now_ts,
    )
    provider_rows = [
        (
            "boltodds",
            "gid_ok",
            "ATL Braves vs PHI Phillies, 2026-04-18",
            "",
            "MLB",
            "",
            "2026-04-18, 07:00 PM",
            1_776_553_200,
            "2026-04-18",
            "ATL Braves",
            "PHI Phillies",
            "ok",
            "",
            "",
            "",
            0,
            now_ts,
        )
    ]
    if include_unresolved:
        provider_rows.append(
            (
                "boltodds",
                "gid_bad",
                "ATL Braves vs Unknown Team, 2026-04-18",
                "",
                "MLB",
                "",
                "2026-04-18, 07:15 PM",
                1_776_553_300,
                "2026-04-18",
                "ATL Braves",
                "Unknown Team",
                "ok",
                "",
                "",
                "",
                0,
                now_ts,
            )
        )
    db.linking.upsert_provider_games(provider_rows)


def test_link_build_writes_v2_snapshot_rows(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    with open_database(runtime) as db:
        _seed_fixture(db, include_unresolved=True)
        result = LinkService(db=db).build_links(provider="boltodds", mapping=mapping, league_scope="all")
        run_id = int(result.run_id)
        n_provider = int(
            db.execute("SELECT COUNT(*) AS n FROM link_run_provider_games WHERE run_id = ?", (run_id,)).fetchone()["n"]
        )
        n_reviews = int(
            db.execute("SELECT COUNT(*) AS n FROM link_run_game_reviews WHERE run_id = ?", (run_id,)).fetchone()["n"]
        )
        n_targets = int(
            db.execute("SELECT COUNT(*) AS n FROM link_run_market_targets WHERE run_id = ?", (run_id,)).fetchone()["n"]
        )
        n_candidates = int(
            db.execute("SELECT COUNT(*) AS n FROM link_run_event_candidates WHERE run_id = ?", (run_id,)).fetchone()["n"]
        )
        ranked = db.execute(
            """
            SELECT candidate_rank
            FROM link_run_event_candidates
            WHERE run_id = ? AND provider = ? AND provider_game_id = ?
            ORDER BY candidate_rank
            """,
            (run_id, "boltodds", "gid_ok"),
        ).fetchall()

    assert n_provider == 2
    assert n_reviews == 2
    assert n_targets == 2
    assert n_candidates >= 1
    if ranked:
        assert [int(r["candidate_rank"]) for r in ranked] == sorted(int(r["candidate_rank"]) for r in ranked)


def test_link_build_snapshot_transaction_rollback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    mapping = load_mapping()
    with open_database(runtime) as db:
        _seed_fixture(db, include_unresolved=False)
        first = LinkService(db=db).build_links(provider="boltodds", mapping=mapping, league_scope="all")
        first_run = int(first.run_id)

        original = db.linking.upsert_run_game_reviews

        def _boom(rows, *, commit: bool = True):
            del rows, commit
            raise RuntimeError("snapshot_insert_failed")

        monkeypatch.setattr(db.linking, "upsert_run_game_reviews", _boom)
        with pytest.raises(RuntimeError, match="snapshot_insert_failed"):
            LinkService(db=db).build_links(provider="boltodds", mapping=mapping, league_scope="all")
        monkeypatch.setattr(db.linking, "upsert_run_game_reviews", original)

        latest = db.linking.load_latest_link_run(provider="boltodds")
        n_rows_after = int(
            db.execute("SELECT COUNT(*) AS n FROM link_run_provider_games WHERE run_id = ?", (first_run,)).fetchone()["n"]
        )
        newer = int(
            db.execute("SELECT COUNT(*) AS n FROM link_runs WHERE provider = ? AND run_id > ?", ("boltodds", first_run)).fetchone()["n"]
        )

    assert latest is not None
    assert int(latest["run_id"]) == first_run
    assert n_rows_after == 1
    assert newer == 0
