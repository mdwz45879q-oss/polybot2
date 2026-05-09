from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.data.storage.db.schema import SCHEMA_VERSION


def _runtime(tmp_path: Path) -> DataRuntimeConfig:
    return DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))


def _sample_event(*, event_id: str, slug: str, ticker: str, game_id: int, teams: list[dict], market_id: str) -> dict:
    return {
        "id": event_id,
        "ticker": ticker,
        "slug": slug,
        "title": "Sample Event",
        "startDate": "2026-04-11T13:05:55.57941Z",
        "startTime": "2026-04-18T01:38:00Z",
        "endDate": "2026-04-25T01:38:00Z",
        "closed": False,
        "gameId": game_id,
        "teams": teams,
        "tags": [{"id": 1, "label": "Sports", "slug": "sports"}],
        "markets": [
            {
                "id": market_id,
                "conditionId": f"cond-{market_id}",
                "question": "Sample question",
                "questionID": f"q-{market_id}",
                "slug": f"{slug}-moneyline",
                "sportsMarketType": "moneyline",
                "line": -1.5,
                "eventStartTime": "2026-04-18T01:38:00Z",
                "gameStartTime": "2026-04-18 01:38:00+00",
                "volume": "123.4",
                "endDate": "2026-04-25T01:38:00Z",
                "closed": False,
                "outcomes": '["Team A","Team B"]',
                "clobTokenIds": '["tok_yes","tok_no"]',
            }
        ],
    }


def test_schema_v3_bootstrap_creates_new_tables_and_indexes(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    with open_database(runtime) as db:
        version = db.execute("SELECT version FROM _schema_version LIMIT 1").fetchone()
        assert int(version["version"]) == int(SCHEMA_VERSION)

        table_names = {
            str(r["name"])
            for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "pm_event_teams" in table_names
        assert "pm_sports_market_types_ref" in table_names
        assert "link_run_provider_games" in table_names
        assert "link_run_game_reviews" in table_names
        assert "link_run_event_candidates" in table_names
        assert "link_run_market_targets" in table_names
        assert "link_review_decisions" in table_names

        index_names = {
            str(r["name"])
            for r in db.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
        assert "idx_pm_event_teams_event_id" in index_names
        assert "idx_pm_event_teams_provider_team_id" in index_names
        assert "idx_link_run_provider_games_run_provider" in index_names
        assert "idx_link_run_game_reviews_run_provider" in index_names
        assert "idx_link_run_event_candidates_run_provider" in index_names
        assert "idx_link_run_market_targets_run_provider" in index_names
        assert "idx_link_review_decisions_run_provider_game" in index_names

        pm_event_cols = {
            str(r["name"])
            for r in db.execute("PRAGMA table_info(pm_events)").fetchall()
        }
        pm_market_cols = {
            str(r["name"])
            for r in db.execute("PRAGMA table_info(pm_markets)").fetchall()
        }
        link_market_binding_cols = {
            str(r["name"])
            for r in db.execute("PRAGMA table_info(link_market_bindings)").fetchall()
        }
        link_run_market_target_cols = {
            str(r["name"])
            for r in db.execute("PRAGMA table_info(link_run_market_targets)").fetchall()
        }
        assert "kickoff_ts_utc" in pm_event_cols
        assert "event_start_ts_utc" in pm_market_cols
        assert "game_start_ts_utc" in pm_market_cols
        assert "sports_market_type" in link_market_binding_cols
        assert "sports_market_type" in link_run_market_target_cols
        assert "market_family" not in link_market_binding_cols
        assert "market_family" not in link_run_market_target_cols


def test_upsert_from_gamma_events_persists_market_id_sports_market_type_line_question_id(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    with open_database(runtime) as db:
        event = _sample_event(
            event_id="e1",
            slug="mlb-sd-laa-2026-04-17",
            ticker="mlb-sd-laa-2026-04-17",
            game_id=100,
            teams=[],
            market_id="1946789",
        )
        db.markets.upsert_from_gamma_events(events_data=[event], updated_ts=1000)

        row = db.execute(
            """
            SELECT market_id, sports_market_type, line, question_id, event_start_ts_utc, game_start_ts_utc
            FROM pm_markets
            WHERE condition_id = ?
            """,
            ("cond-1946789",),
        ).fetchone()
        assert row is not None
        assert row["market_id"] == "1946789"
        assert row["sports_market_type"] == "moneyline"
        assert float(row["line"]) == -1.5
        assert row["question_id"] == "q-1946789"
        assert row["event_start_ts_utc"] is not None
        assert row["game_start_ts_utc"] is not None
        assert int(row["event_start_ts_utc"]) == int(row["game_start_ts_utc"])


def test_upsert_from_gamma_events_persists_event_ticker_and_game_id(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    with open_database(runtime) as db:
        event = _sample_event(
            event_id="e1",
            slug="mlb-sd-laa-2026-04-17",
            ticker="mlb-sd-laa-2026-04-17",
            game_id=10077574,
            teams=[],
            market_id="1946789",
        )
        db.markets.upsert_from_gamma_events(events_data=[event], updated_ts=2000)

        row = db.execute(
            """
            SELECT ticker, game_id, kickoff_ts_utc, start_ts_utc
            FROM pm_events
            WHERE event_id = ?
            """,
            ("e1",),
        ).fetchone()
        assert row is not None
        assert row["ticker"] == "mlb-sd-laa-2026-04-17"
        assert int(row["game_id"]) == 10077574
        assert row["kickoff_ts_utc"] is not None
        assert row["start_ts_utc"] is not None
        assert int(row["kickoff_ts_utc"]) != int(row["start_ts_utc"])


def test_upsert_event_teams_replaces_rows_for_touched_event_only(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    with open_database(runtime) as db:
        e1_initial = _sample_event(
            event_id="e1",
            slug="mlb-sd-laa-2026-04-17",
            ticker="mlb-sd-laa-2026-04-17",
            game_id=1,
            teams=[
                {"id": 10, "providerId": 110, "name": "Team A", "league": "mlb", "abbreviation": "a", "alias": "A"},
                {"id": 20, "providerId": 220, "name": "Team B", "league": "mlb", "abbreviation": "b", "alias": "B"},
            ],
            market_id="m1",
        )
        e2_initial = _sample_event(
            event_id="e2",
            slug="mlb-nyy-bos-2026-04-17",
            ticker="mlb-nyy-bos-2026-04-17",
            game_id=2,
            teams=[
                {"id": 30, "providerId": 330, "name": "Team C", "league": "mlb", "abbreviation": "c", "alias": "C"},
                {"id": 40, "providerId": 440, "name": "Team D", "league": "mlb", "abbreviation": "d", "alias": "D"},
            ],
            market_id="m2",
        )
        db.markets.upsert_from_gamma_events(events_data=[e1_initial, e2_initial], updated_ts=3000)

        e1_updated = _sample_event(
            event_id="e1",
            slug="mlb-sd-laa-2026-04-17",
            ticker="mlb-sd-laa-2026-04-17",
            game_id=1,
            teams=[
                {"id": 999, "providerId": 1999, "name": "Team A Updated", "league": "mlb", "abbreviation": "au", "alias": "A+"}
            ],
            market_id="m1",
        )
        db.markets.upsert_from_gamma_events(events_data=[e1_updated], updated_ts=4000)

        e1_rows = db.execute(
            """
            SELECT event_id, team_index, team_id, name
            FROM pm_event_teams
            WHERE event_id = ?
            ORDER BY team_index
            """,
            ("e1",),
        ).fetchall()
        e2_rows = db.execute(
            """
            SELECT event_id, team_index, team_id, name
            FROM pm_event_teams
            WHERE event_id = ?
            ORDER BY team_index
            """,
            ("e2",),
        ).fetchall()

        assert [tuple(r) for r in e1_rows] == [("e1", 0, 999, "Team A Updated")]
        assert len(e2_rows) == 2
        assert [r["team_id"] for r in e2_rows] == [30, 40]


def test_schema_version_mismatch_still_requires_fresh_bootstrap(tmp_path: Path) -> None:
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS _schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("DELETE FROM _schema_version")
        conn.execute("INSERT INTO _schema_version(version) VALUES (?)", (2,))
        conn.commit()
    finally:
        conn.close()

    runtime = DataRuntimeConfig(db_path=str(db_path))
    with pytest.raises(RuntimeError, match="Delete DB and re-bootstrap"):
        with open_database(runtime) as db:
            db.execute("SELECT 1").fetchone()
