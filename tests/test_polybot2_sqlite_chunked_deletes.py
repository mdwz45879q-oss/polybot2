from __future__ import annotations

from pathlib import Path

from polybot2.data.storage import DataRuntimeConfig, open_database


def test_upsert_pm_market_tokens_handles_many_touched_conditions(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    rows = [
        (f"token_{i}", f"condition_{i}", 0, "Yes", 1)
        for i in range(1_500)
    ]
    with open_database(runtime) as db:
        db.markets.upsert_pm_market_tokens(rows)
        count = int(db.execute("SELECT COUNT(*) AS n FROM pm_market_tokens").fetchone()["n"])
    assert count == len(rows)


def test_upsert_pm_event_teams_handles_many_touched_events(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    touched_event_ids = [f"event_{i}" for i in range(1_500)]
    with open_database(runtime) as db:
        db.markets.upsert_pm_event_teams([], touched_event_ids=touched_event_ids)
        count = int(db.execute("SELECT COUNT(*) AS n FROM pm_event_teams").fetchone()["n"])
    assert count == 0
