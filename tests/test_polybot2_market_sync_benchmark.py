from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest

from polybot2._cli.router import dispatch
from polybot2.data.markets import MarketSync
from polybot2.data.sync_config import MarketSyncConfig
from polybot2._cli.parser import build_parser
from polybot2.data.storage import DataRuntimeConfig, open_database


def _sample_event() -> dict:
    return {
        "id": "evt_bench_1",
        "ticker": "mlb-sd-laa-2026-04-17",
        "slug": "mlb-sd-laa-2026-04-17",
        "title": "Sample Event",
        "startDate": "2026-04-11T13:05:55.57941Z",
        "startTime": "2026-04-18T01:38:00Z",
        "endDate": "2026-04-25T01:38:00Z",
        "closed": False,
        "gameId": 100,
        "teams": [{"id": 1, "providerId": 9, "name": "Team A", "league": "mlb", "abbreviation": "ta"}],
        "tags": [{"id": 1, "label": "Sports", "slug": "sports"}],
        "markets": [
            {
                "id": "1946789",
                "conditionId": "cond-1946789",
                "question": "Sample question",
                "questionID": "q-1946789",
                "slug": "mlb-sd-laa-2026-04-17-moneyline",
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


def test_upsert_from_gamma_events_atomic_rollback_on_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    with open_database(runtime) as db:
        original = db.markets.upsert_pm_markets

        def _boom(rows, *, commit: bool = True):
            del rows, commit
            raise RuntimeError("simulated_failure")

        monkeypatch.setattr(db.markets, "upsert_pm_markets", _boom)
        with pytest.raises(RuntimeError, match="simulated_failure"):
            db.markets.upsert_from_gamma_events(events_data=[_sample_event()], updated_ts=1000)
        monkeypatch.setattr(db.markets, "upsert_pm_markets", original)

        ev_count = int(db.execute("SELECT COUNT(*) AS n FROM pm_events").fetchone()["n"])
        mkt_count = int(db.execute("SELECT COUNT(*) AS n FROM pm_markets").fetchone()["n"])
        tok_count = int(db.execute("SELECT COUNT(*) AS n FROM pm_market_tokens").fetchone()["n"])
        team_count = int(db.execute("SELECT COUNT(*) AS n FROM pm_event_teams").fetchone()["n"])

    assert ev_count == 0
    assert mkt_count == 0
    assert tok_count == 0
    assert team_count == 0


def test_data_sync_cli_overrides_are_applied(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    captured: dict[str, object] = {}

    class _FakeSync:
        def __init__(self, *, db, config):
            del db
            captured["config"] = config
            self._stats = {
                "elapsed_s": 1.0,
                "total_pages_processed": 2,
                "total_rows_processed": 1000,
                "retries_total": 0,
                "hard_failures": 0,
                "config": {
                    "concurrency": int(config.concurrency),
                    "max_rps": int(config.max_rps),
                    "batch_size": int(config.batch_size),
                    "fast_mode": bool(config.fast_mode),
                },
            }

        @property
        def last_run_stats(self):
            return dict(self._stats)

        async def run(self) -> int:
            return 123

    monkeypatch.setattr(data_actions, "MarketSync", _FakeSync)
    parser = build_parser()
    args = parser.parse_args(
        [
            "market",
            "sync",
            "--db",
            str(tmp_path / "db.sqlite"),
            "--batch-size",
            "321",
            "--concurrency",
            "37",
            "--max-rps",
            "111",
            "--open-max-pages",
            "7",
            "--fast-mode",
        ]
    )
    code = asyncio.run(dispatch(args, logger=logging.getLogger("polybot2.test.data_sync_overrides")))
    assert code == 0
    cfg = captured["config"]
    assert int(cfg.batch_size) == 321
    assert int(cfg.concurrency) == 37
    assert int(cfg.max_rps) == 111
    assert int(cfg.open_max_pages) == 7
    assert bool(cfg.open_only) is True
    assert bool(cfg.fast_mode) is True


def test_market_sync_open_only_skips_resolved_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DummyDB:
        pass

    sync = MarketSync(
        db=_DummyDB(),
        config=MarketSyncConfig(
            gamma_api="https://gamma-api.polymarket.com",
            concurrency=1,
            max_rps=0,
            open_only=True,
            resolved_max_pages=3,
            open_max_pages=2,
        ),
    )
    labels: list[str] = []

    async def _fake_run_pass(*args, **kwargs):
        labels.append(str(kwargs.get("label")))
        return (11, 2, 22)

    monkeypatch.setattr(sync, "_run_pass", _fake_run_pass)
    monkeypatch.setattr(sync, "_get_resolved_offset", lambda: (_ for _ in ()).throw(AssertionError("resolved_called")))
    monkeypatch.setattr(sync, "_save_resolved_offset", lambda _offset: (_ for _ in ()).throw(AssertionError("save_called")))

    count = asyncio.run(sync.run())
    stats = sync.last_run_stats or {}
    cfg = stats.get("config") if isinstance(stats.get("config"), dict) else {}

    assert count == 11
    assert labels == ["open"]
    assert int(stats.get("resolved_pages_processed") or 0) == 0
    assert int(stats.get("open_pages_processed") or 0) == 2
    assert bool(cfg.get("open_only")) is True


