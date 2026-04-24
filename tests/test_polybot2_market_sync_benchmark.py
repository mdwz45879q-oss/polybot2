from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

import pytest

from polybot2._cli.actions import dispatch
from polybot2.data.storage.db import markets as markets_module
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
            db.markets.upsert_from_gamma_events(events_data=[_sample_event()], updated_ts=1000, payload_writer=None)
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
                    "enable_payload_artifacts": bool(config.enable_payload_artifacts),
                    "fast_mode": bool(config.fast_mode),
                    "compute_lineage_hash": bool(True),
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
            "data",
            "sync",
            "--markets",
            "--db",
            str(tmp_path / "db.sqlite"),
            "--batch-size",
            "321",
            "--concurrency",
            "37",
            "--max-rps",
            "111",
            "--resolved-max-pages",
            "5",
            "--open-max-pages",
            "7",
            "--open-only",
            "--enable-payload-artifacts",
            "--fast-mode",
        ]
    )
    code = asyncio.run(dispatch(args, logger=logging.getLogger("polybot2.test.data_sync_overrides")))
    assert code == 0
    cfg = captured["config"]
    assert int(cfg.batch_size) == 321
    assert int(cfg.concurrency) == 37
    assert int(cfg.max_rps) == 111
    assert int(cfg.resolved_max_pages) == 5
    assert int(cfg.open_max_pages) == 7
    assert bool(cfg.open_only) is True
    assert bool(cfg.enable_reference_sync) is True
    assert bool(cfg.enable_payload_artifacts) is True
    assert bool(cfg.fast_mode) is True


def test_data_sync_cli_defaults_disable_payload_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    captured: dict[str, object] = {}

    class _FakeSync:
        def __init__(self, *, db, config):
            del db
            captured["config"] = config
            self._stats = {"elapsed_s": 1.0, "config": {"enable_payload_artifacts": bool(config.enable_payload_artifacts)}}

        @property
        def last_run_stats(self):
            return dict(self._stats)

        async def run(self) -> int:
            return 1

    monkeypatch.setattr(data_actions, "MarketSync", _FakeSync)
    parser = build_parser()
    args = parser.parse_args(
        [
            "data",
            "sync",
            "--markets",
            "--db",
            str(tmp_path / "db.sqlite"),
        ]
    )
    code = asyncio.run(dispatch(args, logger=logging.getLogger("polybot2.test.data_sync_defaults")))
    assert code == 0
    cfg = captured["config"]
    assert bool(cfg.enable_payload_artifacts) is False
    assert bool(cfg.open_only) is False


def test_benchmark_cli_matrix_and_output_shape(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import polybot2._cli.commands_data_provider as data_actions

    class _FakeSync:
        def __init__(self, *, db, config):
            del db
            self._cfg = config
            self._stats: dict[str, object] = {}

        @property
        def last_run_stats(self):
            return dict(self._stats)

        async def run(self) -> int:
            markets = int(self._cfg.concurrency) * 1000 + int(self._cfg.max_rps)
            self._stats = {
                "elapsed_s": 1.0,
                "requests_attempted": 10,
                "retries_total": 0,
                "retry_http_429": 0,
                "retry_http_5xx": 0,
                "retry_http_error": 0,
                "hard_failures": 0,
                "resolved_pages_processed": int(self._cfg.resolved_max_pages or 0),
                "open_pages_processed": int(self._cfg.open_max_pages or 0),
                "total_pages_processed": int((self._cfg.resolved_max_pages or 0) + (self._cfg.open_max_pages or 0)),
                "total_rows_processed": markets,
                "total_markets_processed": markets,
            }
            return markets

    monkeypatch.setattr(data_actions, "MarketSync", _FakeSync)

    out_dir = tmp_path / "bench_out"
    parser = build_parser()
    args = parser.parse_args(
        [
            "data",
            "benchmark-markets",
            "--concurrency-values",
            "2,4",
            "--max-rps-values",
            "5,7",
            "--repeats",
            "2",
            "--batch-size",
            "100",
            "--resolved-max-pages",
            "3",
            "--open-max-pages",
            "2",
            "--skip-reference-sync",
            "--disable-payload-artifacts",
            "--fast-mode",
            "--output-dir",
            str(out_dir),
        ]
    )
    code = asyncio.run(dispatch(args, logger=logging.getLogger("polybot2.test.market_benchmark")))
    assert code == 0

    jsonl_files = sorted(out_dir.glob("benchmark_*.jsonl"))
    summary_files = sorted(out_dir.glob("benchmark_*_summary.json"))
    assert len(jsonl_files) == 1
    assert len(summary_files) == 1

    jsonl_lines = [line for line in jsonl_files[0].read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(jsonl_lines) == 8  # 2 conc x 2 rps x 2 repeats

    summary = json.loads(summary_files[0].read_text(encoding="utf-8"))
    assert len(summary["summary_rows"]) == 4
    assert summary["recommended"] is not None
    assert int(summary["recommended"]["concurrency"]) == 4
    assert int(summary["recommended"]["max_rps"]) == 7
    assert bool(summary["matrix"]["enable_reference_sync"]) is False
    assert bool(summary["matrix"]["enable_payload_artifacts"]) is False
    assert bool(summary["matrix"]["fast_mode"]) is True


def test_market_sync_run_stage_timing_and_disabled_reference_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DummyDB:
        pass

    sync = MarketSync(
        db=_DummyDB(),
        config=MarketSyncConfig(
            gamma_api="https://gamma-api.polymarket.com",
            concurrency=1,
            max_rps=0,
            enable_reference_sync=False,
            enable_payload_artifacts=False,
            fast_mode=True,
            resolved_max_pages=1,
            open_max_pages=1,
        ),
    )
    called: list[str] = []

    async def _fake_run_pass(*args, **kwargs):
        stage = kwargs.get("stage_metrics")
        if isinstance(stage, dict):
            stage["fetch_s"] = float(stage.get("fetch_s", 0.0)) + 0.123
            stage["db_upsert_s"] = float(stage.get("db_upsert_s", 0.0)) + 0.456
        return (10, 1, 10)

    async def _fake_sync_reference(*args, **kwargs):
        called.append("ref")

    monkeypatch.setattr(sync, "_run_pass", _fake_run_pass)
    monkeypatch.setattr(sync, "_sync_reference_metadata", _fake_sync_reference)
    monkeypatch.setattr(sync, "_get_resolved_offset", lambda: 0)
    monkeypatch.setattr(sync, "_save_resolved_offset", lambda _offset: None)

    count = asyncio.run(sync.run())
    stats = sync.last_run_stats or {}
    stage = stats.get("stage_timing_s") if isinstance(stats.get("stage_timing_s"), dict) else {}

    assert count == 20
    assert called == []
    assert float(stage.get("fetch") or 0.0) > 0.0
    assert float(stage.get("db_upsert") or 0.0) > 0.0
    assert float(stage.get("reference_sync") or 0.0) == 0.0
    assert float(stage.get("artifact_write") or 0.0) == 0.0


def test_market_sync_open_only_skips_resolved_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DummyDB:
        pass

    sync = MarketSync(
        db=_DummyDB(),
        config=MarketSyncConfig(
            gamma_api="https://gamma-api.polymarket.com",
            concurrency=1,
            max_rps=0,
            enable_reference_sync=False,
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


def test_upsert_from_gamma_events_fast_mode_skips_lineage_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    with open_database(runtime) as db:
        monkeypatch.setattr(markets_module.hashlib, "sha256", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sha256_called")))
        db.markets.upsert_from_gamma_events(
            events_data=[_sample_event()],
            updated_ts=1000,
            payload_writer=None,
            compute_lineage_hash=False,
        )
        event_row = db.execute(
            "SELECT payload_sha256, payload_ref, payload_size_bytes FROM pm_events WHERE event_id = ?",
            ("evt_bench_1",),
        ).fetchone()
        market_row = db.execute(
            "SELECT payload_sha256, payload_ref, payload_size_bytes FROM pm_markets WHERE condition_id = ?",
            ("cond-1946789",),
        ).fetchone()

    assert event_row is not None
    assert str(event_row["payload_sha256"]) == ""
    assert str(event_row["payload_ref"]) == ""
    assert int(event_row["payload_size_bytes"] or 0) == 0
    assert market_row is not None
    assert str(market_row["payload_sha256"]) == ""
    assert str(market_row["payload_ref"]) == ""
    assert int(market_row["payload_size_bytes"] or 0) == 0
