from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
import signal
import time
from typing import Any

from polybot2._cli.actions import run_hotpath
from polybot2._cli.parser import build_parser
from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.linking import LinkReviewService, LinkService, load_mapping


def _seed_link_data(*, runtime: DataRuntimeConfig, include_unresolved: bool, provider: str = "boltodds") -> int:
    provider_name = str(provider).strip().lower() or "boltodds"
    mapping = load_mapping()
    now_ts = int(time.time())
    kickoff_ts = int(now_ts + 3600)
    kickoff_iso = datetime.fromtimestamp(kickoff_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    game_date = datetime.fromtimestamp(kickoff_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    sport_raw = "MLB"
    league_raw = ""
    when_raw = f"{game_date}, 07:00 PM"
    home_team = "ATL Braves"
    away_team = "PHI Phillies"
    unresolved_away = "Unknown Team"
    if provider_name == "kalstrop":
        sport_raw = "BASEBALL"
        league_raw = "mlb"
        when_raw = kickoff_iso
        home_team = "atlanta braves"
        away_team = "philadelphia phillies"
        unresolved_away = "not_a_real_team"
    provider_rows = [
        (
            provider_name,
            "gid_ok",
            f"{home_team} vs {away_team}, {game_date}",
            "",
            sport_raw,
            league_raw,
            when_raw,
            kickoff_ts,
            game_date,
            home_team,
            away_team,
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
                provider_name,
                "gid_bad",
                f"{home_team} vs {unresolved_away}, {game_date}",
                "",
                sport_raw,
                league_raw,
                when_raw,
                kickoff_ts + 900,
                game_date,
                home_team,
                unresolved_away,
                "ok",
                "",
                "",
                "",
                0,
                now_ts,
            )
        )

    with open_database(runtime) as db:
        db.markets.upsert_from_gamma_events(
            events_data=[
                {
                    "id": "evt_1",
                    "title": "Philadelphia Phillies vs Atlanta Braves",
                    "slug": f"mlb-phi-atl-{game_date}",
                    "startTime": kickoff_iso,
                    "teams": [
                        {"id": 10, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                        {"id": 11, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                    ],
                    "markets": [
                        {
                            "id": "mkt_1",
                            "conditionId": "cond_1",
                            "question": "Philadelphia Phillies vs Atlanta Braves",
                            "slug": f"mlb-phi-atl-{game_date}-moneyline",
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
        db.linking.upsert_provider_games(provider_rows)
        result = LinkService(db=db).build_links(provider=provider_name, mapping=mapping, league_scope="all")
    return int(result.run_id)


def _patch_hotpath_runtime(monkeypatch, *, actions_module) -> None:
    class _FakeProvider:
        def __init__(self, config):
            self.config = config

        def close(self) -> None:
            return None

    class _FakeExecution:
        def __init__(self, config):
            self.config = config

        def register_lifecycle_callback(self, callback) -> None:
            del callback
            return None

        def unregister_lifecycle_callback(self, callback) -> None:
            del callback
            return None

    class _FakeHotPath:
        def __init__(self, provider, execution, config, binding_resolver):
            del provider, execution, config, binding_resolver

        def set_subscriptions(self, universal_ids) -> None:
            del universal_ids
            return None

        def start(self) -> None:
            return None

        def drain_metrics(self):
            return []

        def stop(self) -> None:
            return None

    def _signal(_sig, handler):
        handler(_sig, None)
        return lambda *_a, **_k: None

    monkeypatch.setattr(
        actions_module,
        "build_sports_provider",
        lambda **_kwargs: _FakeProvider(config=None),
    )
    monkeypatch.setattr(actions_module, "FastExecutionService", _FakeExecution)
    monkeypatch.setattr(actions_module, "NativeHotPathService", _FakeHotPath)
    monkeypatch.setattr(actions_module.signal, "signal", _signal)
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")


def _approve_all_in_scope(*, runtime: DataRuntimeConfig, run_id: int, provider: str = "boltodds") -> None:
    provider_name = str(provider).strip().lower() or "boltodds"
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        rows = review.get_queue(provider=provider_name, run_id=run_id, parse_status="ok", limit=2000)
        for row in rows:
            review.record_decision(
                provider=provider_name,
                run_id=run_id,
                provider_game_id=str(row["provider_game_id"]),
                decision="approve",
                actor="test",
            )


def _latest_launch_audit(*, runtime: DataRuntimeConfig):
    with open_database(runtime) as db:
        return db.execute(
            """
            SELECT run_id, approved_run_id, gate_result, blocked, force_launch, message
            FROM link_launch_audit
            ORDER BY audit_id DESC
            LIMIT 1
            """
        ).fetchone()


def test_hotpath_v2_gate_blocks_with_pending_reviews(tmp_path: Path, monkeypatch, caplog) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=True)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")
    caplog.set_level(logging.INFO)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 1
    assert "hotpath launch blocked by link review v2" in caplog.text
    audit = _latest_launch_audit(runtime=runtime)
    assert audit is not None
    assert int(audit["blocked"]) == 1
    assert int(audit["force_launch"]) == 0


def test_hotpath_v2_gate_allows_when_all_in_scope_approved(tmp_path: Path, monkeypatch, caplog) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=True)
    _approve_all_in_scope(runtime=runtime, run_id=run_id)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    _patch_hotpath_runtime(monkeypatch, actions_module=runtime_actions)
    caplog.set_level(logging.INFO)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0
    assert "link review preflight v2" in caplog.text
    assert "blockers=none" in caplog.text
    audit = _latest_launch_audit(runtime=runtime)
    assert audit is not None
    assert int(audit["blocked"]) == 0
    assert int(audit["force_launch"]) == 0
    assert int(audit["approved_run_id"]) == run_id


def test_hotpath_v2_gate_allows_force_launch_with_audit(tmp_path: Path, monkeypatch, caplog) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=True)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
            "--force-launch",
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    _patch_hotpath_runtime(monkeypatch, actions_module=runtime_actions)
    caplog.set_level(logging.INFO)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0
    assert "hotpath launch proceeding due to --force-launch override" in caplog.text
    audit = _latest_launch_audit(runtime=runtime)
    assert audit is not None
    assert int(audit["blocked"]) == 0
    assert int(audit["force_launch"]) == 1


def test_hotpath_v2_gate_clean_run_still_blocks_until_approved(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    _patch_hotpath_runtime(monkeypatch, actions_module=runtime_actions)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 1
    audit = _latest_launch_audit(runtime=runtime)
    assert audit is not None
    assert int(audit["blocked"]) == 1


def test_hotpath_v2_gate_clean_run_all_approved_allows(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    _approve_all_in_scope(runtime=runtime, run_id=run_id)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    _patch_hotpath_runtime(monkeypatch, actions_module=runtime_actions)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0


def test_hotpath_v2_gate_blocks_on_skipped_decision(tmp_path: Path, monkeypatch, caplog) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    with open_database(runtime) as db:
        review = LinkReviewService(db=db)
        review.record_decision(
            provider="boltodds",
            run_id=run_id,
            provider_game_id="gid_ok",
            decision="skip",
            actor="test",
        )
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    caplog.set_level(logging.INFO)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 1
    assert "has_skipped" in caplog.text


def test_hotpath_mlb_runtime_forces_scores_only(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    _approve_all_in_scope(runtime=runtime, run_id=run_id)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")

    seen: dict[str, bool] = {}

    class _FakeProvider:
        def __init__(self, config):
            self.config = config

        def close(self) -> None:
            return None

    class _FakeExecution:
        def __init__(self, config):
            self.config = config

    class _FakeHotPath:
        def __init__(self, provider, execution, config, binding_resolver):
            del provider, execution, binding_resolver
            seen["run_odds"] = bool(config.run_odds)

        def set_subscriptions(self, universal_ids) -> None:
            del universal_ids
            return None

        def start(self) -> None:
            return None

        def drain_metrics(self):
            return []

        def stop(self) -> None:
            return None

    def _signal(_sig, handler):
        handler(_sig, None)
        return lambda *_a, **_k: None

    monkeypatch.setattr(runtime_actions,
        "build_sports_provider",
        lambda **_kwargs: _FakeProvider(config=None),
    )
    monkeypatch.setattr(runtime_actions, "FastExecutionService", _FakeExecution)
    monkeypatch.setattr(runtime_actions, "NativeHotPathService", _FakeHotPath)
    monkeypatch.setattr(runtime_actions.signal, "signal", _signal)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0
    assert seen.get("run_odds") is False


def test_hotpath_starts_with_empty_initial_timed_subscriptions(tmp_path: Path, monkeypatch, caplog) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    _approve_all_in_scope(runtime=runtime, run_id=run_id)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")
    monkeypatch.setenv("POLYBOT2_SUBSCRIBE_UNIVERSAL_IDS", "uid_not_in_plan")

    class _FakeProvider:
        def __init__(self, config):
            self.config = config

        def close(self) -> None:
            return None

    class _FakeExecution:
        def __init__(self, config):
            self.config = config

    class _FakeHotPath:
        def __init__(self, provider, execution, config, binding_resolver):
            del provider, execution, config, binding_resolver

        def set_subscriptions(self, universal_ids) -> None:
            del universal_ids
            return None

        def start(self) -> None:
            return None

        def drain_metrics(self):
            return []

        def stop(self) -> None:
            return None

    def _signal(_sig, handler):
        handler(_sig, None)
        return lambda *_a, **_k: None

    monkeypatch.setattr(runtime_actions,
        "build_sports_provider",
        lambda **_kwargs: _FakeProvider(config=None),
    )
    monkeypatch.setattr(runtime_actions, "FastExecutionService", _FakeExecution)
    monkeypatch.setattr(runtime_actions, "NativeHotPathService", _FakeHotPath)
    monkeypatch.setattr(runtime_actions.signal, "signal", _signal)
    caplog.set_level(logging.INFO)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0
    assert "waiting for games to enter subscription window" in caplog.text


def test_hotpath_kalstrop_runtime_enabled(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False, provider="kalstrop")
    _approve_all_in_scope(runtime=runtime, run_id=run_id, provider="kalstrop")
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "kalstrop",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")
    _patch_hotpath_runtime(monkeypatch, actions_module=runtime_actions)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0


def test_hotpath_default_provider_missing_kalstrop_credentials_fails_fast(tmp_path: Path, monkeypatch, caplog) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False, provider="kalstrop")
    _approve_all_in_scope(runtime=runtime, run_id=run_id, provider="kalstrop")
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.delenv("KALSTROP_CLIENT_ID", raising=False)
    monkeypatch.delenv("KALSTROP_SHARED_SECRET_RAW", raising=False)
    monkeypatch.delenv("CLIENT_ID", raising=False)
    monkeypatch.delenv("SHARED_SECRET_RAW", raising=False)
    caplog.set_level(logging.INFO)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 1
    assert "Kalstrop credentials are required for hotpath run" in caplog.text


def test_hotpath_startup_blocks_when_provider_resolves_no_in_window_subscriptions(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    _approve_all_in_scope(runtime=runtime, run_id=run_id)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")

    class _FakeProvider:
        def __init__(self, config):
            self.config = config

        def close(self) -> None:
            return None

    class _FakeExecution:
        def __init__(self, config):
            self.config = config

    class _FakeHotPath:
        def __init__(self, provider, execution, config, binding_resolver):
            del provider, execution, config, binding_resolver
            self._subs: list[str] = []

        def set_subscriptions(self, universal_ids) -> None:
            del universal_ids
            self._subs = []

        def health(self):
            return {"subscriptions": list(self._subs)}

        def start(self) -> None:
            return None

        def drain_metrics(self):
            return []

        def stop(self) -> None:
            return None

    monkeypatch.setattr(runtime_actions,
        "build_sports_provider",
        lambda **_kwargs: _FakeProvider(config=None),
    )
    monkeypatch.setattr(runtime_actions, "FastExecutionService", _FakeExecution)
    monkeypatch.setattr(runtime_actions, "NativeHotPathService", _FakeHotPath)
    caplog.set_level(logging.INFO)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 1
    assert "provider resolved none" in caplog.text


def test_hotpath_run_scopes_kalstrop_catalog_to_league(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False, provider="kalstrop")
    _approve_all_in_scope(runtime=runtime, run_id=run_id, provider="kalstrop")
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "kalstrop",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")

    class _Cfg:
        def __init__(self) -> None:
            self.catalog_sport_codes = ("baseball", "soccer")

    cfg = _Cfg()

    class _FakeProvider:
        def __init__(self, config):
            del config
            self.config = cfg

        def close(self) -> None:
            return None

    class _FakeExecution:
        def __init__(self, config):
            self.config = config

        def register_lifecycle_callback(self, callback) -> None:
            del callback
            return None

        def unregister_lifecycle_callback(self, callback) -> None:
            del callback
            return None

    class _FakeHotPath:
        def __init__(self, provider, execution, config, binding_resolver):
            del provider, execution, config, binding_resolver

        def set_subscriptions(self, universal_ids) -> None:
            del universal_ids
            return None

        def start(self) -> None:
            return None

        def drain_metrics(self):
            return {}

        def stop(self) -> None:
            return None

    def _signal(_sig, handler):
        handler(_sig, None)
        return lambda *_a, **_k: None

    monkeypatch.setattr(runtime_actions, "build_sports_provider", lambda **_kwargs: _FakeProvider(config=None))
    monkeypatch.setattr(runtime_actions, "FastExecutionService", _FakeExecution)
    monkeypatch.setattr(runtime_actions, "NativeHotPathService", _FakeHotPath)
    monkeypatch.setattr(runtime_actions.signal, "signal", _signal)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0
    assert tuple(cfg.catalog_sport_codes) == ("baseball",)


def test_hotpath_run_has_no_inline_monitor_dependency(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    assert not hasattr(runtime_actions, "HotpathInlineMonitor")

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    _approve_all_in_scope(runtime=runtime, run_id=run_id)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")

    handlers: dict[int, Any] = {}

    class _FakeProvider:
        def __init__(self, config):
            self.config = config

        def close(self) -> None:
            return None

    class _FakeExecution:
        def __init__(self, config):
            self.config = config

    class _FakeHotPath:
        def __init__(self, provider, execution, config, binding_resolver):
            del provider, execution, config, binding_resolver

        def set_subscriptions(self, universal_ids) -> None:
            del universal_ids
            return None

        def start(self) -> None:
            return None

        def drain_metrics(self):
            return {"messages_total": 1, "messages_no_action": 1}

        def stop(self) -> None:
            return None

    def _signal(sig, handler):
        handlers[int(sig)] = handler
        return lambda *_a, **_k: None

    def _sleep(_seconds):
        handler = handlers.get(int(signal.SIGINT))
        if handler is not None:
            handler(int(signal.SIGINT), None)

    monkeypatch.setattr(runtime_actions, "build_sports_provider", lambda **_kwargs: _FakeProvider(config=None))
    monkeypatch.setattr(runtime_actions, "FastExecutionService", _FakeExecution)
    monkeypatch.setattr(runtime_actions, "NativeHotPathService", _FakeHotPath)
    monkeypatch.setattr(runtime_actions.signal, "signal", _signal)
    monkeypatch.setattr(runtime_actions.time, "sleep", _sleep)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0


def test_hotpath_run_does_not_write_run_artifacts(tmp_path: Path, monkeypatch) -> None:
    import polybot2._cli.commands_hotpath_runtime as runtime_actions

    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id = _seed_link_data(runtime=runtime, include_unresolved=False)
    _approve_all_in_scope(runtime=runtime, run_id=run_id)
    parser = build_parser()
    args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
        ]
    )

    artifacts_root = tmp_path / "hotpath_run_artifacts_disabled"
    monkeypatch.setenv("POLYBOT2_HOTPATH_RUN_ARTIFACTS_DIR", str(artifacts_root))
    monkeypatch.setenv("BOLTODDS_API_KEY", "test_key")
    monkeypatch.setenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", "test_presign_key")

    handlers: dict[int, Any] = {}

    class _FakeProvider:
        def __init__(self, config):
            self.config = config

        def close(self) -> None:
            return None

    class _FakeExecution:
        def __init__(self, config):
            self.config = config

    class _FakeHotPath:
        def __init__(self, provider, execution, config, binding_resolver):
            del provider, execution, config, binding_resolver

        def set_subscriptions(self, universal_ids) -> None:
            del universal_ids
            return None

        def start(self) -> None:
            return None

        def drain_metrics(self):
            return {"messages_total": 1, "messages_no_action": 1}

        def stop(self) -> None:
            return None

    def _signal(sig, handler):
        handlers[int(sig)] = handler
        return lambda *_a, **_k: None

    def _sleep(_seconds):
        handler = handlers.get(int(signal.SIGINT))
        if handler is not None:
            handler(int(signal.SIGINT), None)

    monkeypatch.setattr(runtime_actions, "build_sports_provider", lambda **_kwargs: _FakeProvider(config=None))
    monkeypatch.setattr(runtime_actions, "FastExecutionService", _FakeExecution)
    monkeypatch.setattr(runtime_actions, "NativeHotPathService", _FakeHotPath)
    monkeypatch.setattr(runtime_actions.signal, "signal", _signal)
    monkeypatch.setattr(runtime_actions.time, "sleep", _sleep)

    code = run_hotpath(args, logger=logging.getLogger("polybot2.test.hotpath_gate"))
    assert code == 0
    assert not artifacts_root.exists()
