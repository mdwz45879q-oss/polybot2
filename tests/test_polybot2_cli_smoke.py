from __future__ import annotations

import pytest

from polybot2._cli.parser import build_parser


def test_cli_parser_smoke() -> None:
    parser = build_parser()
    args = parser.parse_args(["mapping", "validate"])
    assert args.command == "mapping"
    assert args.mapping_command == "validate"

    review_args = parser.parse_args(
        [
            "link",
            "review",
            "card",
            "--provider",
            "boltodds",
            "--run-id",
            "1",
            "--provider-game-id",
            "gid1",
        ]
    )
    assert review_args.command == "link"
    assert review_args.link_command == "review"
    assert review_args.link_review_command == "card"

    review_v2_args = parser.parse_args(
        [
            "link",
            "review",
            "candidates",
            "--provider",
            "boltodds",
            "--run-id",
            "1",
            "--provider-game-id",
            "gid1",
        ]
    )
    assert review_v2_args.command == "link"
    assert review_v2_args.link_command == "review"
    assert review_v2_args.link_review_command == "candidates"
    review_default_provider_args = parser.parse_args(
        ["link", "review", "card", "--run-id", "1", "--provider-game-id", "gid1"]
    )
    assert review_default_provider_args.provider in {"", None}

    provider_capture_args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "boltodds",
            "--universal-id",
            "u1",
            "--league",
            "mlb",
            "--out",
            "/tmp/cap",
        ]
    )
    assert provider_capture_args.command == "provider"
    assert provider_capture_args.provider_command == "capture"
    assert provider_capture_args.universal_ids == ["u1"]
    provider_capture_kalstrop_args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "kalstrop",
            "--universal-id",
            "u1",
            "--league",
            "mlb",
            "--out",
            "/tmp/cap",
        ]
    )
    assert provider_capture_kalstrop_args.provider == "kalstrop"
    provider_capture_today_args = parser.parse_args(
        [
            "provider",
            "capture",
            "--provider",
            "kalstrop",
            "--today",
            "--date-et",
            "2026-04-19",
            "--league",
            "mlb",
            "--out",
            "/tmp/cap",
        ]
    )
    assert provider_capture_today_args.today is True
    provider_capture_default_provider_args = parser.parse_args(
        [
            "provider",
            "capture",
            "--universal-id",
            "u1",
            "--league",
            "mlb",
            "--out",
            "/tmp/cap",
        ]
    )
    assert provider_capture_default_provider_args.provider in {"", None}

    bench_args = parser.parse_args(["data", "benchmark-markets", "--concurrency-values", "20", "--max-rps-values", "48"])
    assert bench_args.command == "data"
    assert bench_args.data_command == "benchmark-markets"
    data_sync_args = parser.parse_args(["data", "sync", "--markets", "--open-only"])
    assert bool(data_sync_args.open_only) is True

    hotpath_args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            "1",
            "--profile-latency",
            "--force-launch",
        ]
    )
    assert hotpath_args.command == "hotpath"
    assert hotpath_args.hotpath_command == "run"
    assert hotpath_args.profile_latency is True
    assert hotpath_args.force_launch is True
    hotpath_kalstrop_args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--provider",
            "kalstrop",
            "--league",
            "mlb",
            "--link-run-id",
            "1",
            "--execution-mode",
            "paper",
        ]
    )
    assert hotpath_kalstrop_args.provider == "kalstrop"
    assert hotpath_kalstrop_args.execution_mode == "paper"
    hotpath_default_provider_args = parser.parse_args(
        [
            "hotpath",
            "run",
            "--league",
            "mlb",
            "--link-run-id",
            "1",
        ]
    )
    assert hotpath_default_provider_args.provider in {"", None}

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "hotpath",
                "run",
                "--league",
                "mlb",
                "--link-run-id",
                "1",
                "--run-profile",
            ]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "hotpath",
                "run",
                "--league",
                "mlb",
                "--link-run-id",
                "1",
                "--no-monitor",
            ]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "hotpath",
                "run",
                "--league",
                "mlb",
                "--link-run-id",
                "1",
                "--monitor-refresh-seconds",
                "60",
            ]
        )

    hotpath_replay_args = parser.parse_args(
        [
            "hotpath",
            "replay",
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            "1",
            "--capture-manifest",
            "/tmp/cap/manifest.json",
            "--universal-id",
            "gid1",
            "--mode",
            "as_fast",
            "--format",
            "json",
        ]
    )
    assert hotpath_replay_args.command == "hotpath"
    assert hotpath_replay_args.hotpath_command == "replay"
    assert hotpath_replay_args.universal_ids == ["gid1"]
    hotpath_replay_default_provider_args = parser.parse_args(
        [
            "hotpath",
            "replay",
            "--league",
            "mlb",
            "--link-run-id",
            "1",
            "--capture-manifest",
            "/tmp/cap/manifest.json",
        ]
    )
    assert hotpath_replay_default_provider_args.provider in {"", None}
    hotpath_observe_args = parser.parse_args(
        [
            "hotpath",
            "observe",
            "--socket-path",
            "/tmp/polybot2_hotpath_telemetry.sock",
            "--refresh-seconds",
            "1.5",
            "--max-games",
            "24",
            "--no-color",
        ]
    )
    assert hotpath_observe_args.command == "hotpath"
    assert hotpath_observe_args.hotpath_command == "observe"
    assert hotpath_observe_args.socket_path == "/tmp/polybot2_hotpath_telemetry.sock"
    assert float(hotpath_observe_args.refresh_seconds) == 1.5
    assert int(hotpath_observe_args.max_games) == 24
    assert bool(hotpath_observe_args.no_color) is True

    with pytest.raises(SystemExit):
        parser.parse_args(["hotpath", "observe", "--timeline-max", "2000"])
    with pytest.raises(SystemExit):
        parser.parse_args(["hotpath", "observe", "--per-game-max", "120"])
    with pytest.raises(SystemExit):
        parser.parse_args(["hotpath", "observe", "--per-chain-max", "80"])

    with pytest.raises(SystemExit):
        parser.parse_args(["hotpath", "run", "--provider", "boltodds", "--league", "mlb"])
    with pytest.raises(SystemExit):
        parser.parse_args(["hotpath", "replay", "--provider", "boltodds", "--league", "mlb", "--link-run-id", "1"])
    with pytest.raises(SystemExit):
        parser.parse_args(["hotpath", "benchmark"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "provider",
                "capture",
                "--provider",
                "boltodds",
                "--today",
                "--universal-id",
                "u1",
                "--league",
                "mlb",
                "--out",
                "/tmp/cap",
            ]
        )
    with pytest.raises(SystemExit):
        parser.parse_args(["link", "review", "summary", "--provider", "boltodds"])
    with pytest.raises(SystemExit):
        parser.parse_args(["link", "review", "queue", "--provider", "boltodds"])
