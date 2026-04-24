from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from pathlib import Path

from polybot2._cli.actions import run_hotpath_replay
from polybot2._cli.parser import build_parser
from polybot2.data.storage import DataRuntimeConfig, open_database
from polybot2.hotpath.replay import ReplayConfig, run_hotpath_replay as run_hotpath_replay_api
from polybot2.linking import LinkService, load_mapping


def _seed_run(
    *,
    runtime: DataRuntimeConfig,
    totals_lines: tuple[float, ...] = (0.5, 6.5),
) -> tuple[int, str]:
    mapping = load_mapping()
    now_ts = 1_777_000_100
    provider_game_id = "gid_mlb_replay"
    provider_rows = [
        (
            "boltodds",
            provider_game_id,
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
        ),
    ]

    with open_database(runtime) as db:
        db.markets.upsert_from_gamma_events(
            events_data=[
                {
                    "id": "evt_mlb_replay",
                    "title": "Philadelphia Phillies vs Atlanta Braves",
                    "slug": "mlb-phi-atl-2026-04-18",
                    "startTime": "2026-04-18T23:00:00Z",
                    "teams": [
                        {"id": 10, "name": "philadelphia phillies", "abbreviation": "phi", "alias": ""},
                        {"id": 11, "name": "atlanta braves", "abbreviation": "atl", "alias": ""},
                    ],
                    "markets": [
                        {
                            "id": "mkt_moneyline_replay",
                            "conditionId": "cond_moneyline_replay",
                            "question": "Who will win the game?",
                            "slug": "mlb-phi-atl-2026-04-18-moneyline",
                            "sportsMarketType": "moneyline",
                            "line": None,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Atlanta Braves", "Philadelphia Phillies"],
                            "clobTokenIds": ["tok_ml_home_replay", "tok_ml_away_replay"],
                        },
                        *[
                            {
                                "id": f"mkt_tot_{str(line).replace('.', '_')}_replay",
                                "conditionId": f"cond_tot_{str(line).replace('.', '_')}_replay",
                                "question": f"Braves vs Phillies: O/U {line}",
                                "slug": f"mlb-phi-atl-2026-04-18-ou-{line}",
                                "sportsMarketType": "totals",
                                "line": float(line),
                                "closed": False,
                                "resolved": False,
                                "volume": 1000,
                                "outcomes": ["Over", "Under"],
                                "clobTokenIds": [
                                    f"tok_over_{str(line).replace('.', '_')}_replay",
                                    f"tok_under_{str(line).replace('.', '_')}_replay",
                                ],
                            }
                            for line in totals_lines
                        ],
                        {
                            "id": "mkt_spread_replay",
                            "conditionId": "cond_spread_replay",
                            "question": "Spread market",
                            "slug": "mlb-phi-atl-2026-04-18-spread",
                            "sportsMarketType": "spreads",
                            "line": 1.5,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Home", "Away"],
                            "clobTokenIds": ["tok_spread_home_replay", "tok_spread_away_replay"],
                        },
                        {
                            "id": "mkt_nrfi_replay",
                            "conditionId": "cond_nrfi_replay",
                            "question": "Will there be a run in the first inning?",
                            "slug": "mlb-phi-atl-2026-04-18-nrfi",
                            "sportsMarketType": "nrfi",
                            "line": None,
                            "closed": False,
                            "resolved": False,
                            "volume": 1000,
                            "outcomes": ["Yes", "No"],
                            "clobTokenIds": ["tok_nrfi_yes_replay", "tok_nrfi_no_replay"],
                        },
                    ],
                }
            ],
            updated_ts=now_ts,
        )
        db.linking.upsert_provider_games(provider_rows)
        result = LinkService(db=db).build_links(provider="boltodds", mapping=mapping, league_scope="all")
        db.linking.upsert_run_market_targets(
            [
                (
                    int(result.run_id),
                    "boltodds",
                    provider_game_id,
                    "cond_nrfi_replay",
                    0,
                    "tok_nrfi_yes_replay",
                    "mlb-phi-atl-2026-04-18-nrfi",
                    "NRFI",
                    "exact",
                    "",
                    1,
                    now_ts,
                ),
                (
                    int(result.run_id),
                    "boltodds",
                    provider_game_id,
                    "cond_nrfi_replay",
                    1,
                    "tok_nrfi_no_replay",
                    "mlb-phi-atl-2026-04-18-nrfi",
                    "NRFI",
                    "exact",
                    "",
                    1,
                    now_ts,
                ),
            ]
        )
    return (int(result.run_id), provider_game_id)


def _score_event(
    *,
    uid: str,
    home: int,
    away: int,
    inning: int,
    half: str,
    received_ts: int,
    match_completed: bool = False,
) -> dict[str, object]:
    half_key = str(half).strip().lower()
    if half_key == "top":
        period = f"AT_TOP_{inning}TH_INNING"
        top_of_inning = True
    elif half_key == "bottom":
        period = f"AT_BOT_{inning}TH_INNING"
        top_of_inning = False
    elif half_key == "end":
        period = f"AT_END_{inning}TH_INNING"
        top_of_inning = False
    else:
        period = ""
        top_of_inning = False
    return {
        "provider": "boltodds",
        "stream": "scores",
        "universal_id": uid,
        "payload_kind": "match_update",
        "received_ts": int(received_ts),
        "dedup_key": f"{uid}_{received_ts}_{home}_{away}_{half_key}",
        "event": {
            "provider": "boltodds",
            "universal_id": uid,
            "action": "match_update",
            "provider_timestamp": str(datetime.fromtimestamp(received_ts, tz=timezone.utc).isoformat()),
            "game": uid,
            "home_team": "ATL Braves",
            "away_team": "PHI Phillies",
            "period": period,
            "home_score": int(home),
            "away_score": int(away),
            "match_completed": bool(match_completed),
            "raw_payload": {
                "state": {
                    "inning": int(inning),
                    "topOfInning": bool(top_of_inning),
                    "out": 0,
                    "ball": 0,
                    "strike": 0,
                    "base1": False,
                    "base2": False,
                    "base3": False,
                    "matchCompleted": bool(match_completed),
                }
            },
        },
    }


def _write_capture_manifest(
    *,
    root: Path,
    provider: str,
    rows_by_file: dict[str, list[dict[str, object]]],
) -> Path:
    parsed_dir = root / "parsed"
    for rel, rows in rows_by_file.items():
        path = parsed_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, sort_keys=False, default=str))
                f.write("\n")
    manifest = {
        "provider": provider,
        "parsed_dir": str(parsed_dir),
        "raw_dir": str(root / "raw"),
        "started_at_utc": "2026-04-18T00:00:00Z",
        "ended_at_utc": "2026-04-18T00:10:00Z",
        "resolved_ids": [],
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


def test_hotpath_replay_emits_expected_intents_and_correctness(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
                _score_event(uid=uid, home=1, away=0, inning=1, half="end", received_ts=1002),
                _score_event(uid=uid, home=4, away=1, inning=9, half="end", received_ts=1003, match_completed=True),
            ],
        },
    )
    with open_database(runtime) as db:
        summary = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
            ),
        )
    payload = json.loads(Path(summary.summary_path).read_text(encoding="utf-8"))
    keys = {str(x.get("strategy_key")): x for x in payload.get("evaluated_intents", [])}
    assert any(k.endswith(":TOTAL:OVER:0.5") for k in keys)
    assert any(k.endswith(":TOTAL:UNDER:6.5") for k in keys)
    assert int(payload["counts"]["intents_attempted"]) >= 2
    assert int(payload["counts"]["incorrect"]) == 0


def test_hotpath_replay_correctness_for_nrfi_moneyline_and_spread(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime, totals_lines=(1.5,))
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
                _score_event(uid=uid, home=1, away=0, inning=1, half="end", received_ts=1002),
                _score_event(uid=uid, home=4, away=1, inning=9, half="end", received_ts=1003, match_completed=False),
                _score_event(uid=uid, home=4, away=1, inning=9, half="end", received_ts=1004, match_completed=True),
                _score_event(uid=uid, home=4, away=1, inning=9, half="end", received_ts=1005, match_completed=False),
            ],
        },
    )
    with open_database(runtime) as db:
        summary = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
            ),
        )
    payload = json.loads(Path(summary.summary_path).read_text(encoding="utf-8"))
    keys = {str(x.get("strategy_key")): x for x in payload.get("evaluated_intents", [])}
    assert keys
    assert int(payload["counts"]["incorrect"]) == 0
    for key, row in keys.items():
        if key.endswith(":NRFI:YES") or key.endswith(":MONEYLINE:HOME") or key.endswith(":SPREAD:HOME:1.5"):
            assert str(row.get("correctness")) == "correct"


def test_hotpath_replay_timeline_is_deterministically_ordered(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1002),
            ],
            f"provider=boltodds/stream=scores/date=2026-04-17/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1001),
                _score_event(uid=uid, home=2, away=0, inning=1, half="top", received_ts=1003),
            ],
        },
    )
    with open_database(runtime) as db:
        summary = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
            ),
        )
    rows = []
    with Path(summary.timeline_path).open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    assert [int(x["received_ts"]) for x in rows] == [1001, 1002, 1003]
    assert [int(x["seq"]) for x in rows] == [1, 2, 3]


def test_hotpath_replay_records_cooldown_and_one_shot_drops(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1001),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1002),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1004),
            ],
        },
    )
    with open_database(runtime) as db:
        summary = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
            ),
        )
    assert int(summary.n_drops_cooldown) >= 1
    assert int(summary.n_drops_one_shot) >= 1


def test_hotpath_replay_records_debounce_drop_when_cooldown_disabled(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1002),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1003),
            ],
        },
    )
    with open_database(runtime) as db:
        summary = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
                decision_cooldown_seconds=0.0,
                decision_debounce_seconds=10.0,
            ),
        )
    assert int(summary.n_drops_debounce) >= 1


def test_hotpath_replay_partial_final_state_marks_unknown(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
            ],
        },
    )
    with open_database(runtime) as db:
        summary = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
            ),
        )
    assert int(summary.n_unknown) >= 1


def test_hotpath_replay_cli_json_and_filtering(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
            ],
            "provider=boltodds/stream=scores/date=2026-04-18/game=gid_other.jsonl": [
                _score_event(uid="gid_other", home=0, away=0, inning=1, half="top", received_ts=1000),
            ],
        },
    )

    parser = build_parser()
    args_ok = parser.parse_args(
        [
            "hotpath",
            "replay",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
            "--capture-manifest",
            str(manifest),
            "--universal-id",
            uid,
            "--format",
            "json",
        ]
    )
    code_ok = run_hotpath_replay(args_ok, logger=logging.getLogger("polybot2.test.replay"))
    assert code_ok == 0

    args_bad = parser.parse_args(
        [
            "hotpath",
            "replay",
            "--db",
            runtime.db_path,
            "--provider",
            "boltodds",
            "--league",
            "mlb",
            "--link-run-id",
            str(run_id),
            "--capture-manifest",
            str(manifest),
            "--universal-id",
            "gid_missing",
        ]
    )
    code_bad = run_hotpath_replay(args_bad, logger=logging.getLogger("polybot2.test.replay"))
    assert code_bad == 1


def test_hotpath_replay_profiling_omits_latency_section(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
            ],
        },
    )
    with open_database(runtime) as db:
        summary = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
                profiling_enabled=True,
            ),
        )
    payload = json.loads(Path(summary.summary_path).read_text(encoding="utf-8"))
    assert "latency_ns" not in payload
    counts = payload.get("counts") if isinstance(payload, dict) else {}
    assert isinstance(counts, dict)
    assert int(counts.get("events_total") or 0) >= 2
    assert int(counts.get("events_material") or 0) >= 1


def test_hotpath_replay_counts_include_action_vs_no_action(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
            ],
        },
    )
    with open_database(runtime) as db:
        summary = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
                profiling_enabled=True,
            ),
        )
    payload = json.loads(Path(summary.summary_path).read_text(encoding="utf-8"))
    assert "latency_ns" not in payload
    counts = payload.get("counts") if isinstance(payload, dict) else {}
    assert isinstance(counts, dict)
    assert int(counts.get("events_total") or 0) == 2
    assert int(counts.get("events_material") or 0) >= 1
    assert int(counts.get("events_non_material") or 0) >= 0
    assert int(counts.get("events_material") or 0) + int(counts.get("events_non_material") or 0) == 2


def test_hotpath_replay_burst_mode_is_deterministic_and_preserves_correctness(tmp_path: Path) -> None:
    runtime = DataRuntimeConfig(db_path=str(tmp_path / "db.sqlite"))
    run_id, uid = _seed_run(runtime=runtime)
    cap_root = tmp_path / "capture"
    manifest = _write_capture_manifest(
        root=cap_root,
        provider="boltodds",
        rows_by_file={
            f"provider=boltodds/stream=scores/date=2026-04-18/game={uid}.jsonl": [
                _score_event(uid=uid, home=0, away=0, inning=1, half="top", received_ts=1000),
                _score_event(uid=uid, home=1, away=0, inning=1, half="top", received_ts=1001),
                _score_event(uid=uid, home=1, away=0, inning=1, half="end", received_ts=1002),
                _score_event(uid=uid, home=4, away=1, inning=9, half="end", received_ts=1003, match_completed=True),
            ],
        },
    )
    with open_database(runtime) as db:
        captured = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
                timestamp_mode="captured",
                profiling_enabled=True,
            ),
        )
        burst = run_hotpath_replay_api(
            db=db,
            config=ReplayConfig(
                provider="boltodds",
                league="mlb",
                run_id=run_id,
                capture_manifest=str(manifest),
                mode="as_fast",
                timestamp_mode="burst",
                burst_interval_ms=2,
                profiling_enabled=True,
            ),
        )

    assert int(captured.n_intents_attempted) == int(burst.n_intents_attempted)
    assert int(captured.n_correct) == int(burst.n_correct)
    assert int(captured.n_incorrect) == int(burst.n_incorrect)

    burst_rows = []
    with Path(burst.timeline_path).open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                burst_rows.append(json.loads(line))
    recv_vals = [int(r.get("recv_monotonic_ns") or 0) for r in burst_rows]
    deltas = [b - a for a, b in zip(recv_vals[:-1], recv_vals[1:])]
    assert deltas
    assert all(int(d) == 2_000_000 for d in deltas)
