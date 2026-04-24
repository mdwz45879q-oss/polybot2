"""Data/provider command handlers."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import signal
import statistics
import tempfile
import time
from typing import Any
from zoneinfo import ZoneInfo

from polybot2._cli.common import _color
from polybot2._cli.common import _int_or_none
from polybot2._cli.common import _parse_int_list
from polybot2._cli.common import _render_table
from polybot2._cli.common import _resolve_provider_name
from polybot2._cli.common import _runtime_from_args
from polybot2.data import DataRuntimeConfig
from polybot2.data import MarketSync as _MarketSync
from polybot2.data import MarketSyncConfig
from polybot2.data import open_database
from polybot2.providers import sync_provider_games
from polybot2.sports import JsonlRawFrameRecorder
from polybot2.sports import JsonlUpdateRecorder
from polybot2.sports import build_sports_provider as _build_sports_provider
from polybot2.sports import capture_stream_profile

# Patchable dependency hooks for tests.
MarketSync = _MarketSync
build_sports_provider = _build_sports_provider

async def run_data_sync(args: Any, *, logger: logging.Logger) -> int:
    if not bool(getattr(args, "sync_markets", False)):
        logger.error("No data sync action selected. Use --markets.")
        return 1
    runtime = _runtime_from_args(args)
    batch_size = _int_or_none(getattr(args, "batch_size", None))
    concurrency = _int_or_none(getattr(args, "concurrency", None))
    max_rps = _int_or_none(getattr(args, "max_rps", None))
    resolved_max_pages = _int_or_none(getattr(args, "resolved_max_pages", None))
    open_max_pages = _int_or_none(getattr(args, "open_max_pages", None))
    open_only = bool(getattr(args, "open_only", False))
    skip_reference_sync = bool(getattr(args, "skip_reference_sync", False))
    enable_payload_artifacts = bool(getattr(args, "enable_payload_artifacts", False))
    disable_payload_artifacts = bool(getattr(args, "disable_payload_artifacts", False))
    fast_mode = bool(getattr(args, "fast_mode", False))
    if enable_payload_artifacts and disable_payload_artifacts:
        logger.error("Choose only one of --enable-payload-artifacts or --disable-payload-artifacts.")
        return 2
    cfg_kwargs: dict[str, Any] = {"gamma_api": runtime.gamma_api}
    if batch_size is not None:
        cfg_kwargs["batch_size"] = int(batch_size)
    if concurrency is not None:
        cfg_kwargs["concurrency"] = int(concurrency)
    if max_rps is not None:
        cfg_kwargs["max_rps"] = int(max_rps)
    if resolved_max_pages is not None:
        cfg_kwargs["resolved_max_pages"] = int(resolved_max_pages)
    if open_max_pages is not None:
        cfg_kwargs["open_max_pages"] = int(open_max_pages)
    cfg_kwargs["open_only"] = bool(open_only)
    cfg_kwargs["enable_reference_sync"] = not skip_reference_sync
    if enable_payload_artifacts:
        cfg_kwargs["enable_payload_artifacts"] = True
    elif disable_payload_artifacts:
        cfg_kwargs["enable_payload_artifacts"] = False
    cfg_kwargs["fast_mode"] = bool(fast_mode)
    with open_database(runtime) as db:
        sync = MarketSync(db=db, config=MarketSyncConfig(**cfg_kwargs))
        count = await sync.run()
        stats = sync.last_run_stats or {}
    stage = stats.get("stage_timing_s") if isinstance(stats.get("stage_timing_s"), dict) else {}
    logger.info(
        "polybot2 data sync complete: markets=%d elapsed_s=%.3f pages=%s rows=%s retries=%s failures=%s stage(fetch=%.3f,db=%.3f,refs=%.3f,artifact=%.3f) cfg(concurrency=%s,max_rps=%s,batch=%s,refs=%s,artifacts=%s,fast_mode=%s,hash_lineage=%s)",
        int(count),
        float(stats.get("elapsed_s") or 0.0),
        stats.get("total_pages_processed"),
        stats.get("total_rows_processed"),
        stats.get("retries_total"),
        stats.get("hard_failures"),
        float(stage.get("fetch") or 0.0),
        float(stage.get("db_upsert") or 0.0),
        float(stage.get("reference_sync") or 0.0),
        float(stage.get("artifact_write") or 0.0),
        (stats.get("config") or {}).get("concurrency"),
        (stats.get("config") or {}).get("max_rps"),
        (stats.get("config") or {}).get("batch_size"),
        (stats.get("config") or {}).get("enable_reference_sync"),
        (stats.get("config") or {}).get("enable_payload_artifacts"),
        (stats.get("config") or {}).get("fast_mode"),
        (stats.get("config") or {}).get("compute_lineage_hash"),
    )
    return 0


async def run_data_benchmark_markets(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)
    conc_values = _parse_int_list(
        str(getattr(args, "concurrency_values", "")),
        default=[20, 30, 40, 60],
    )
    rps_values = _parse_int_list(
        str(getattr(args, "max_rps_values", "")),
        default=[24, 48, 96, 160, 220],
    )
    repeats = max(1, int(getattr(args, "repeats", 3) or 3))
    batch_size = max(1, int(getattr(args, "batch_size", 500) or 500))
    resolved_max_pages = max(1, int(getattr(args, "resolved_max_pages", 20) or 20))
    open_max_pages = max(1, int(getattr(args, "open_max_pages", 20) or 20))
    request_delay = float(getattr(args, "request_delay", 0.0) or 0.0)
    fetch_max_retries = max(1, int(getattr(args, "fetch_max_retries", 3) or 3))
    skip_reference_sync = bool(getattr(args, "skip_reference_sync", False))
    enable_payload_artifacts = bool(getattr(args, "enable_payload_artifacts", False))
    disable_payload_artifacts = bool(getattr(args, "disable_payload_artifacts", False))
    fast_mode = bool(getattr(args, "fast_mode", False))
    if enable_payload_artifacts and disable_payload_artifacts:
        logger.error("Choose only one of --enable-payload-artifacts or --disable-payload-artifacts.")
        return 2
    payload_artifacts_enabled = bool(enable_payload_artifacts and not disable_payload_artifacts)
    out_dir = Path(str(getattr(args, "output_dir", "artifacts/polybot2_market_benchmarks"))).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    jsonl_path = out_dir / f"benchmark_{ts}.jsonl"
    summary_path = out_dir / f"benchmark_{ts}_summary.json"

    rows: list[dict[str, Any]] = []
    matrix = [(c, r) for c in conc_values for r in rps_values]
    logger.info(
        "starting market benchmark: cases=%d repeats=%d batch_size=%d resolved_max_pages=%d open_max_pages=%d refs=%s artifacts=%s fast_mode=%s jsonl=%s",
        len(matrix),
        repeats,
        batch_size,
        resolved_max_pages,
        open_max_pages,
        not skip_reference_sync,
        payload_artifacts_enabled,
        fast_mode,
        str(jsonl_path),
    )
    for rep in range(1, repeats + 1):
        for concurrency, max_rps in matrix:
            case_started = time.perf_counter()
            rec: dict[str, Any] = {
                "repeat": int(rep),
                "concurrency": int(concurrency),
                "max_rps": int(max_rps),
                "batch_size": int(batch_size),
                "resolved_max_pages": int(resolved_max_pages),
                "open_max_pages": int(open_max_pages),
                "status": "ok",
                "markets": 0,
                "elapsed_s": 0.0,
                "stats": {},
                "error": "",
            }
            try:
                with tempfile.TemporaryDirectory(prefix="polybot2_bench_") as td:
                    db_path = str(Path(td) / "benchmark.sqlite")
                    payload_dir = str(Path(td) / "payloads")
                    case_runtime = DataRuntimeConfig.from_env(
                        {
                            "db_path": db_path,
                            "gamma_api": runtime.gamma_api,
                        }
                    )
                    with open_database(case_runtime) as db:
                        sync = MarketSync(
                            db=db,
                            config=MarketSyncConfig(
                                gamma_api=case_runtime.gamma_api,
                                batch_size=batch_size,
                                request_delay=request_delay,
                                concurrency=concurrency,
                                max_rps=max_rps,
                                fetch_max_retries=fetch_max_retries,
                                resolved_max_pages=resolved_max_pages,
                                open_max_pages=open_max_pages,
                                enable_reference_sync=(not skip_reference_sync),
                                enable_payload_artifacts=payload_artifacts_enabled,
                                fast_mode=fast_mode,
                                payload_artifact_dir=payload_dir,
                            ),
                        )
                        count = await sync.run()
                        stats = sync.last_run_stats or {}
                        rec["markets"] = int(count)
                        rec["stats"] = stats
            except Exception as exc:
                rec["status"] = "error"
                rec["error"] = str(exc)
            rec["elapsed_s"] = float(time.perf_counter() - case_started)
            with jsonl_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, sort_keys=True, default=str) + "\n")
            rows.append(rec)
            logger.info(
                "benchmark case done: rep=%d concurrency=%d max_rps=%d status=%s markets=%d elapsed_s=%.3f",
                rep,
                concurrency,
                max_rps,
                rec["status"],
                int(rec.get("markets") or 0),
                float(rec.get("elapsed_s") or 0.0),
            )

    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in rows:
        key = (int(row["concurrency"]), int(row["max_rps"]))
        grouped.setdefault(key, []).append(row)

    summary_rows: list[dict[str, Any]] = []
    for (concurrency, max_rps), items in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
        throughputs = []
        hard_failures = 0
        retries = 0
        errors = 0
        for item in items:
            if str(item.get("status")) != "ok":
                errors += 1
                continue
            stats = item.get("stats") if isinstance(item.get("stats"), dict) else {}
            elapsed = float(stats.get("elapsed_s") or item.get("elapsed_s") or 0.0)
            markets = int(stats.get("total_markets_processed") or item.get("markets") or 0)
            if elapsed > 0:
                throughputs.append(markets / elapsed)
            hard_failures += int(stats.get("hard_failures") or 0)
            retries += int(stats.get("retries_total") or 0)
        summary_rows.append(
            {
                "concurrency": int(concurrency),
                "max_rps": int(max_rps),
                "samples_ok": int(len(items) - errors),
                "samples_error": int(errors),
                "mean_markets_per_s": float(statistics.mean(throughputs)) if throughputs else 0.0,
                "stdev_markets_per_s": float(statistics.pstdev(throughputs)) if len(throughputs) > 1 else 0.0,
                "hard_failures": int(hard_failures),
                "retries_total": int(retries),
                "mean_fetch_s": float(
                    statistics.mean(
                        float((item.get("stats") or {}).get("stage_timing_s", {}).get("fetch") or 0.0)  # type: ignore[union-attr]
                        for item in items
                        if str(item.get("status")) == "ok"
                    )
                )
                if any(str(item.get("status")) == "ok" for item in items)
                else 0.0,
                "mean_db_upsert_s": float(
                    statistics.mean(
                        float((item.get("stats") or {}).get("stage_timing_s", {}).get("db_upsert") or 0.0)  # type: ignore[union-attr]
                        for item in items
                        if str(item.get("status")) == "ok"
                    )
                )
                if any(str(item.get("status")) == "ok" for item in items)
                else 0.0,
                "mean_reference_sync_s": float(
                    statistics.mean(
                        float((item.get("stats") or {}).get("stage_timing_s", {}).get("reference_sync") or 0.0)  # type: ignore[union-attr]
                        for item in items
                        if str(item.get("status")) == "ok"
                    )
                )
                if any(str(item.get("status")) == "ok" for item in items)
                else 0.0,
                "mean_artifact_write_s": float(
                    statistics.mean(
                        float((item.get("stats") or {}).get("stage_timing_s", {}).get("artifact_write") or 0.0)  # type: ignore[union-attr]
                        for item in items
                        if str(item.get("status")) == "ok"
                    )
                )
                if any(str(item.get("status")) == "ok" for item in items)
                else 0.0,
            }
        )

    safe_rows = [r for r in summary_rows if int(r["samples_error"]) == 0 and int(r["hard_failures"]) == 0]
    ranked = sorted(
        safe_rows if safe_rows else summary_rows,
        key=lambda r: (
            -float(r["mean_markets_per_s"]),
            float(r["stdev_markets_per_s"]),
            int(r["retries_total"]),
            int(r["concurrency"]),
            int(r["max_rps"]),
        ),
    )
    recommended = ranked[0] if ranked else None
    summary_payload = {
        "timestamp": int(ts),
        "repeats": int(repeats),
        "matrix": {
            "concurrency_values": conc_values,
            "max_rps_values": rps_values,
            "batch_size": int(batch_size),
            "resolved_max_pages": int(resolved_max_pages),
            "open_max_pages": int(open_max_pages),
            "fetch_max_retries": int(fetch_max_retries),
            "request_delay": float(request_delay),
            "enable_reference_sync": bool(not skip_reference_sync),
            "enable_payload_artifacts": bool(payload_artifacts_enabled),
            "fast_mode": bool(fast_mode),
        },
        "results_jsonl": str(jsonl_path),
        "summary_rows": summary_rows,
        "recommended": recommended,
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    logger.info(
        "market benchmark complete: cases=%d rows=%d recommended=%s summary=%s",
        len(matrix),
        len(rows),
        json.dumps(recommended, sort_keys=True, default=str) if recommended is not None else "{}",
        str(summary_path),
    )
    logger.info(
        "Benchmark Summary\n%s",
        _render_table(
            rows=sorted(summary_rows, key=lambda r: (-float(r["mean_markets_per_s"]), int(r["samples_error"]), int(r["hard_failures"]))),
            columns=[
                ("concurrency", "concurrency"),
                ("max_rps", "max_rps"),
                ("samples_ok", "samples_ok"),
                ("samples_error", "samples_error"),
                ("mean_markets_per_s", "mean_markets_per_s"),
                ("stdev_markets_per_s", "stdev_markets_per_s"),
                ("hard_failures", "hard_failures"),
                ("retries_total", "retries_total"),
                ("mean_fetch_s", "mean_fetch_s"),
                ("mean_db_upsert_s", "mean_db_upsert_s"),
                ("mean_reference_sync_s", "mean_reference_sync_s"),
                ("mean_artifact_write_s", "mean_artifact_write_s"),
            ],
        ),
    )
    return 0


def run_provider_sync(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)
    provider = _resolve_provider_name(args=args, logger=logger, context="provider sync")
    if provider is None:
        return 1
    with open_database(runtime) as db:
        res = sync_provider_games(db=db, provider=provider)
    if res.status != "ok":
        logger.error("provider sync failed: provider=%s status=%s reason=%s", res.provider, res.status, res.reason)
        return 1
    logger.info("provider sync complete: provider=%s rows=%d", res.provider, int(res.n_rows))
    return 0


def _sanitize_capture_component(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    out: list[str] = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("_")
    sanitized = "".join(out).strip("_")
    return sanitized or "unknown"


def _dedup_ids(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _load_ids_from_file(*, path: Path, ids_var: str) -> list[str]:
    suffix = str(path.suffix or "").strip().lower()
    text = path.read_text(encoding="utf-8")
    if suffix in {".txt", ".list", ".ids"}:
        return _dedup_ids([line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("#")])
    if suffix == ".json":
        payload = json.loads(text)
        if isinstance(payload, list):
            return _dedup_ids([str(x).strip() for x in payload if str(x).strip()])
        if isinstance(payload, dict):
            vals = payload.get("universal_ids")
            if isinstance(vals, list):
                return _dedup_ids([str(x).strip() for x in vals if str(x).strip()])
        raise ValueError("json_file_must_be_list_or_object_with_universal_ids")
    if suffix == ".py":
        mod = ast.parse(text, filename=str(path))
        wanted = str(ids_var or "UNIVERSAL_IDS").strip() or "UNIVERSAL_IDS"
        for node in mod.body:
            if isinstance(node, ast.Assign):
                targets = [t for t in node.targets if isinstance(t, ast.Name)]
                if not any(str(t.id) == wanted for t in targets):
                    continue
                value = ast.literal_eval(node.value)
                if isinstance(value, (list, tuple, set)):
                    return _dedup_ids([str(x).strip() for x in value if str(x).strip()])
                raise ValueError("python_ids_var_must_be_list_like")
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and str(node.target.id) == wanted:
                value = ast.literal_eval(node.value) if node.value is not None else []
                if isinstance(value, (list, tuple, set)):
                    return _dedup_ids([str(x).strip() for x in value if str(x).strip()])
                raise ValueError("python_ids_var_must_be_list_like")
        raise ValueError("python_ids_var_not_found")
    raise ValueError("unsupported_ids_file_extension")


def _provider_record_game_date_et(record: Any) -> str:
    ts = _int_or_none(getattr(record, "start_ts_utc", None))
    if ts is not None:
        return datetime.fromtimestamp(int(ts), tz=ZoneInfo("America/New_York")).date().isoformat()
    when_raw = str(getattr(record, "when_raw", "") or "").strip()
    if not when_raw:
        return ""
    try:
        if when_raw.endswith("Z"):
            ts_val = int(datetime.fromisoformat(when_raw.replace("Z", "+00:00")).timestamp())
        else:
            dt = datetime.fromisoformat(when_raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            ts_val = int(dt.timestamp())
        return datetime.fromtimestamp(ts_val, tz=ZoneInfo("America/New_York")).date().isoformat()
    except Exception:
        return ""


def _count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _count_raw_lines_for_game(*, raw_root: Path, provider: str, universal_id: str) -> int:
    p = raw_root / f"provider={_sanitize_capture_component(provider)}"
    uid = _sanitize_capture_component(universal_id)
    total = 0
    for stream_dir in p.glob("stream=*"):
        total += _count_jsonl_lines(stream_dir / f"game={uid}.jsonl")
    return total


def run_provider_capture(args: Any, *, logger: logging.Logger) -> int:
    provider_name = _resolve_provider_name(args=args, logger=logger, context="provider capture")
    if provider_name is None:
        return 1
    league = str(getattr(args, "league", "")).strip().lower()
    if not league:
        logger.error("--league is required")
        return 1
    out_arg = str(getattr(args, "out", "")).strip()
    if not out_arg:
        logger.error("--out is required")
        return 1

    today_mode = bool(getattr(args, "today", False))
    ids_file_text = str(getattr(args, "ids_file", "") or "").strip()
    ids_var = str(getattr(args, "ids_var", "UNIVERSAL_IDS") or "UNIVERSAL_IDS").strip() or "UNIVERSAL_IDS"
    cli_ids = _dedup_ids([str(x).strip() for x in (getattr(args, "universal_ids", None) or []) if str(x).strip()])

    selection_mode = "cli_ids"
    requested_ids: list[str] = []
    target_date_et = ""
    if today_mode:
        selection_mode = "today"
        date_arg = str(getattr(args, "date_et", "") or "").strip()
        if date_arg:
            try:
                datetime.strptime(date_arg, "%Y-%m-%d")
            except ValueError:
                logger.error("--date-et must be YYYY-MM-DD")
                return 1
            target_date_et = date_arg
        else:
            target_date_et = datetime.now(tz=ZoneInfo("America/New_York")).date().isoformat()
    elif ids_file_text:
        selection_mode = "ids_file"
        ids_file = Path(ids_file_text).expanduser().resolve()
        if not ids_file.exists():
            logger.error("--ids-file not found: %s", str(ids_file))
            return 1
        try:
            requested_ids = _load_ids_from_file(path=ids_file, ids_var=ids_var)
        except Exception as exc:
            logger.error("failed to parse --ids-file %s: %s", str(ids_file), exc)
            return 1
    else:
        requested_ids = list(cli_ids)

    tail_raw = getattr(args, "tail_seconds", 120.0)
    max_duration_raw = getattr(args, "max_duration_seconds", 21600.0)
    read_timeout_raw = getattr(args, "read_timeout_seconds", 1.0)
    tail_seconds = max(0.0, float(120.0 if tail_raw is None else tail_raw))
    max_duration_seconds = max(1.0, float(21600.0 if max_duration_raw is None else max_duration_raw))
    read_timeout_seconds = max(0.05, float(1.0 if read_timeout_raw is None else read_timeout_raw))

    capture_tag = ""
    if selection_mode == "today":
        capture_tag = f"{league}_{target_date_et}"
    elif len(requested_ids) == 1:
        capture_tag = requested_ids[0]
    elif requested_ids:
        capture_tag = f"{league}_{len(requested_ids)}games"
    else:
        capture_tag = f"{league}_selection"

    start_wall = time.time()
    started_at = datetime.now(timezone.utc)
    run_dir = Path(out_arg).expanduser().resolve() / (
        f"capture_{provider_name}_{_sanitize_capture_component(capture_tag)}_{started_at.strftime('%Y%m%dT%H%M%SZ')}"
    )
    parsed_root = run_dir / "parsed"
    raw_root = run_dir / "raw"
    run_dir.mkdir(parents=True, exist_ok=True)
    parsed_recorder = JsonlUpdateRecorder(parsed_root)
    raw_recorder = JsonlRawFrameRecorder(raw_root, default_universal_id="")
    try:
        provider = build_sports_provider(
            provider_name=provider_name,
            recorder=parsed_recorder,
            raw_frame_recorder=raw_recorder,
            logger=logger,
        )
    except ValueError as exc:
        reason = str(exc)
        if reason == "missing_BOLTODDS_API_KEY":
            logger.error("BOLTODDS_API_KEY is required for provider capture")
        elif reason == "missing_kalstrop_credentials":
            logger.error("Kalstrop credentials are required for provider capture (KALSTROP_* or legacy CLIENT_ID/SHARED_SECRET_RAW)")
        else:
            logger.error("provider capture failed to initialize provider: %s", reason)
        return 1

    stop_reason = "unknown"
    manual_stop = False
    seen_any_payload = False
    connect_grace_seconds = max(5.0, min(30.0, max_duration_seconds))
    missing_ids: list[str] = []
    resolved_ids: list[str] = []
    mismatched_ids: list[str] = []
    per_game_state: dict[str, dict[str, Any]] = {}
    provider_records: dict[str, Any] = {}

    prev_int = signal.getsignal(signal.SIGINT)
    prev_term = signal.getsignal(signal.SIGTERM)

    def _handle_stop(_sig: int, _frame: Any) -> None:
        nonlocal manual_stop
        manual_stop = True

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    try:
        catalog = provider.load_game_catalog()
        if selection_mode == "today":
            requested_ids = []
            for record in catalog:
                rec_league = " ".join(str(getattr(record, "league_key", "") or "").strip().lower().split())
                if not rec_league or rec_league != league:
                    continue
                rec_date_et = _provider_record_game_date_et(record)
                if rec_date_et != target_date_et:
                    continue
                uid = str(getattr(record, "provider_game_id", "") or "").strip()
                if uid:
                    requested_ids.append(uid)
            requested_ids = _dedup_ids(requested_ids)
            if not requested_ids:
                logger.error("no provider games found for --today provider=%s league=%s date_et=%s", provider_name, league, target_date_et)
                stop_reason = "no_games_for_today"
                return 1

        requested_ids = _dedup_ids(requested_ids)
        if not requested_ids:
            logger.error("no universal ids selected for capture")
            stop_reason = "no_selected_ids"
            return 1

        for uid in requested_ids:
            rec = provider.get_provider_record(uid)
            if rec is None:
                missing_ids.append(uid)
                continue
            rec_league = " ".join(str(getattr(rec, "league_key", "") or "").strip().lower().split())
            if rec_league and rec_league != league:
                mismatched_ids.append(uid)
                continue
            provider_records[uid] = rec
            resolved_ids.append(uid)

        if missing_ids:
            logger.warning("provider capture skipping missing ids: provider=%s missing=%s", provider_name, ",".join(missing_ids))
        if mismatched_ids:
            logger.warning("provider capture skipping league-mismatched ids: provider=%s league=%s ids=%s", provider_name, league, ",".join(mismatched_ids))
        if not resolved_ids:
            logger.error("provider capture has no valid ids after resolution: provider=%s requested=%d", provider_name, len(requested_ids))
            stop_reason = "no_resolved_ids"
            return 1

        for uid in resolved_ids:
            rec = provider_records.get(uid)
            kickoff_ts_utc: int | None = None
            kickoff_raw = None if rec is None else getattr(rec, "start_ts_utc", None)
            try:
                kickoff_ts_utc = None if kickoff_raw is None else int(kickoff_raw)
            except (TypeError, ValueError):
                kickoff_ts_utc = None
            anchor_ts = float(start_wall)
            if kickoff_ts_utc is not None and kickoff_ts_utc > 0:
                anchor_ts = max(float(start_wall), float(kickoff_ts_utc))
            per_game_state[uid] = {
                "completed_seen_at": None,
                "done": False,
                "stop_reason": "",
                "seen_payload": False,
                "parsed_count": 0,
                "kickoff_ts_utc": kickoff_ts_utc,
                "duration_anchor_ts": float(anchor_ts),
                "duration_deadline_ts": float(anchor_ts + max_duration_seconds),
            }

        stream_profile = capture_stream_profile(provider_name)
        run_scores = bool(stream_profile.get("scores"))
        run_odds = bool(stream_profile.get("odds"))
        run_playbyplay = bool(stream_profile.get("playbyplay"))
        provider.start()
        if run_scores:
            provider.subscribe_scores(resolved_ids)
        if run_odds:
            provider.subscribe_odds(resolved_ids)
        if run_playbyplay:
            provider.subscribe_playbyplay(resolved_ids)
        logger.info(
            "provider capture started: provider=%s mode=%s n_games=%d league=%s out=%s streams=scores:%s,odds:%s,playbyplay:%s tail_seconds=%.1f max_duration_seconds=%.1f",
            provider_name,
            selection_mode,
            len(resolved_ids),
            league,
            str(run_dir),
            run_scores,
            run_odds,
            run_playbyplay,
            tail_seconds,
            max_duration_seconds,
        )

        while True:
            if manual_stop:
                stop_reason = "manual_interrupt"
                break

            scores_envs: list[Any] = []
            odds_envs: list[Any] = []
            pbp_envs: list[Any] = []
            try:
                if run_scores:
                    scores_envs = provider.stream_scores(read_timeout_seconds=read_timeout_seconds)
                if run_odds:
                    odds_envs = provider.stream_odds(read_timeout_seconds=read_timeout_seconds)
                if run_playbyplay:
                    pbp_envs = provider.stream_playbyplay(read_timeout_seconds=read_timeout_seconds)
            except Exception as exc:
                logger.warning("provider capture stream read failed (recoverable): %s: %s", type(exc).__name__, exc)
                time.sleep(0.2)

            env_groups = (scores_envs, odds_envs, pbp_envs)
            if any(env_groups):
                seen_any_payload = True
            for envs in env_groups:
                for env in envs:
                    uid = str(getattr(env, "universal_id", "") or "").strip()
                    if uid in per_game_state:
                        per_game_state[uid]["seen_payload"] = True
                        per_game_state[uid]["parsed_count"] = int(per_game_state[uid]["parsed_count"]) + 1

            connect_metrics = provider.get_stream_metrics()
            scores_connected_now = (not run_scores) or int((connect_metrics.get("scores") or {}).get("connect_successes") or 0) > 0
            odds_connected_now = (not run_odds) or int((connect_metrics.get("odds") or {}).get("connect_successes") or 0) > 0
            pbp_connected_now = (not run_playbyplay) or int((connect_metrics.get("playbyplay") or {}).get("connect_successes") or 0) > 0
            if (time.time() - start_wall) >= connect_grace_seconds and not (
                scores_connected_now and odds_connected_now and pbp_connected_now
            ):
                stop_reason = "connection_not_established"
                logger.error(
                    "provider capture failed to establish required streams within %.1fs: scores_connected=%s odds_connected=%s playbyplay_connected=%s",
                    connect_grace_seconds,
                    scores_connected_now,
                    odds_connected_now,
                    pbp_connected_now,
                )
                break

            for env in scores_envs:
                uid = str(getattr(env, "universal_id", "") or "").strip()
                if uid not in per_game_state:
                    continue
                event = env.event
                if hasattr(event, "match_completed") and bool(getattr(event, "match_completed")):
                    if per_game_state[uid]["completed_seen_at"] is None:
                        per_game_state[uid]["completed_seen_at"] = time.time()
                        logger.info("game completion signal detected: universal_id=%s tail_seconds=%.1f", uid, tail_seconds)

            all_done = True
            now2 = time.time()
            for uid, state in per_game_state.items():
                if bool(state["done"]):
                    continue
                completed_seen_at = state.get("completed_seen_at")
                if completed_seen_at is not None and (now2 - float(completed_seen_at)) >= tail_seconds:
                    state["done"] = True
                    state["stop_reason"] = "game_completed_tail"
                    continue
                deadline_ts = float(state.get("duration_deadline_ts") or 0.0)
                if deadline_ts > 0 and now2 >= deadline_ts:
                    state["done"] = True
                    state["stop_reason"] = "max_duration"
                    continue
                all_done = False
            if all_done:
                reasons = [str((per_game_state.get(uid) or {}).get("stop_reason") or "") for uid in resolved_ids]
                if len(resolved_ids) == 1:
                    stop_reason = reasons[0] or "stopped"
                elif reasons and all(r == "game_completed_tail" for r in reasons):
                    stop_reason = "all_games_completed_tail"
                elif reasons and all(r == "max_duration" for r in reasons):
                    stop_reason = "all_games_max_duration"
                else:
                    stop_reason = "all_games_terminal"
                break
    finally:
        try:
            provider.close()
        except Exception:
            pass
        signal.signal(signal.SIGINT, prev_int)
        signal.signal(signal.SIGTERM, prev_term)

    ended_at = datetime.now(timezone.utc)
    stream_metrics = provider.get_stream_metrics()
    parsed_stats = parsed_recorder.stats()
    raw_stats = raw_recorder.stats()

    for uid, state in per_game_state.items():
        if bool(state.get("done")):
            continue
        if stop_reason in {"manual_interrupt", "max_duration", "connection_not_established"}:
            state["stop_reason"] = stop_reason
        elif not str(state.get("stop_reason") or ""):
            state["stop_reason"] = "stopped"

    per_game_summary: list[dict[str, Any]] = []
    for uid in resolved_ids:
        record = provider_records.get(uid)
        state = per_game_state.get(uid, {})
        per_game_summary.append(
            {
                "universal_id": uid,
                "game_label": "" if record is None else str(getattr(record, "game_label", "") or ""),
                "provider_league_key": "" if record is None else str(getattr(record, "league_key", "") or ""),
                "game_date_et": ("" if record is None else _provider_record_game_date_et(record)),
                "seen_payload": bool(state.get("seen_payload")),
                "parsed_count": int(state.get("parsed_count") or 0),
                "raw_count": int(_count_raw_lines_for_game(raw_root=raw_root, provider=provider_name, universal_id=uid)),
                "stop_reason": str(state.get("stop_reason") or ""),
                "completed_seen": state.get("completed_seen_at") is not None,
                "kickoff_ts_utc": state.get("kickoff_ts_utc"),
                "duration_anchor_ts": state.get("duration_anchor_ts"),
                "duration_deadline_ts": state.get("duration_deadline_ts"),
            }
        )

    n_games_completed = sum(1 for row in per_game_summary if bool(row.get("completed_seen")))
    incomplete_ids = [str(row.get("universal_id") or "") for row in per_game_summary if not bool(row.get("completed_seen"))]

    manifest = {
        "provider": provider_name,
        "league": league,
        "selection_mode": selection_mode,
        "date_et": target_date_et if selection_mode == "today" else "",
        "requested_ids": requested_ids,
        "resolved_ids": resolved_ids,
        "missing_ids": missing_ids,
        "mismatched_ids": mismatched_ids,
        "universal_id": (resolved_ids[0] if len(resolved_ids) == 1 else ""),
        "game_label": (
            ""
            if len(resolved_ids) != 1
            else str(getattr(provider_records.get(resolved_ids[0]), "game_label", "") or "")
        ),
        "provider_league_key": (
            ""
            if len(resolved_ids) != 1
            else str(getattr(provider_records.get(resolved_ids[0]), "league_key", "") or "")
        ),
        "out_dir": str(run_dir),
        "parsed_dir": str(parsed_root),
        "raw_dir": str(raw_root),
        "started_at_utc": started_at.isoformat(),
        "ended_at_utc": ended_at.isoformat(),
        "duration_seconds": round(max(0.0, time.time() - start_wall), 3),
        "stop_reason": stop_reason,
        "options": {
            "tail_seconds": tail_seconds,
            "max_duration_seconds": max_duration_seconds,
            "read_timeout_seconds": read_timeout_seconds,
            "max_duration_mode": "per_game_from_max(command_start,kickoff_ts_utc)",
        },
        "counts": {
            "parsed": parsed_stats,
            "raw": raw_stats,
        },
        "stream_metrics": stream_metrics,
        "seen_any_payload": bool(seen_any_payload),
        "catalog_size": len(catalog) if "catalog" in locals() else None,
        "n_games_completed": int(n_games_completed),
        "n_games_incomplete": int(max(0, len(resolved_ids) - n_games_completed)),
        "incomplete_ids": incomplete_ids,
        "per_game": per_game_summary,
    }
    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str), encoding="utf-8")

    stream_profile = capture_stream_profile(provider_name)
    scores_connected = (not bool(stream_profile.get("scores"))) or int((stream_metrics.get("scores") or {}).get("connect_successes") or 0) > 0
    odds_connected = (not bool(stream_profile.get("odds"))) or int((stream_metrics.get("odds") or {}).get("connect_successes") or 0) > 0
    pbp_connected = (not bool(stream_profile.get("playbyplay"))) or int((stream_metrics.get("playbyplay") or {}).get("connect_successes") or 0) > 0
    if not (scores_connected and odds_connected and pbp_connected):
        logger.error(
            "provider capture failed to establish required streams: scores_connected=%s odds_connected=%s playbyplay_connected=%s manifest=%s",
            scores_connected,
            odds_connected,
            pbp_connected,
            str(manifest_path),
        )
        return 1
    if stop_reason in {"no_games_for_today", "no_selected_ids", "no_resolved_ids"}:
        logger.error("provider capture failed: reason=%s manifest=%s", stop_reason, str(manifest_path))
        return 1
    if int(n_games_completed) == int(len(resolved_ids)):
        logger.info(
            "provider capture complete: n_games=%d stop_reason=%s parsed_total=%s raw_total=%s manifest=%s",
            len(resolved_ids),
            stop_reason,
            int(parsed_stats.get("total", 0)),
            int(raw_stats.get("total", 0)),
            str(manifest_path),
        )
    else:
        logger.warning(
            "provider capture stopped with incomplete games: completed=%d/%d stop_reason=%s parsed_total=%s raw_total=%s manifest=%s",
            int(n_games_completed),
            len(resolved_ids),
            stop_reason,
            int(parsed_stats.get("total", 0)),
            int(raw_stats.get("total", 0)),
            str(manifest_path),
        )
    return 0


__all__ = [
    "run_data_sync",
    "run_data_benchmark_markets",
    "run_provider_sync",
    "run_provider_capture",
    "MarketSync",
    "build_sports_provider",
    "_sanitize_capture_component",
    "_dedup_ids",
    "_load_ids_from_file",
    "_provider_record_game_date_et",
    "_count_jsonl_lines",
    "_count_raw_lines_for_game",
]
