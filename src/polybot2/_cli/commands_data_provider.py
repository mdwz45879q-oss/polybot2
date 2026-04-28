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


import os


def run_provider_capture(args: Any, *, logger: logging.Logger) -> int:
    """Record raw Kalstrop score frames to a single JSONL file."""
    from polybot2.sports.factory import build_sports_provider as make_provider

    league = str(getattr(args, "league", "") or "").strip().lower()
    out_dir = str(getattr(args, "out", "") or "").strip()
    if not league or not out_dir:
        logger.error("--league and --out are required")
        return 1

    os.makedirs(out_dir, exist_ok=True)
    provider_name = str(getattr(args, "provider", "") or "kalstrop").strip().lower()
    provider = make_provider(provider_name=provider_name)

    # --- Resolve game IDs ---
    if getattr(args, "today", False):
        target_date_et = str(getattr(args, "date_et", "") or "").strip()
        if not target_date_et:
            target_date_et = datetime.now(tz=ZoneInfo("America/New_York")).date().isoformat()
        logger.info("Loading game catalog for %s %s on %s ET...", provider_name, league, target_date_et)
        catalog = provider.load_game_catalog()
        game_ids: list[str] = []
        seen_leagues: set[str] = set()
        # Normalize the CLI league arg: lowercase, replace spaces with hyphens
        # to match the hyphenated league_key from normalize_league_key().
        league_norm = league.lower().replace(" ", "-")
        for rec in catalog:
            rec_league_key = str(getattr(rec, "league_key", "") or "").strip()
            rec_league_raw = str(getattr(rec, "league_raw", "") or "").strip()
            if rec_league_key:
                seen_leagues.add(rec_league_key)
            if rec_league_raw:
                seen_leagues.add(rec_league_raw)
            # Match against both league_key (normalized, hyphenated) and
            # league_raw (original provider string). Substring match so the
            # user doesn't need the exact string.
            rec_key_lower = rec_league_key.lower()
            rec_raw_lower = rec_league_raw.lower().replace(" ", "-")
            if league_norm not in rec_key_lower and league_norm not in rec_raw_lower:
                continue
            rec_date = _provider_record_game_date_et(rec)
            if rec_date != target_date_et:
                continue
            uid = str(getattr(rec, "provider_game_id", "") or getattr(rec, "uid", "") or "").strip()
            if uid:
                game_ids.append(uid)
        game_ids = sorted(set(game_ids))
        logger.info("Found %d games for %s on %s ET", len(game_ids), league, target_date_et)
        if not game_ids and seen_leagues:
            logger.info("Available leagues in catalog:")
            for lk in sorted(seen_leagues):
                logger.info("  %s", lk)
    else:
        raw_ids = getattr(args, "universal_ids", None) or []
        game_ids = sorted(set(str(uid).strip() for uid in raw_ids if str(uid).strip()))

    if not game_ids:
        logger.error("No game IDs resolved")
        return 1

    for gid in game_ids:
        logger.info("  %s", gid)

    # --- Output file ---
    ts_label = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = os.path.join(out_dir, f"capture_{league}_{ts_label}.jsonl")
    logger.info("Writing to %s", out_path)

    # --- Connect and subscribe ---
    provider.start()
    provider.subscribe_scores(game_ids)

    # --- Recording loop ---
    frame_count = 0
    max_dur = float(getattr(args, "max_duration_seconds", 0) or 21600.0)
    started = time.time()
    stop_reason = "max_duration"

    try:
        with open(out_path, "w", encoding="utf-8") as f:
            while (time.time() - started) < max_dur:
                raw = provider.recv_raw_score_frame(timeout=1.0)
                if raw is None:
                    continue
                ts = time.time()
                try:
                    frame = json.loads(raw)
                except Exception:
                    frame = raw
                f.write(json.dumps({"ts": ts, "frame": frame}) + "\n")
                frame_count += 1
                if frame_count % 100 == 0:
                    f.flush()
    except KeyboardInterrupt:
        stop_reason = "keyboard_interrupt"

    elapsed = time.time() - started
    mins, secs = divmod(int(elapsed), 60)
    logger.info(
        "capture: %d games, %d frames, %dm%02ds, stopped by %s",
        len(game_ids), frame_count, mins, secs, stop_reason,
    )
    return 0


__all__ = [
    "run_data_sync",
    "run_data_benchmark_markets",
    "run_provider_sync",
    "run_provider_capture",
    "MarketSync",
    "build_sports_provider",
    "_dedup_ids",
    "_provider_record_game_date_et",
]
