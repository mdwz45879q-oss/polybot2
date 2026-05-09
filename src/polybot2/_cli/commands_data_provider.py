"""Data/provider command handlers."""

from __future__ import annotations

import logging
from typing import Any

from polybot2._cli.common import _int_or_none
from polybot2._cli.common import _runtime_from_args
from polybot2.data import MarketSync as _MarketSync
from polybot2.data import MarketSyncConfig
from polybot2.data import open_database
from polybot2.providers import sync_provider_games

# Patchable dependency hooks for tests.
MarketSync = _MarketSync

async def run_market_sync(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)
    batch_size = _int_or_none(getattr(args, "batch_size", None))
    concurrency = _int_or_none(getattr(args, "concurrency", None))
    max_rps = _int_or_none(getattr(args, "max_rps", None))
    open_max_pages = _int_or_none(getattr(args, "open_max_pages", None))
    include_all = bool(getattr(args, "all", False))
    fast_mode = bool(getattr(args, "fast_mode", False))
    cfg_kwargs: dict[str, Any] = {"gamma_api": runtime.gamma_api}
    if batch_size is not None:
        cfg_kwargs["batch_size"] = int(batch_size)
    if concurrency is not None:
        cfg_kwargs["concurrency"] = int(concurrency)
    if max_rps is not None:
        cfg_kwargs["max_rps"] = int(max_rps)
    if open_max_pages is not None:
        cfg_kwargs["open_max_pages"] = int(open_max_pages)
    cfg_kwargs["open_only"] = not include_all
    cfg_kwargs["fast_mode"] = bool(fast_mode)
    with open_database(runtime) as db:
        sync = MarketSync(db=db, config=MarketSyncConfig(**cfg_kwargs))
        count = await sync.run()
        stats = sync.last_run_stats or {}
    stage = stats.get("stage_timing_s") if isinstance(stats.get("stage_timing_s"), dict) else {}
    logger.info(
        "market sync complete: markets=%d elapsed_s=%.3f pages=%s rows=%s retries=%s failures=%s stage(fetch=%.3f,db=%.3f) cfg(concurrency=%s,max_rps=%s,batch=%s,fast_mode=%s)",
        int(count),
        float(stats.get("elapsed_s") or 0.0),
        stats.get("total_pages_processed"),
        stats.get("total_rows_processed"),
        stats.get("retries_total"),
        stats.get("hard_failures"),
        float(stage.get("fetch") or 0.0),
        float(stage.get("db_upsert") or 0.0),
        (stats.get("config") or {}).get("concurrency"),
        (stats.get("config") or {}).get("max_rps"),
        (stats.get("config") or {}).get("batch_size"),
        (stats.get("config") or {}).get("fast_mode"),
    )
    return 0


def run_provider_sync(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)
    explicit = str(getattr(args, "provider", "")).strip().lower()

    if explicit:
        providers = [explicit]
    else:
        from polybot2.linking import load_mapping
        mapping = load_mapping()
        providers = sorted({
            str(cfg.get("provider", "")).strip().lower()
            for cfg in mapping.leagues.values()
            if str(cfg.get("provider", "")).strip()
        })
        if not providers:
            logger.error("no providers configured in LEAGUES")
            return 1
        logger.info("syncing all configured providers: %s", ", ".join(providers))

    failed = False
    with open_database(runtime) as db:
        for provider in providers:
            res = sync_provider_games(db=db, provider=provider)
            if res.status != "ok":
                logger.error("provider sync failed: provider=%s status=%s reason=%s", res.provider, res.status, res.reason)
                failed = True
            else:
                logger.info("provider sync complete: provider=%s rows=%d", res.provider, int(res.n_rows))
    return 1 if failed else 0


__all__ = [
    "run_market_sync",
    "run_provider_sync",
    "MarketSync",
]
