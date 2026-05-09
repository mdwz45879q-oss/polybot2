"""Incremental Polymarket market metadata ingestion from Gamma /events."""

from __future__ import annotations

import asyncio
import copy
import logging
import sys
import time
from typing import Any, Callable

import httpx
from tqdm import tqdm

from polybot2.data._http import request_json_with_retry
from polybot2.data._rate_limit import SlidingWindowRateLimiter
from polybot2.data.storage.database import Database
from polybot2.data.sync_config import MarketSyncConfig

log = logging.getLogger(__name__)


class MarketSync:
    def __init__(
        self,
        db: Database,
        *,
        config: MarketSyncConfig | None = None,
    ):
        cfg = config or MarketSyncConfig()
        self._db = db
        self._gamma_api = str(cfg.gamma_api)
        self._batch_size = int(cfg.batch_size)
        self._timeout = float(cfg.timeout)
        self._request_delay = float(cfg.request_delay)
        self._concurrency = max(1, int(cfg.concurrency))
        self._max_rps = max(0, int(cfg.max_rps))
        self._fetch_max_retries = max(1, int(cfg.fetch_max_retries))
        self._resolved_max_pages = None if cfg.resolved_max_pages is None else max(1, int(cfg.resolved_max_pages))
        self._open_max_pages = None if cfg.open_max_pages is None else max(1, int(cfg.open_max_pages))
        self._open_only = bool(getattr(cfg, "open_only", False))
        self._fast_mode = bool(cfg.fast_mode)
        self._http2 = bool(getattr(cfg, "http2", True))
        self._rate_limiter = (
            SlidingWindowRateLimiter(self._max_rps, window_seconds=1.0)
            if self._max_rps > 0
            else None
        )
        self._http_metrics: dict[str, int] = {}
        self._last_run_stats: dict[str, Any] | None = None

    @property
    def last_run_stats(self) -> dict[str, Any] | None:
        if self._last_run_stats is None:
            return None
        return copy.deepcopy(self._last_run_stats)

    @staticmethod
    def _to_optional_int(value: Any) -> int | None:
        try:
            if value is None or str(value).strip() == "":
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _get_resolved_offset(self) -> int:
        row = self._db.execute("SELECT value FROM sync_state WHERE key = 'resolved_offset'").fetchone()
        return int(row["value"]) if row else 0

    def _save_resolved_offset(self, offset: int) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('resolved_offset', ?)",
            (int(offset),),
        )
        self._db.commit()

    async def _wait_for_request_slot(self) -> None:
        if self._rate_limiter is not None:
            await self._rate_limiter.acquire()
        if self._request_delay > 0:
            await asyncio.sleep(self._request_delay)

    async def _fetch_page_with_retry(self, client: httpx.AsyncClient, params: dict[str, Any]) -> list[dict[str, Any]]:
        payload = await request_json_with_retry(
            client=client,
            method="GET",
            url=f"{self._gamma_api}/events",
            params=params,
            max_retries=self._fetch_max_retries,
            before_request=self._wait_for_request_slot,
            logger=log,
            log_context=f"offset={params.get('offset', 0)}",
            metrics=self._http_metrics,
        )
        if isinstance(payload, list):
            return payload
        return []

    async def _fetch_page_closed(self, client: httpx.AsyncClient, page_offset: int) -> list[dict[str, Any]]:
        return await self._fetch_page_with_retry(
            client,
            {
                "closed": "true",
                "order": "updatedAt",
                "ascending": "true",
                "limit": self._batch_size,
                "offset": page_offset,
            },
        )

    async def _fetch_page_open(self, client: httpx.AsyncClient, page_offset: int) -> list[dict[str, Any]]:
        return await self._fetch_page_with_retry(
            client,
            {
                "active": "true",
                "closed": "false",
                "limit": self._batch_size,
                "offset": page_offset,
            },
        )

    async def _run_pass(
        self,
        client: httpx.AsyncClient,
        fetch_fn: Callable[[httpx.AsyncClient, int], Any],
        start_offset: int,
        save_offset_fn: Callable[[int], None] | None,
        label: str,
        now_ts: int,
        max_pages: int | None = None,
        stage_metrics: dict[str, float] | None = None,
    ) -> tuple[int, int, int]:
        offset = int(start_offset)
        total_markets = 0
        total_pages_processed = 0
        total_rows_advanced = 0
        end_reached = False
        limit_pages = None if max_pages is None else max(1, int(max_pages))
        progress = tqdm(
            desc=f"Markets [{label}]",
            unit="page",
            dynamic_ncols=True,
            leave=True,
            disable=not sys.stderr.isatty(),
        )
        try:
            while not end_reached:
                if limit_pages is not None and total_pages_processed >= limit_pages:
                    break
                tasks = [fetch_fn(client, offset + i * self._batch_size) for i in range(self._concurrency)]
                fetch_started = time.perf_counter()
                results = await asyncio.gather(*tasks)
                if stage_metrics is not None:
                    stage_metrics["fetch_s"] = float(stage_metrics.get("fetch_s", 0.0)) + float(
                        time.perf_counter() - fetch_started
                    )
                pages_processed = 0
                rows_advanced = 0
                ingest_events: list[dict[str, Any]] = []
                for data in results:
                    if limit_pages is not None and total_pages_processed + pages_processed >= limit_pages:
                        end_reached = True
                        break
                    if not data:
                        end_reached = True
                        break
                    pages_processed += 1
                    rows_advanced += len(data)
                    ingest_events.extend(data)
                    if len(data) < self._batch_size:
                        end_reached = True
                        break
                if ingest_events:
                    db_started = time.perf_counter()
                    _, market_n, _, _ = self._db.markets.upsert_from_gamma_events(
                        events_data=ingest_events,
                        updated_ts=now_ts,
                    )
                    if stage_metrics is not None:
                        stage_metrics["db_upsert_s"] = float(stage_metrics.get("db_upsert_s", 0.0)) + float(
                            time.perf_counter() - db_started
                        )
                    total_markets += int(market_n)
                offset += rows_advanced
                total_pages_processed += pages_processed
                total_rows_advanced += rows_advanced
                if save_offset_fn is not None:
                    save_offset_fn(offset)
                if pages_processed > 0:
                    progress.update(pages_processed)
                    progress.set_postfix(markets=total_markets, offset=offset, refresh=False)
        finally:
            progress.close()
        return (int(total_markets), int(total_pages_processed), int(total_rows_advanced))

    async def run(self) -> int:
        started = time.perf_counter()
        stage_metrics: dict[str, float] = {
            "fetch_s": 0.0,
            "db_upsert_s": 0.0,
        }
        now_ts = int(time.time())
        self._http_metrics = {}
        self._last_run_stats = None
        mode = "open-only" if self._open_only else "resolved+open"
        log.info("Polymarket Market sync starting (%s; concurrency=%d, max_rps=%d)", mode, self._concurrency, self._max_rps)
        total = 0
        resolved_pages = 0
        resolved_rows = 0
        resolved_markets = 0
        open_pages = 0
        open_rows = 0
        open_markets = 0
        try:
            async with httpx.AsyncClient(
                timeout=self._timeout,
                follow_redirects=True,
                http2=self._http2,
                headers={"Accept-Encoding": "gzip, deflate"},
                limits=httpx.Limits(
                    max_keepalive_connections=self._concurrency * 2,
                    max_connections=self._concurrency * 2,
                ),
            ) as client:
                if not self._open_only:
                    resolved_offset = self._get_resolved_offset()
                    log.info("Resolved pass offset=%d", resolved_offset)
                    (
                        resolved_markets,
                        resolved_pages,
                        resolved_rows,
                    ) = await self._run_pass(
                        client,
                        fetch_fn=self._fetch_page_closed,
                        start_offset=resolved_offset,
                        save_offset_fn=self._save_resolved_offset,
                        label="resolved",
                        now_ts=now_ts,
                        max_pages=self._resolved_max_pages,
                        stage_metrics=stage_metrics,
                    )
                    total += resolved_markets
                else:
                    log.info("Resolved pass skipped (--open-only)")

                log.info("Open pass offset=0")
                (
                    open_markets,
                    open_pages,
                    open_rows,
                ) = await self._run_pass(
                    client,
                    fetch_fn=self._fetch_page_open,
                    start_offset=0,
                    save_offset_fn=None,
                    label="open",
                    now_ts=now_ts,
                    max_pages=self._open_max_pages,
                    stage_metrics=stage_metrics,
                )
                total += open_markets
        finally:
            elapsed_s = float(time.perf_counter() - started)
            self._last_run_stats = {
                "elapsed_s": elapsed_s,
                "requests_attempted": int(self._http_metrics.get("requests_attempted", 0)),
                "retries_total": int(self._http_metrics.get("retries_total", 0)),
                "retry_http_429": int(self._http_metrics.get("retry_http_429", 0)),
                "retry_http_5xx": int(self._http_metrics.get("retry_http_5xx", 0)),
                "retry_http_error": int(self._http_metrics.get("retry_http_error", 0)),
                "hard_failures": int(self._http_metrics.get("hard_failures", 0)),
                "resolved_pages_processed": int(resolved_pages),
                "resolved_rows_processed": int(resolved_rows),
                "resolved_markets_processed": int(resolved_markets),
                "open_pages_processed": int(open_pages),
                "open_rows_processed": int(open_rows),
                "open_markets_processed": int(open_markets),
                "total_pages_processed": int(resolved_pages + open_pages),
                "total_rows_processed": int(resolved_rows + open_rows),
                "total_markets_processed": int(total),
                "stage_timing_s": {
                    "fetch": float(stage_metrics.get("fetch_s", 0.0)),
                    "db_upsert": float(stage_metrics.get("db_upsert_s", 0.0)),
                },
                "config": {
                    "batch_size": int(self._batch_size),
                    "concurrency": int(self._concurrency),
                    "max_rps": int(self._max_rps),
                    "resolved_max_pages": self._resolved_max_pages,
                    "open_max_pages": self._open_max_pages,
                    "open_only": bool(self._open_only),
                    "fast_mode": bool(self._fast_mode),
                },
            }
        log.info("Polymarket Market sync complete: %d markets processed", total)
        return int(total)
