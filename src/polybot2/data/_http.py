"""Shared async HTTP JSON retry helper for data sync clients."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import httpx


async def request_json_with_retry(
    *,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    max_retries: int = 3,
    before_request: Callable[[], Awaitable[None]] | None = None,
    logger: logging.Logger | None = None,
    log_context: str = "",
    retry_http_5xx: bool = True,
    metrics: dict[str, int] | None = None,
) -> Any | None:
    """Perform an HTTP request with bounded retry/backoff and JSON decode."""
    attempts = max(1, int(max_retries))
    method_upper = str(method).upper()
    for attempt in range(1, attempts + 1):
        if metrics is not None:
            metrics["requests_attempted"] = int(metrics.get("requests_attempted", 0)) + 1
        if before_request is not None:
            await before_request()
        try:
            if method_upper == "GET":
                response = await client.get(url, params=params)
            elif method_upper == "POST":
                response = await client.post(url, params=params, json=json_body)
            else:
                response = await client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                )
        except httpx.HTTPError as exc:
            if attempt >= attempts:
                if logger is not None:
                    logger.debug("HTTP %s failed (%s): %s", method, log_context, exc)
                if metrics is not None:
                    metrics["hard_failures"] = int(metrics.get("hard_failures", 0)) + 1
                return None
            if metrics is not None:
                metrics["retries_total"] = int(metrics.get("retries_total", 0)) + 1
                metrics["retry_http_error"] = int(metrics.get("retry_http_error", 0)) + 1
            await asyncio.sleep(min(2 * attempt, 5))
            continue

        try:
            status_code = int(getattr(response, "status_code"))
        except (TypeError, ValueError):
            return None

        if status_code == 429:
            if attempt >= attempts:
                if metrics is not None:
                    metrics["hard_failures"] = int(metrics.get("hard_failures", 0)) + 1
                return None
            if metrics is not None:
                metrics["retries_total"] = int(metrics.get("retries_total", 0)) + 1
                metrics["retry_http_429"] = int(metrics.get("retry_http_429", 0)) + 1
            await asyncio.sleep(min(2 * attempt, 10))
            continue

        if retry_http_5xx and 500 <= status_code < 600:
            if attempt >= attempts:
                if metrics is not None:
                    metrics["hard_failures"] = int(metrics.get("hard_failures", 0)) + 1
                return None
            if metrics is not None:
                metrics["retries_total"] = int(metrics.get("retries_total", 0)) + 1
                metrics["retry_http_5xx"] = int(metrics.get("retry_http_5xx", 0)) + 1
            await asyncio.sleep(min(2 * attempt, 5))
            continue

        if status_code != 200:
            if metrics is not None:
                metrics["hard_failures"] = int(metrics.get("hard_failures", 0)) + 1
                metrics["hard_failures_http_non200"] = int(metrics.get("hard_failures_http_non200", 0)) + 1
            return None

        try:
            payload = response.json()
            if asyncio.iscoroutine(payload):
                payload = await payload
            return payload
        except (ValueError, KeyError):
            if metrics is not None:
                metrics["hard_failures"] = int(metrics.get("hard_failures", 0)) + 1
                metrics["hard_failures_json"] = int(metrics.get("hard_failures_json", 0)) + 1
            return None
    return None
