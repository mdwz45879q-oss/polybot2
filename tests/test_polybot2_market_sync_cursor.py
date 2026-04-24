from __future__ import annotations

from typing import Any

from polybot2.data.markets import MarketSync
from polybot2.data.sync_config import MarketSyncConfig


class _DummyMarketsAdapter:
    def __init__(self) -> None:
        self.upsert_batch_sizes: list[int] = []

    def upsert_from_gamma_events(
        self,
        *,
        events_data: list[dict[str, Any]],
        updated_ts: int,
        payload_writer: Any,
        compute_lineage_hash: bool = True,
        commit: bool = True,
    ) -> tuple[int, int, int, int]:
        del updated_ts, payload_writer, compute_lineage_hash, commit
        n = len(events_data)
        self.upsert_batch_sizes.append(n)
        return (0, n, 0, 0)


class _DummyDB:
    def __init__(self) -> None:
        self.markets = _DummyMarketsAdapter()


def _build_sync(*, batch_size: int, concurrency: int) -> MarketSync:
    return MarketSync(
        db=_DummyDB(),
        config=MarketSyncConfig(
            batch_size=int(batch_size),
            concurrency=int(concurrency),
            max_rps=0,
        ),
    )


async def test_resolved_offset_advances_by_rows_not_page_slots() -> None:
    sync = _build_sync(batch_size=500, concurrency=1)
    saved_offsets: list[int] = []

    async def fetch_page(_client: Any, page_offset: int) -> list[dict[str, Any]]:
        if page_offset == 0:
            return [{}] * 500
        if page_offset == 500:
            return [{}] * 200
        return []

    total, pages, rows = await sync._run_pass(
        None,
        fetch_fn=fetch_page,
        start_offset=0,
        save_offset_fn=saved_offsets.append,
        label="resolved",
        now_ts=0,
    )

    assert total == 700
    assert pages == 2
    assert rows == 700
    assert saved_offsets == [500, 700]


async def test_concurrent_batch_partial_page_does_not_overshoot() -> None:
    sync = _build_sync(batch_size=500, concurrency=3)
    saved_offsets: list[int] = []
    requested_offsets: list[int] = []

    async def fetch_page(_client: Any, page_offset: int) -> list[dict[str, Any]]:
        requested_offsets.append(page_offset)
        if page_offset == 0:
            return [{}] * 500
        if page_offset == 500:
            return [{}] * 200
        if page_offset == 1000:
            return []
        return []

    total, pages, rows = await sync._run_pass(
        None,
        fetch_fn=fetch_page,
        start_offset=0,
        save_offset_fn=saved_offsets.append,
        label="resolved",
        now_ts=0,
    )

    assert requested_offsets[:3] == [0, 500, 1000]
    assert total == 700
    assert pages == 2
    assert rows == 700
    assert saved_offsets == [700]
    assert sync._db.markets.upsert_batch_sizes == [700]


async def test_total_markets_count_unchanged() -> None:
    sync = _build_sync(batch_size=500, concurrency=1)
    saved_offsets: list[int] = []

    async def fetch_page(_client: Any, page_offset: int) -> list[dict[str, Any]]:
        if page_offset == 0:
            return [{}] * 500
        if page_offset == 500:
            return [{}] * 200
        return []

    total, pages, rows = await sync._run_pass(
        None,
        fetch_fn=fetch_page,
        start_offset=0,
        save_offset_fn=saved_offsets.append,
        label="resolved",
        now_ts=0,
    )

    assert total == 700
    assert pages == 2
    assert rows == 700
    assert sync._db.markets.upsert_batch_sizes == [500, 200]
    assert saved_offsets[-1] == 700


async def test_run_pass_respects_max_pages_cap() -> None:
    sync = _build_sync(batch_size=500, concurrency=3)
    saved_offsets: list[int] = []
    requested_offsets: list[int] = []

    async def fetch_page(_client: Any, page_offset: int) -> list[dict[str, Any]]:
        requested_offsets.append(page_offset)
        return [{}] * 500

    total, pages, rows = await sync._run_pass(
        None,
        fetch_fn=fetch_page,
        start_offset=0,
        save_offset_fn=saved_offsets.append,
        label="resolved",
        now_ts=0,
        max_pages=1,
    )

    assert requested_offsets[:3] == [0, 500, 1000]
    assert total == 500
    assert pages == 1
    assert rows == 500
    assert saved_offsets == [500]
    assert sync._db.markets.upsert_batch_sizes == [500]


async def test_run_pass_records_fetch_and_db_stage_timing() -> None:
    sync = _build_sync(batch_size=500, concurrency=1)
    stage: dict[str, float] = {}

    async def fetch_page(_client: Any, page_offset: int) -> list[dict[str, Any]]:
        if page_offset == 0:
            return [{}] * 100
        return []

    total, pages, rows = await sync._run_pass(
        None,
        fetch_fn=fetch_page,
        start_offset=0,
        save_offset_fn=None,
        label="resolved",
        now_ts=0,
        stage_metrics=stage,
    )

    assert total == 100
    assert pages == 1
    assert rows == 100
    assert float(stage.get("fetch_s") or 0.0) >= 0.0
    assert float(stage.get("db_upsert_s") or 0.0) >= 0.0
