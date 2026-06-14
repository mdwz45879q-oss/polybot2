"""Incremental market discovery for running hotpath sessions.

Fetches new markets under already-linked Polymarket events and diffs
against the current compiled plan.  No provider sync, no link build,
no new run_id — only targeted Gamma API calls for known event IDs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from polybot2.data._http import request_json_with_retry
from polybot2.hotpath.compiler import compile_hotpath_plan
from polybot2.hotpath.contracts import CompiledPlan, CompiledTarget
from polybot2.linking.mapping_loader import (
    LoadedLiveTradingPolicy,
    load_live_trading_policy,
)
from polybot2.market_types import normalize_sports_market_type

log = logging.getLogger(__name__)

_GAMMA_API_DEFAULT = "https://gamma-api.polymarket.com"


@dataclass(frozen=True, slots=True)
class IncrementalRefreshResult:
    new_plan: CompiledPlan | None
    new_targets: tuple[CompiledTarget, ...]
    new_condition_ids: frozenset[str]
    events_fetched: int
    markets_discovered: int
    targets_inserted: int


def _extract_condition_ids(plan: CompiledPlan) -> set[str]:
    cids: set[str] = set()
    for game in plan.games:
        for market in game.markets:
            cid = str(market.condition_id or "").strip()
            if cid:
                cids.add(cid)
    return cids


def _extract_strategy_keys(plan: CompiledPlan) -> set[str]:
    keys: set[str] = set()
    for game in plan.games:
        for market in game.markets:
            for target in market.targets:
                sk = str(target.strategy_key or "").strip()
                if sk:
                    keys.add(sk)
    return keys


def _extract_new_targets(
    old_plan: CompiledPlan, new_plan: CompiledPlan,
) -> tuple[CompiledTarget, ...]:
    old_keys = _extract_strategy_keys(old_plan)
    new_targets: list[CompiledTarget] = []
    for game in new_plan.games:
        for market in game.markets:
            for target in market.targets:
                if target.strategy_key not in old_keys:
                    new_targets.append(target)
    return tuple(new_targets)


def _gather_event_ids(
    db: Any, provider: str, game_ids: list[str],
) -> set[str]:
    if not game_ids:
        return set()
    placeholders = ",".join("?" for _ in game_ids)
    rows = db.execute(
        f"""
        SELECT DISTINCT event_id
        FROM link_event_bindings
        WHERE provider = ?
          AND provider_game_id IN ({placeholders})
          AND COALESCE(TRIM(event_id), '') <> ''
        """,
        (provider, *game_ids),
    ).fetchall()
    return {str(r["event_id"]).strip() for r in rows if str(r["event_id"] or "").strip()}


async def _fetch_events_by_ids(
    *,
    gamma_api: str,
    event_ids: set[str],
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    all_events: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=timeout) as client:
        for event_id in sorted(event_ids):
            payload = await request_json_with_retry(
                client=client,
                method="GET",
                url=f"{gamma_api}/events",
                params={"id": event_id},
                max_retries=3,
                logger=log,
                log_context=f"incremental_event_id={event_id}",
            )
            if isinstance(payload, list):
                all_events.extend(e for e in payload if isinstance(e, dict))
            elif isinstance(payload, dict):
                all_events.append(payload)
    return all_events


def _find_new_condition_ids(
    db: Any, event_ids: set[str], plan_condition_ids: set[str],
) -> set[str]:
    """Find condition_ids under known events that are not in the current plan.

    Diffs against the plan (not the DB link_run_market_targets table) so that
    condition_ids from a previously failed patch are retried on the next cycle.
    """
    if not event_ids:
        return set()
    ev_ph = ",".join("?" for _ in event_ids)
    all_cids_rows = db.execute(
        f"""
        SELECT DISTINCT condition_id
        FROM pm_markets
        WHERE event_id IN ({ev_ph})
          AND COALESCE(resolved, 0) = 0
          AND COALESCE(TRIM(condition_id), '') <> ''
        """,
        tuple(sorted(event_ids)),
    ).fetchall()
    all_cids = {str(r["condition_id"]).strip() for r in all_cids_rows}
    if not all_cids:
        return set()
    return all_cids - plan_condition_ids


def _build_event_to_game_map(
    db: Any, provider: str, game_ids: list[str],
) -> dict[str, str]:
    if not game_ids:
        return {}
    placeholders = ",".join("?" for _ in game_ids)
    rows = db.execute(
        f"""
        SELECT provider_game_id, event_id
        FROM link_event_bindings
        WHERE provider = ?
          AND provider_game_id IN ({placeholders})
          AND COALESCE(TRIM(event_id), '') <> ''
        """,
        (provider, *game_ids),
    ).fetchall()
    out: dict[str, str] = {}
    for r in rows:
        eid = str(r["event_id"] or "").strip()
        gid = str(r["provider_game_id"] or "").strip()
        if eid and gid:
            out[eid] = gid
    return out


def _insert_new_market_targets(
    *,
    db: Any,
    run_id: int,
    provider: str,
    new_condition_ids: set[str],
    event_to_game: dict[str, str],
    allowed_market_types: set[str],
) -> int:
    if not new_condition_ids:
        return 0
    cid_ph = ",".join("?" for _ in new_condition_ids)
    markets = db.execute(
        f"""
        SELECT condition_id, event_id, slug, sports_market_type, resolved
        FROM pm_markets
        WHERE condition_id IN ({cid_ph})
        """,
        tuple(sorted(new_condition_ids)),
    ).fetchall()
    tokens = db.markets.load_tokens_for_condition_ids(sorted(new_condition_ids))
    token_lookup: dict[tuple[str, int], dict[str, Any]] = {}
    for t in tokens:
        key = (str(t["condition_id"]), int(t.get("outcome_index") or 0))
        token_lookup[key] = t

    now_ts = int(time.time())
    rows: list[tuple[Any, ...]] = []
    for m in markets:
        cid = str(m["condition_id"] or "").strip()
        event_id = str(m["event_id"] or "").strip()
        if int(m["resolved"] or 0) != 0:
            continue
        market_type = normalize_sports_market_type(m["sports_market_type"])
        if market_type not in allowed_market_types:
            continue
        gid = event_to_game.get(event_id, "")
        if not gid:
            continue
        market_slug = str(m["slug"] or "").strip()
        for outcome_index in (0, 1):
            tok = token_lookup.get((cid, outcome_index))
            token_id = str(tok["token_id"]) if tok else ""
            is_tradeable = 1 if bool(token_id) else 0
            binding_status = "exact" if is_tradeable else "unresolved"
            reason_code = "ok" if is_tradeable else "missing_token"
            rows.append((
                int(run_id), provider, gid, cid, outcome_index,
                token_id, market_slug, market_type,
                binding_status, reason_code, is_tradeable, now_ts,
            ))
    if rows:
        db.linking.upsert_run_market_targets(rows, commit=True)
    return len(rows)


async def discover_new_markets(
    *,
    current_plan: CompiledPlan,
    db: Any,
    gamma_api: str = _GAMMA_API_DEFAULT,
    live_policy: LoadedLiveTradingPolicy | None = None,
    exclude_strategy_keys: set[str] | None = None,
    plan_horizon_hours: int | None = None,
    now_ts_utc: int | None = None,
) -> IncrementalRefreshResult:
    empty = IncrementalRefreshResult(
        new_plan=None, new_targets=(), new_condition_ids=frozenset(),
        events_fetched=0, markets_discovered=0, targets_inserted=0,
    )
    if not current_plan or not current_plan.games:
        return empty

    policy = live_policy or load_live_trading_policy()
    provider = current_plan.provider
    league = current_plan.league
    run_id = current_plan.run_id
    game_ids = [
        str(g.provider_game_id) for g in current_plan.games
        if str(g.provider_game_id or "").strip()
    ]
    if not game_ids:
        return empty

    event_ids = _gather_event_ids(db, provider, game_ids)
    if not event_ids:
        return empty

    events_data = await _fetch_events_by_ids(
        gamma_api=gamma_api, event_ids=event_ids,
    )
    events_fetched = len(events_data)
    if not events_data:
        return IncrementalRefreshResult(
            new_plan=None, new_targets=(), new_condition_ids=frozenset(),
            events_fetched=events_fetched, markets_discovered=0, targets_inserted=0,
        )

    db.markets.upsert_from_gamma_events(
        events_data=events_data,
        updated_ts=int(time.time()),
        commit=True,
    )

    plan_cids = _extract_condition_ids(current_plan)
    new_cids = _find_new_condition_ids(db, event_ids, plan_cids)
    if not new_cids:
        return IncrementalRefreshResult(
            new_plan=None, new_targets=(), new_condition_ids=frozenset(),
            events_fetched=events_fetched, markets_discovered=0, targets_inserted=0,
        )

    league_market_types = {
        normalize_sports_market_type(x)
        for x in (policy.live_betting_market_types_by_league or {}).get(league, [])
        if normalize_sports_market_type(x) != "other"
    }
    if not league_market_types:
        return IncrementalRefreshResult(
            new_plan=None, new_targets=(), new_condition_ids=frozenset(new_cids),
            events_fetched=events_fetched, markets_discovered=len(new_cids), targets_inserted=0,
        )

    event_to_game = _build_event_to_game_map(db, provider, game_ids)
    targets_inserted = _insert_new_market_targets(
        db=db,
        run_id=run_id,
        provider=provider,
        new_condition_ids=new_cids,
        event_to_game=event_to_game,
        allowed_market_types=league_market_types,
    )
    if targets_inserted == 0:
        return IncrementalRefreshResult(
            new_plan=None, new_targets=(), new_condition_ids=frozenset(new_cids),
            events_fetched=events_fetched, markets_discovered=len(new_cids), targets_inserted=0,
        )

    # Resolve sets_to_win from the LEAGUES config (source of truth), not
    # from the first game in the plan — a multi-league plan can mix BO3
    # and BO5 leagues (e.g., women's BO3 + men's BO5 French Open).
    from polybot2.linking.mapping_loader import load_mapping as _load_mapping
    _mapping = _load_mapping()
    _sets_to_win = int(_mapping.leagues.get(league, {}).get("sets_to_win", 2))

    # include_inactive=True: skip the catalog-staleness check. The games
    # are already in the running plan — we're just looking for new markets.
    # Without this, incremental refresh fails with no_in_scope_games if
    # provider sync hasn't run within provider_catalog_max_age_seconds.
    new_plan = compile_hotpath_plan(
        db=db,
        provider=provider,
        league=league,
        run_id=run_id,
        sport=getattr(current_plan, "sport", "") or "",
        sets_to_win=_sets_to_win,
        live_policy=policy,
        now_ts_utc=now_ts_utc if now_ts_utc is not None else int(time.time()),
        plan_horizon_hours=plan_horizon_hours,
        exclude_strategy_keys=exclude_strategy_keys,
        include_inactive=True,
    )
    new_targets = _extract_new_targets(current_plan, new_plan)
    if not new_targets:
        return IncrementalRefreshResult(
            new_plan=None, new_targets=(), new_condition_ids=frozenset(new_cids),
            events_fetched=events_fetched, markets_discovered=len(new_cids),
            targets_inserted=targets_inserted,
        )
    log.info(
        "incremental refresh: events=%d new_markets=%d new_targets=%d",
        events_fetched, len(new_cids), len(new_targets),
    )
    return IncrementalRefreshResult(
        new_plan=new_plan,
        new_targets=tuple(new_targets),
        new_condition_ids=frozenset(new_cids),
        events_fetched=events_fetched,
        markets_discovered=len(new_cids),
        targets_inserted=targets_inserted,
    )


def discover_new_markets_sync(
    *,
    current_plan: CompiledPlan,
    db: Any,
    gamma_api: str = _GAMMA_API_DEFAULT,
    live_policy: LoadedLiveTradingPolicy | None = None,
    exclude_strategy_keys: set[str] | None = None,
    plan_horizon_hours: int | None = None,
    now_ts_utc: int | None = None,
) -> IncrementalRefreshResult:
    # The CLI main() runs inside an asyncio event loop (cli.py:40), and Python
    # 3.12 prevents any run_until_complete (even on a new loop) while another
    # loop is running on the same thread. Run the async work in a separate
    # thread to get a clean event loop.
    import concurrent.futures
    coro = discover_new_markets(
        current_plan=current_plan,
        db=db,
        gamma_api=gamma_api,
        live_policy=live_policy,
        exclude_strategy_keys=exclude_strategy_keys,
        plan_horizon_hours=plan_horizon_hours,
        now_ts_utc=now_ts_utc,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        return future.result()


@dataclass(frozen=True, slots=True)
class IncrementalGameResult:
    new_plan: CompiledPlan | None
    new_targets: tuple[CompiledTarget, ...]
    games_discovered: int
    markets_discovered: int
    provider_games_refreshed: int
    pm_events_refreshed: int


async def _fetch_events_by_tags(
    *,
    gamma_api: str,
    tags: set[str],
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    all_events: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    async with httpx.AsyncClient(timeout=timeout) as client:
        for tag in sorted(tags):
            offset = 0
            while True:
                payload = await request_json_with_retry(
                    client=client,
                    method="GET",
                    url=f"{gamma_api}/events",
                    params={"tag": tag, "active": "true", "closed": "false", "limit": 100, "offset": offset},
                    max_retries=3,
                    logger=log,
                    log_context=f"game_discovery_tag={tag}",
                )
                if not isinstance(payload, list) or not payload:
                    break
                for ev in payload:
                    if isinstance(ev, dict):
                        eid = str(ev.get("id", ""))
                        if eid and eid not in seen_ids:
                            seen_ids.add(eid)
                            all_events.append(ev)
                if len(payload) < 100:
                    break
                offset += 100
    return all_events


async def discover_new_games(
    *,
    current_plan: CompiledPlan,
    db: Any,
    mapping: Any,
    live_policy: LoadedLiveTradingPolicy,
    league_keys: list[str],
    provider_name: str,
    run_id: int,
    plan_horizon_hours: int,
    sport: str,
    sets_to_win_by_league: dict[str, int] | None = None,
    gamma_api: str = _GAMMA_API_DEFAULT,
) -> IncrementalGameResult:
    empty = IncrementalGameResult(
        new_plan=None, new_targets=(), games_discovered=0,
        markets_discovered=0, provider_games_refreshed=0, pm_events_refreshed=0,
    )
    if not current_plan or not league_keys:
        return empty

    existing_game_ids = {
        str(g.provider_game_id) for g in current_plan.games
        if str(g.provider_game_id or "").strip()
    }

    # Step 1: Refresh provider catalog (upsert, not replace).
    from polybot2.providers.sync import load_provider_catalog
    try:
        catalog_rows = load_provider_catalog(provider=provider_name)
        if catalog_rows:
            db.linking.upsert_provider_games(catalog_rows)
        provider_games_refreshed = len(catalog_rows) if catalog_rows else 0
    except Exception as exc:
        log.warning("game discovery: catalog refresh failed: %s: %s", type(exc).__name__, exc)
        provider_games_refreshed = 0

    # Step 2: Refresh PM events by league tags.
    pm_league_codes = {
        str(mapping.leagues.get(lk, {}).get("polymarket_league_code", "")).strip().lower()
        for lk in league_keys
    }
    pm_league_codes.discard("")
    try:
        events_data = await _fetch_events_by_tags(gamma_api=gamma_api, tags=pm_league_codes)
        if events_data:
            db.markets.upsert_from_gamma_events(
                events_data=events_data, updated_ts=int(time.time()), commit=True,
            )
        pm_events_refreshed = len(events_data)
    except Exception as exc:
        log.warning("game discovery: PM event refresh failed: %s: %s", type(exc).__name__, exc)
        pm_events_refreshed = 0

    # Step 3: Incremental link.
    from polybot2.linking.service import LinkService
    link_service = LinkService(db=db)
    league_provider_pairs = [(lk, provider_name) for lk in league_keys]
    try:
        link_result = link_service.build_links_incremental(
            run_id=run_id,
            league_provider_pairs=league_provider_pairs,
            mapping=mapping,
            live_policy=live_policy,
            existing_game_ids=existing_game_ids,
            horizon_hours=plan_horizon_hours,
        )
    except Exception as exc:
        log.warning("game discovery: incremental link failed: %s: %s", type(exc).__name__, exc)
        return IncrementalGameResult(
            new_plan=None, new_targets=(), games_discovered=0,
            markets_discovered=0, provider_games_refreshed=provider_games_refreshed,
            pm_events_refreshed=pm_events_refreshed,
        )

    new_game_ids = link_result.report.get("new_game_ids", []) if isinstance(link_result.report, dict) else []
    if not new_game_ids:
        log.info("game discovery: no new games (catalog=%d, pm_events=%d)", provider_games_refreshed, pm_events_refreshed)
        return IncrementalGameResult(
            new_plan=None, new_targets=(), games_discovered=0,
            markets_discovered=0, provider_games_refreshed=provider_games_refreshed,
            pm_events_refreshed=pm_events_refreshed,
        )

    # Step 4: Recompile full plan and extract delta.
    from polybot2.hotpath.compiler import compile_multi_league_plan
    league_provider_pairs = [(lk, provider_name) for lk in league_keys]
    new_plan = compile_multi_league_plan(
        db=db,
        leagues=league_provider_pairs,
        run_id=run_id,
        sport=sport,
        sets_to_win_by_league=sets_to_win_by_league,
        live_policy=live_policy,
        now_ts_utc=int(time.time()),
        plan_horizon_hours=plan_horizon_hours,
        include_inactive=True,
    )

    old_keys = {
        t.strategy_key
        for g in current_plan.games for m in g.markets for t in m.targets
        if t.strategy_key
    }
    new_targets = [
        t
        for g in new_plan.games for m in g.markets for t in m.targets
        if t.strategy_key and t.strategy_key not in old_keys
    ]

    log.info(
        "game discovery: %d new games, %d new targets (catalog=%d, pm_events=%d)",
        len(new_game_ids), len(new_targets), provider_games_refreshed, pm_events_refreshed,
    )
    return IncrementalGameResult(
        new_plan=new_plan,
        new_targets=tuple(new_targets),
        games_discovered=len(new_game_ids),
        markets_discovered=len(new_targets),
        provider_games_refreshed=provider_games_refreshed,
        pm_events_refreshed=pm_events_refreshed,
    )


def discover_new_games_sync(
    *,
    current_plan: CompiledPlan,
    db: Any,
    mapping: Any,
    live_policy: LoadedLiveTradingPolicy,
    league_keys: list[str],
    provider_name: str,
    run_id: int,
    plan_horizon_hours: int,
    sport: str,
    sets_to_win_by_league: dict[str, int] | None = None,
    gamma_api: str = _GAMMA_API_DEFAULT,
) -> IncrementalGameResult:
    import concurrent.futures
    coro = discover_new_games(
        current_plan=current_plan,
        db=db,
        mapping=mapping,
        live_policy=live_policy,
        league_keys=league_keys,
        provider_name=provider_name,
        run_id=run_id,
        plan_horizon_hours=plan_horizon_hours,
        sport=sport,
        sets_to_win_by_league=sets_to_win_by_league,
        gamma_api=gamma_api,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        return future.result()


__all__ = [
    "IncrementalRefreshResult",
    "IncrementalGameResult",
    "discover_new_markets",
    "discover_new_markets_sync",
    "discover_new_games",
    "discover_new_games_sync",
]
