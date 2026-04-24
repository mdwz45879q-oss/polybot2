"""Shared actionable-game predicates for review/launch/compile flows."""

from __future__ import annotations

import time
from typing import Any


_CLOSED_EVENT_STATUSES = (
    "closed",
    "resolved",
    "ended",
    "finished",
    "final",
    "complete",
    "completed",
    "cancelled",
    "canceled",
)


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def provider_snapshot_info(*, db: Any, provider: str) -> tuple[set[str], int | None]:
    p = _norm(provider)
    rows = db.execute(
        """
        SELECT provider_game_id, updated_at
        FROM provider_games
        WHERE provider = ?
        ORDER BY provider_game_id
        """,
        (p,),
    ).fetchall()
    game_ids: set[str] = set()
    max_updated_at: int | None = None
    for row in rows:
        gid = str(row["provider_game_id"] or "").strip()
        if gid:
            game_ids.add(gid)
        updated = row["updated_at"]
        if updated is None:
            continue
        updated_i = int(updated)
        if max_updated_at is None or updated_i > max_updated_at:
            max_updated_at = updated_i
    return (game_ids, max_updated_at)


def unresolved_open_target_game_ids(
    *,
    db: Any,
    provider: str,
    run_id: int,
    league: str | None = None,
) -> set[str]:
    p = _norm(provider)
    rid = int(run_id)
    lk = _norm(str(league or ""))
    status_placeholders = ",".join("?" for _ in _CLOSED_EVENT_STATUSES)
    rows = db.execute(
        f"""
        SELECT DISTINCT t.provider_game_id
        FROM link_run_market_targets t
        LEFT JOIN link_run_provider_games pg
          ON pg.run_id = t.run_id
         AND pg.provider = t.provider
         AND pg.provider_game_id = t.provider_game_id
        LEFT JOIN link_run_game_reviews gr
          ON gr.run_id = t.run_id
         AND gr.provider = t.provider
         AND gr.provider_game_id = t.provider_game_id
        LEFT JOIN pm_events pe
          ON pe.event_id = gr.selected_event_id
        LEFT JOIN pm_markets m
          ON m.condition_id = t.condition_id
        WHERE t.run_id = ?
          AND t.provider = ?
          AND COALESCE(pg.parse_status, '') = 'ok'
          AND (? = '' OR COALESCE(pg.canonical_league, '') = ?)
          AND t.is_tradeable = 1
          AND m.condition_id IS NOT NULL
          AND COALESCE(m.resolved, 0) = 0
          AND COALESCE(TRIM(gr.selected_event_id), '') <> ''
          AND pe.event_id IS NOT NULL
          AND LOWER(TRIM(COALESCE(pe.status, ''))) NOT IN ({status_placeholders})
        """,
        (rid, p, lk, lk, *_CLOSED_EVENT_STATUSES),
    ).fetchall()
    return {str(r["provider_game_id"] or "").strip() for r in rows if str(r["provider_game_id"] or "").strip()}


def run_scope_game_ids(
    *,
    db: Any,
    provider: str,
    run_id: int,
    league: str | None = None,
) -> set[str]:
    p = _norm(provider)
    rid = int(run_id)
    lk = _norm(str(league or ""))
    rows = db.execute(
        """
        SELECT provider_game_id
        FROM link_run_provider_games
        WHERE run_id = ?
          AND provider = ?
          AND parse_status = 'ok'
          AND (? = '' OR COALESCE(canonical_league, '') = ?)
        """,
        (rid, p, lk, lk),
    ).fetchall()
    return {str(r["provider_game_id"] or "").strip() for r in rows if str(r["provider_game_id"] or "").strip()}


def closed_selected_event_game_ids(
    *,
    db: Any,
    provider: str,
    run_id: int,
    league: str | None = None,
) -> set[str]:
    p = _norm(provider)
    rid = int(run_id)
    lk = _norm(str(league or ""))
    status_placeholders = ",".join("?" for _ in _CLOSED_EVENT_STATUSES)
    rows = db.execute(
        f"""
        SELECT DISTINCT gr.provider_game_id
        FROM link_run_game_reviews gr
        LEFT JOIN link_run_provider_games pg
          ON pg.run_id = gr.run_id
         AND pg.provider = gr.provider
         AND pg.provider_game_id = gr.provider_game_id
        LEFT JOIN pm_events pe
          ON pe.event_id = gr.selected_event_id
        WHERE gr.run_id = ?
          AND gr.provider = ?
          AND (? = '' OR COALESCE(pg.canonical_league, '') = ?)
          AND COALESCE(TRIM(gr.selected_event_id), '') <> ''
          AND LOWER(TRIM(COALESCE(pe.status, ''))) IN ({status_placeholders})
        """,
        (rid, p, lk, lk, *_CLOSED_EVENT_STATUSES),
    ).fetchall()
    return {str(r["provider_game_id"] or "").strip() for r in rows if str(r["provider_game_id"] or "").strip()}


def actionable_game_ids(
    *,
    db: Any,
    provider: str,
    run_id: int,
    max_age_seconds: int,
    now_ts_utc: int | None = None,
    league: str | None = None,
    require_open_targets: bool = True,
) -> set[str]:
    max_age = max(1, int(max_age_seconds))
    now_ts = int(time.time()) if now_ts_utc is None else int(now_ts_utc)
    snapshot_ids, snapshot_updated_at = provider_snapshot_info(db=db, provider=provider)
    if not snapshot_ids or snapshot_updated_at is None:
        return set()
    if int(now_ts - int(snapshot_updated_at)) > max_age:
        return set()
    run_ids = run_scope_game_ids(db=db, provider=provider, run_id=run_id, league=league)
    if not run_ids:
        return set()
    candidate = snapshot_ids.intersection(run_ids)
    if not candidate:
        return set()
    candidate.difference_update(closed_selected_event_game_ids(db=db, provider=provider, run_id=run_id, league=league))
    if not candidate:
        return set()
    if bool(require_open_targets):
        target_ids = unresolved_open_target_game_ids(db=db, provider=provider, run_id=run_id, league=league)
        if not target_ids:
            return set()
        candidate.intersection_update(target_ids)
    return candidate


__all__ = [
    "actionable_game_ids",
    "closed_selected_event_game_ids",
    "provider_snapshot_info",
    "run_scope_game_ids",
    "unresolved_open_target_game_ids",
]
