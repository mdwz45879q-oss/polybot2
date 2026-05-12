"""Kalstrop V2 fixture ID resolver.

V2 prematch event_ids change when games go live. This module discovers the
live event_id by re-fetching tournament fixtures (matching by team names +
start time), then resolves the BetGenius fixture_id via /providers.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

import requests

from polybot2.hotpath.compiler import compile_hotpath_plan
from polybot2.hotpath.contracts import CompiledPlan
from polybot2.sports.kalstrop_auth import kalstrop_auth_headers

logger = logging.getLogger("polybot2")

V2_API = "https://stats.kalstropservice.com/api/v2"
_REQUEST_TIMEOUT = 15


@dataclass(frozen=True, slots=True)
class V2PendingGame:
    prematch_event_id: str
    home_raw: str
    away_raw: str
    when_raw: str
    start_ts_utc: int | None
    league: str
    category_slug: str
    tournament_slug: str


@dataclass(frozen=True, slots=True)
class V2ResolvedGame:
    pending: V2PendingGame
    live_event_id: str
    fixture_id: str
    competition_id: str
    sport_id: str
    resolved_home: str
    resolved_away: str
    resolved_start_time: str
    match_status: str = ""


@dataclass(frozen=True, slots=True)
class V2ResolutionResult:
    resolved: list[V2ResolvedGame]
    finished: list[V2PendingGame]


def build_pending_games(
    db: Any,
    league: str,
    run_id: int,
    provider: str,
    already_resolved: set[str],
) -> list[V2PendingGame]:
    rows = db.execute(
        """
        SELECT pg.provider_game_id, pg.home_raw, pg.away_raw, pg.when_raw, pg.start_ts_utc,
               p.category_name AS category_slug, p.league_raw AS tournament_slug
        FROM link_run_provider_games pg
        LEFT JOIN provider_games p
          ON p.provider = pg.provider AND p.provider_game_id = pg.provider_game_id
        WHERE pg.run_id = ? AND pg.provider = ? AND pg.canonical_league = ?
          AND pg.parse_status = 'ok'
        """,
        (run_id, provider, league),
    ).fetchall()

    pending: list[V2PendingGame] = []
    for r in rows:
        eid = str(r["provider_game_id"] or "").strip()
        if not eid or eid in already_resolved:
            continue
        cat_slug = str(r["category_slug"] or "").strip()
        tourn_slug = str(r["tournament_slug"] or "").strip()
        if not cat_slug or not tourn_slug:
            continue
        pending.append(V2PendingGame(
            prematch_event_id=eid,
            home_raw=str(r["home_raw"] or "").strip(),
            away_raw=str(r["away_raw"] or "").strip(),
            when_raw=str(r["when_raw"] or "").strip(),
            start_ts_utc=r["start_ts_utc"],
            league=league,
            category_slug=cat_slug,
            tournament_slug=tourn_slug,
        ))
    return pending


_FINISHED_STATUSES = frozenset({"ENDED", "FINISHED", "COMPLETED"})


def try_resolve_games(
    pending: list[V2PendingGame],
    client_id: str = "",
    shared_secret_raw: str = "",
    time_tolerance_seconds: int = 900,
) -> V2ResolutionResult:
    if not pending:
        return V2ResolutionResult(resolved=[], finished=[])

    headers = kalstrop_auth_headers(client_id, shared_secret_raw) if client_id and shared_secret_raw else {}

    by_tournament: dict[tuple[str, str], list[V2PendingGame]] = {}
    for game in pending:
        key = (game.category_slug, game.tournament_slug)
        by_tournament.setdefault(key, []).append(game)

    resolved: list[V2ResolvedGame] = []
    finished: list[V2PendingGame] = []
    for (cat_slug, tourn_slug), games in by_tournament.items():
        fixtures = _fetch_tournament_fixtures(cat_slug, tourn_slug, headers)
        if not fixtures:
            continue
        for game in games:
            match, is_finished = _match_fixture(
                fixtures, game.home_raw, game.away_raw,
                game.start_ts_utc, time_tolerance_seconds,
            )
            if is_finished:
                finished.append(game)
                continue
            if match is None:
                continue
            live_event_id = str(match.get("event_id") or "").strip()
            if not live_event_id:
                continue
            match_status = str(match.get("match_status") or "").strip()
            provider_info = _resolve_fixture_id(live_event_id, headers)
            if provider_info is None:
                continue
            fixture_id = str(provider_info.get("fixture_id") or "").strip()
            if not fixture_id:
                continue
            competitors = match.get("competitors", {})
            resolved.append(V2ResolvedGame(
                pending=game,
                live_event_id=live_event_id,
                fixture_id=fixture_id,
                competition_id=str(provider_info.get("competition_id") or ""),
                sport_id=str(provider_info.get("sport_id") or ""),
                resolved_home=str(competitors.get("home", {}).get("name") or ""),
                resolved_away=str(competitors.get("away", {}).get("name") or ""),
                resolved_start_time=str(match.get("start_time") or ""),
                match_status=match_status,
            ))
    return V2ResolutionResult(resolved=resolved, finished=finished)


def compile_for_resolved_game(
    resolved: V2ResolvedGame,
    db: Any,
    run_id: int,
    provider: str,
    league: str,
    live_policy: Any = None,
    plan_horizon_hours: int | None = None,
) -> CompiledPlan | None:
    plan = compile_hotpath_plan(
        db=db,
        provider=provider,
        league=league,
        run_id=run_id,
        live_policy=live_policy,
        now_ts_utc=int(time.time()),
        plan_horizon_hours=plan_horizon_hours,
        include_inactive=True,
    )
    prematch_id = resolved.pending.prematch_event_id
    single_game = None
    for g in plan.games:
        if g.provider_game_id == prematch_id:
            single_game = replace(g, provider_game_id=resolved.fixture_id)
            break
    if single_game is None:
        return None
    return replace(plan, games=(single_game,))


def resolution_time_delta_seconds(resolved: V2ResolvedGame) -> int:
    if not resolved.resolved_start_time or not resolved.pending.start_ts_utc:
        return 0
    try:
        resolved_ts = int(
            datetime.fromisoformat(resolved.resolved_start_time)
            .astimezone(timezone.utc)
            .timestamp()
        )
        return abs(resolved_ts - resolved.pending.start_ts_utc)
    except (ValueError, OSError):
        return 0


def _fetch_tournament_fixtures(
    category_slug: str, tournament_slug: str, headers: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    url = f"{V2_API}/sports/football/competitions/{category_slug}/{tournament_slug}/fixtures"
    try:
        resp = requests.get(url, headers=headers or {}, timeout=_REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.debug("V2 fixtures HTTP %d for %s/%s", resp.status_code, category_slug, tournament_slug)
            return []
        data = resp.json()
        return data.get("fixtures", []) if isinstance(data, dict) else []
    except Exception as exc:
        logger.debug("V2 fixtures error for %s/%s: %s", category_slug, tournament_slug, exc)
        return []


def _resolve_fixture_id(event_id: str, headers: dict[str, str] | None = None) -> dict[str, Any] | None:
    url = f"{V2_API}/fixtures/{event_id}/providers"
    try:
        resp = requests.get(url, params={"sport": "football"}, headers=headers or {}, timeout=_REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.debug("V2 /providers HTTP %d for event_id=%s", resp.status_code, event_id)
            return None
        data = resp.json()
        bg = data.get("providers", {}).get("bet_genius", {})
        fid = bg.get("fixture_id")
        if not fid or str(fid) == str(event_id):
            logger.debug("V2 /providers echoed event_id or missing fixture_id for %s", event_id)
            return None
        return bg
    except Exception as exc:
        logger.debug("V2 /providers error for event_id=%s: %s", event_id, exc)
        return None


def _match_fixture(
    fixtures: list[dict[str, Any]],
    home_raw: str,
    away_raw: str,
    start_ts_utc: int | None,
    tolerance_seconds: int,
) -> tuple[dict[str, Any] | None, bool]:
    """Find a fixture matching teams + time.

    Returns (fixture_dict, False) for live matches,
    (None, True) if the game was found but already finished,
    (None, False) if not found at all.
    """
    home_norm = home_raw.strip().lower()
    away_norm = away_raw.strip().lower()

    for f in fixtures:
        competitors = f.get("competitors", {})
        if not isinstance(competitors, dict):
            continue
        f_home = str(competitors.get("home", {}).get("name") or "").strip().lower()
        f_away = str(competitors.get("away", {}).get("name") or "").strip().lower()
        if f_home != home_norm or f_away != away_norm:
            continue
        if start_ts_utc is not None:
            f_start = str(f.get("start_time") or "").strip()
            if f_start:
                try:
                    f_ts = int(
                        datetime.fromisoformat(f_start)
                        .astimezone(timezone.utc)
                        .timestamp()
                    )
                    if abs(f_ts - start_ts_utc) > tolerance_seconds:
                        continue
                except (ValueError, OSError):
                    pass
        # Teams + time match — check status
        status = str(f.get("match_status") or "").strip().upper()
        if status in _FINISHED_STATUSES:
            return (None, True)
        if status in ("PENDING", ""):
            return (None, False)
        return (f, False)
    return (None, False)
