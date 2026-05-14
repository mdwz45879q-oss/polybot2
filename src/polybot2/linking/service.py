"""Deterministic mapping-driven linking service for polybot2."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
import logging
import time
from typing import Any

from polybot2.data.storage.database import Database
from polybot2.linking.contracts import LinkBuildResult
from polybot2.linking.mapping_loader import (
    LoadedLiveTradingPolicy,
    LoadedMapping,
    MappingValidationError,
    load_live_trading_policy,
)
from polybot2.market_types import normalize_sports_market_type
from polybot2.linking.normalize import normalize_league_key

log = logging.getLogger(__name__)


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _date_add_days(date_text: str, days: int) -> str:
    d = date.fromisoformat(str(date_text))
    return (d + timedelta(days=int(days))).isoformat()


@dataclass(frozen=True)
class _LeagueMatchRules:
    date_tolerance_days: int
    kickoff_tolerance_minutes: int
    provider_order_reliable: bool
    pm_order_reliable: bool


@dataclass(frozen=True)
class _ResolvedProviderGame:
    provider_game_id: str
    canonical_league: str
    polymarket_league_key: str
    canonical_home_team: str
    canonical_away_team: str
    canonical_team_pair: tuple[str, str]
    game_date_et: str
    provider_start_ts_utc: int | None
    slug_hints: tuple[str, ...]


@dataclass(frozen=True)
class _EventChoice:
    event: dict[str, Any]
    events: tuple[dict[str, Any], ...]
    slug_prefix: str
    diagnostics: dict[str, Any]


@dataclass
class _LinkBuildBatch:
    provider: str
    league: str
    n_games_seen: int
    n_games_linked: int
    n_games_tradeable: int
    n_targets: int
    n_targets_tradeable: int
    game_binding_rows: list[tuple[Any, ...]]
    event_binding_rows: list[tuple[Any, ...]]
    market_binding_rows: list[tuple[Any, ...]]
    run_provider_games: list[dict[str, Any]]
    run_game_reviews: list[dict[str, Any]]
    run_event_candidates: list[dict[str, Any]]
    run_market_targets: list[dict[str, Any]]
    unresolved_reason_counter: dict[str, int]
    candidate_debug: list[dict[str, Any]]


class LinkService:
    def __init__(self, *, db: Database):
        self._db = db

    def _load_match_rules(self, *, mapping: LoadedMapping, league: str) -> _LeagueMatchRules:
        cfg = mapping.league_match_rules.get(_norm(league), mapping.league_match_rules.get("default", {}))
        if not isinstance(cfg, dict):
            cfg = {}
        return _LeagueMatchRules(
            date_tolerance_days=max(0, int(cfg.get("date_tolerance_days", 0) or 0)),
            kickoff_tolerance_minutes=max(0, int(cfg.get("kickoff_tolerance_minutes", 180) or 180)),
            provider_order_reliable=bool(cfg.get("provider_order_reliable", False)),
            pm_order_reliable=bool(cfg.get("pm_order_reliable", False)),
        )

    def _build_provider_team_alias_index(self, mapping: LoadedMapping, provider: str) -> dict[tuple[str, str], str]:
        out: dict[tuple[str, str], str] = {}
        p = _norm(provider)
        for league, teams in mapping.team_map.items():
            lk = _norm(league)
            if not isinstance(teams, dict):
                continue
            for canonical_team, meta in teams.items():
                ct = _norm(canonical_team)
                if not ct:
                    continue
                aliases = meta.get("provider_aliases", {}) if isinstance(meta, dict) else {}
                if isinstance(aliases, dict):
                    for alias in aliases.get(p, []):
                        an = _norm(alias)
                        if an:
                            out[(lk, an)] = ct
                out[(lk, ct)] = ct
        return out

    def _build_pm_team_alias_index(self, mapping: LoadedMapping) -> dict[tuple[str, str], str]:
        out: dict[tuple[str, str], str] = {}
        for league, teams in mapping.team_map.items():
            lk = _norm(league)
            if not isinstance(teams, dict):
                continue
            for canonical_team, meta in teams.items():
                ct = _norm(canonical_team)
                if not ct or not isinstance(meta, dict):
                    continue
                keys: set[str] = {ct}
                pm_code = _norm(str(meta.get("polymarket_code") or ""))
                if pm_code:
                    keys.add(pm_code)
                for alias in meta.get("pm_aliases", []) if isinstance(meta.get("pm_aliases"), list) else []:
                    an = _norm(alias)
                    if an:
                        keys.add(an)
                for abbr in meta.get("pm_abbreviations", []) if isinstance(meta.get("pm_abbreviations"), list) else []:
                    an = _norm(abbr)
                    if an:
                        keys.add(an)
                for key in keys:
                    out[(lk, key)] = ct
        return out

    def _build_slug_hints(
        self,
        *,
        mapping: LoadedMapping,
        league: str,
        home_team: str,
        away_team: str,
        game_date_et: str,
    ) -> tuple[str, ...]:
        league_meta = mapping.leagues.get(_norm(league), {})
        league_code = _norm(str(league_meta.get("polymarket_league_code") or ""))
        teams = mapping.team_map.get(_norm(league), {})
        if not league_code or not isinstance(teams, dict):
            return tuple()
        home_meta = teams.get(home_team, {}) if isinstance(teams, dict) else {}
        away_meta = teams.get(away_team, {}) if isinstance(teams, dict) else {}
        home_code = _norm(str(home_meta.get("polymarket_code") or ""))
        away_code = _norm(str(away_meta.get("polymarket_code") or ""))
        date_text = str(game_date_et or "").strip()
        if not home_code or not away_code or len(date_text) != 10:
            return tuple()
        home_first = f"{league_code}-{home_code}-{away_code}-{date_text}"
        away_first = f"{league_code}-{away_code}-{home_code}-{date_text}"
        pm_ordering = _norm(str(mapping.pm_league_orderings.get(_norm(league), "home")))
        ordered = [away_first, home_first] if pm_ordering == "away" else [home_first, away_first]
        deduped: list[str] = []
        for s in ordered:
            if s not in deduped:
                deduped.append(s)
        return tuple(deduped)

    def _resolve_provider_game(
        self,
        *,
        row: dict[str, Any],
        mapping: LoadedMapping,
        provider: str,
        team_alias_idx: dict[tuple[str, str], str],
        league_scope: str,
        live_betting_leagues: set[str],
    ) -> tuple[_ResolvedProviderGame | None, str]:
        league_raw = _norm(row.get("league_raw") or "")
        sport_raw = _norm(row.get("sport_raw") or "")
        category_name = str(row.get("category_name") or "").strip()

        # Strip composite "Name|ID" suffix for league resolution (Opta encoding).
        # Backward-compatible: if no "|", rsplit returns the whole string.
        league_raw_name = league_raw.rsplit("|", 1)[0].strip() if "|" in league_raw else league_raw
        category_name_stripped = category_name.rsplit("|", 1)[0].strip() if "|" in category_name else category_name
        league_signal = league_raw_name or sport_raw

        # Try country-qualified disambiguation first (PROVIDER_LEAGUE_COUNTRY).
        canonical_league = ""
        if category_name_stripped:
            raw_country_map = mapping.provider_league_country.get(_norm(provider), {})
            # Support both "country|league" string keys and (country, league) tuple keys
            country_key_str = f"{_norm(category_name_stripped)}|{_norm(league_signal)}"
            country_key_tuple = (_norm(category_name_stripped), _norm(league_signal))
            matched = raw_country_map.get(country_key_str) or raw_country_map.get(country_key_tuple, "")
            canonical_league = _norm(str(matched))

        # Fall back to unambiguous aliases (PROVIDER_LEAGUE_ALIASES).
        if not canonical_league:
            provider_alias_map = mapping.provider_league_aliases.get(_norm(provider), {})
            canonical_league = _norm(provider_alias_map.get(league_signal) or "")

        # Fall back to normalize_league_key.
        if not canonical_league:
            normalized_guess = _norm(normalize_league_key(league_signal))
            if normalized_guess in mapping.leagues:
                canonical_league = normalized_guess

        if not canonical_league:
            return (None, "league_unmapped")
        if league_scope == "live" and canonical_league not in live_betting_leagues:
            return (None, "league_out_of_scope")
        league_meta = mapping.leagues.get(canonical_league, {})
        polymarket_league_key = _norm(str(league_meta.get("polymarket_league_code") or ""))
        if not polymarket_league_key:
            return (None, "league_unmapped")

        home_raw = _norm(row.get("home_raw") or "")
        away_raw = _norm(row.get("away_raw") or "")
        if not home_raw or not away_raw:
            return (None, "missing_team_parse")

        home_team = team_alias_idx.get((canonical_league, home_raw), "")
        away_team = team_alias_idx.get((canonical_league, away_raw), "")
        if not home_team or not away_team:
            return (None, "team_alias_unmapped")
        if home_team == away_team:
            return (None, "identical_teams")

        game_date_et = str(row.get("game_date_et") or "").strip()
        if len(game_date_et) != 10:
            return (None, "missing_game_date")
        try:
            date.fromisoformat(game_date_et)
        except ValueError:
            return (None, "missing_game_date")

        slug_hints = self._build_slug_hints(
            mapping=mapping,
            league=canonical_league,
            home_team=home_team,
            away_team=away_team,
            game_date_et=game_date_et,
        )

        return (
            _ResolvedProviderGame(
                provider_game_id=str(row.get("provider_game_id") or ""),
                canonical_league=canonical_league,
                polymarket_league_key=polymarket_league_key,
                canonical_home_team=home_team,
                canonical_away_team=away_team,
                canonical_team_pair=tuple(sorted((home_team, away_team))),
                game_date_et=game_date_et,
                provider_start_ts_utc=_int_or_none(row.get("start_ts_utc")),
                slug_hints=slug_hints,
            ),
            "ok",
        )

    def _map_pm_team_to_canonical(
        self,
        *,
        league: str,
        team_row: dict[str, Any],
        pm_alias_idx: dict[tuple[str, str], str],
    ) -> str:
        lk = _norm(league)
        keys = [
            _norm(team_row.get("name") or ""),
            _norm(team_row.get("abbreviation") or ""),
            _norm(team_row.get("alias") or ""),
        ]
        matches = {pm_alias_idx.get((lk, k), "") for k in keys if k}
        matches.discard("")
        if len(matches) == 1:
            return next(iter(matches))
        return ""

    def _select_event_for_provider_game(
        self,
        *,
        resolved: _ResolvedProviderGame,
        mapping: LoadedMapping,
        pm_alias_idx: dict[tuple[str, str], str],
    ) -> tuple[_EventChoice | None, str, dict[str, Any]]:
        rules = self._load_match_rules(mapping=mapping, league=resolved.canonical_league)
        date_from = _date_add_days(resolved.game_date_et, -rules.date_tolerance_days)
        date_to = _date_add_days(resolved.game_date_et, rules.date_tolerance_days)
        events = self._db.markets.load_pm_events_by_league_and_date_range(
            league_key=resolved.polymarket_league_key,
            date_from=date_from,
            date_to=date_to,
        )
        # Supplementary kickoff-based loading (catches postponed games whose
        # game_date_et is stale but kickoff_ts_utc was updated).
        if resolved.provider_start_ts_utc is not None:
            tol_sec = int(rules.kickoff_tolerance_minutes) * 60
            kickoff_events = self._db.markets.load_pm_events_by_league_and_kickoff_range(
                league_key=resolved.polymarket_league_key,
                kickoff_from_utc=resolved.provider_start_ts_utc - tol_sec,
                kickoff_to_utc=resolved.provider_start_ts_utc + tol_sec,
            )
            seen = {e["event_id"] for e in events}
            for e in kickoff_events:
                if e["event_id"] not in seen:
                    events.append(e)
                    seen.add(e["event_id"])
        diagnostics: dict[str, Any] = {
            "candidate_window": {
                "league": resolved.canonical_league,
                "polymarket_league_key": resolved.polymarket_league_key,
                "date_from": date_from,
                "date_to": date_to,
            },
            "kickoff_tolerance_minutes": int(rules.kickoff_tolerance_minutes),
            "used_slug_fallback": False,
            "n_events_scanned": len(events),
            "n_team_matched": 0,
            "selected_event_id": "",
            "selected_event_ids": [],
            "selected_slug": "",
            "selected_kickoff_ts_utc": None,
            "provider_start_ts_utc": resolved.provider_start_ts_utc,
            "selected_score_tuple": [],
            "failure_reason": "",
            "candidates": [],
        }
        if not events:
            diagnostics["failure_reason"] = "no_event_candidates"
            return (None, "no_event_candidates", diagnostics)

        event_ids = [str(e.get("event_id") or "") for e in events if str(e.get("event_id") or "")]
        team_rows = self._db.markets.load_event_teams_for_event_ids(event_ids)
        teams_by_event: dict[str, set[str]] = defaultdict(set)
        ordered_teams_by_event: dict[str, list[str]] = defaultdict(list)
        mapped_any = False
        for row in team_rows:
            event_id = str(row.get("event_id") or "")
            if not event_id:
                continue
            canonical_team = self._map_pm_team_to_canonical(
                league=resolved.canonical_league,
                team_row=row,
                pm_alias_idx=pm_alias_idx,
            )
            if not canonical_team:
                continue
            mapped_any = True
            teams_by_event[event_id].add(canonical_team)
            if canonical_team not in ordered_teams_by_event[event_id]:
                ordered_teams_by_event[event_id].append(canonical_team)

        candidate_map: dict[str, dict[str, Any]] = {}
        for ev in events:
            ev_id = str(ev.get("event_id") or "")
            slug_raw = _norm(str(ev.get("slug_raw") or ev.get("slug") or ""))
            kickoff_ts = _int_or_none(ev.get("kickoff_ts_utc"))
            candidate_map[ev_id] = {
                "event_id": ev_id,
                "event_slug": slug_raw,
                "kickoff_ts_utc": kickoff_ts,
                "team_set_match": 0,
                "kickoff_within_tolerance": None,
                "slug_hint_match": 0,
                "ordering_bonus": 0,
                "kickoff_delta_sec": None,
                "score_tuple": [],
                "is_selected": 0,
                "reject_reason": "",
            }

        matched_events: list[dict[str, Any]] = []
        used_slug_fallback = False
        if mapped_any:
            target_set = set(resolved.canonical_team_pair)
            # Strict home/away ordering for soccer: reject provider games whose
            # home/away is flipped relative to the PM event. This filters out
            # BoltOdds duplicate entries with swapped team designation.
            # Baseball is excluded because PM uses away-first ordering and
            # provider home/away is less reliable.
            league_cfg = mapping.leagues.get(resolved.canonical_league, {})
            sport_family = _norm(str(league_cfg.get("sport_family", "")))
            enforce_order = sport_family == "soccer"
            pm_ordering = _norm(str(mapping.pm_league_orderings.get(resolved.canonical_league, "home")))
            for ev in events:
                ev_id = str(ev.get("event_id") or "")
                if not ev_id:
                    continue
                if teams_by_event.get(ev_id, set()) != target_set:
                    if ev_id in candidate_map:
                        candidate_map[ev_id]["reject_reason"] = "team_set_not_found"
                    continue
                if enforce_order:
                    ev_ordered = ordered_teams_by_event.get(ev_id, [])
                    if len(ev_ordered) >= 2:
                        if pm_ordering == "away":
                            pm_home, pm_away = ev_ordered[1], ev_ordered[0]
                        else:
                            pm_home, pm_away = ev_ordered[0], ev_ordered[1]
                        if pm_home != resolved.canonical_home_team or pm_away != resolved.canonical_away_team:
                            if ev_id in candidate_map:
                                candidate_map[ev_id]["reject_reason"] = "home_away_order_mismatch"
                            continue
                matched_events.append(ev)
                if ev_id in candidate_map:
                    candidate_map[ev_id]["team_set_match"] = 1
            if not matched_events:
                diagnostics["used_slug_fallback"] = False
                diagnostics["failure_reason"] = "team_set_not_found"
                diagnostics["candidates"] = sorted(candidate_map.values(), key=lambda x: (str(x["event_id"]),))
                return (None, "team_set_not_found", diagnostics)
        else:
            # Backward-compatible fallback for legacy fixtures without pm_event_teams.
            used_slug_fallback = True
            hint_set = set(resolved.slug_hints)
            for ev in events:
                ev_id = str(ev.get("event_id") or "")
                slug_raw = _norm(str(ev.get("slug_raw") or ev.get("slug") or ""))
                if slug_raw and slug_raw in hint_set:
                    matched_events.append(ev)
                    if ev_id in candidate_map:
                        candidate_map[ev_id]["team_set_match"] = 1
                        candidate_map[ev_id]["slug_hint_match"] = 1
                elif ev_id in candidate_map:
                    candidate_map[ev_id]["reject_reason"] = "team_set_not_found"
            if not matched_events:
                diagnostics["used_slug_fallback"] = True
                diagnostics["failure_reason"] = "team_set_not_found"
                diagnostics["candidates"] = sorted(candidate_map.values(), key=lambda x: (str(x["event_id"]),))
                return (None, "team_set_not_found", diagnostics)

        provider_ts = resolved.provider_start_ts_utc
        tol_sec = int(rules.kickoff_tolerance_minutes) * 60
        with_kickoff = [ev for ev in matched_events if _int_or_none(ev.get("kickoff_ts_utc")) is not None]
        if provider_ts is not None and with_kickoff:
            for ev in matched_events:
                ev_id = str(ev.get("event_id") or "")
                kickoff_ts = _int_or_none(ev.get("kickoff_ts_utc"))
                if ev_id in candidate_map and kickoff_ts is not None:
                    delta = abs(int(kickoff_ts) - int(provider_ts))
                    candidate_map[ev_id]["kickoff_delta_sec"] = int(delta)
                    candidate_map[ev_id]["kickoff_within_tolerance"] = 1 if int(delta) <= tol_sec else 0
            within = [
                ev
                for ev in with_kickoff
                if abs(int(_int_or_none(ev.get("kickoff_ts_utc")) or 0) - int(provider_ts)) <= tol_sec
            ]
            if within:
                matched_events = within
            else:
                for ev in matched_events:
                    ev_id = str(ev.get("event_id") or "")
                    if ev_id in candidate_map and not candidate_map[ev_id]["reject_reason"]:
                        candidate_map[ev_id]["reject_reason"] = "kickoff_out_of_tolerance"
                diagnostics["used_slug_fallback"] = bool(used_slug_fallback)
                diagnostics["failure_reason"] = "kickoff_out_of_tolerance"
                diagnostics["n_team_matched"] = len(matched_events)
                diagnostics["candidates"] = sorted(candidate_map.values(), key=lambda x: (str(x["event_id"]),))
                return (None, "kickoff_out_of_tolerance", diagnostics)

        pm_ordering = _norm(str(mapping.pm_league_orderings.get(resolved.canonical_league, "home")))
        expected_order = (
            [resolved.canonical_away_team, resolved.canonical_home_team]
            if pm_ordering == "away"
            else [resolved.canonical_home_team, resolved.canonical_away_team]
        )

        slug_hints_set = set(resolved.slug_hints)
        scored: list[tuple[tuple[int, int, int], str, dict[str, Any]]] = []
        for ev in matched_events:
            ev_id = str(ev.get("event_id") or "")
            slug_raw = _norm(str(ev.get("slug_raw") or ev.get("slug") or ""))
            kickoff_ts = _int_or_none(ev.get("kickoff_ts_utc"))
            kickoff_delta = abs(int(kickoff_ts) - int(provider_ts)) if kickoff_ts is not None and provider_ts is not None else 10**12
            slug_bonus = 0
            for hint in slug_hints_set:
                if slug_raw == hint or slug_raw.startswith(f"{hint}-"):
                    slug_bonus = 1
                    break
            ordering_bonus = 0
            if rules.provider_order_reliable and rules.pm_order_reliable:
                observed = ordered_teams_by_event.get(ev_id, [])
                if len(observed) >= 2 and observed[0] == expected_order[0] and observed[1] == expected_order[1]:
                    ordering_bonus = 1
            if ev_id in candidate_map:
                candidate_map[ev_id]["ordering_bonus"] = int(ordering_bonus)
                candidate_map[ev_id]["slug_hint_match"] = int(slug_bonus)
                candidate_map[ev_id]["score_tuple"] = [int(kickoff_delta), -int(slug_bonus), -int(ordering_bonus)]
            scored.append(((int(kickoff_delta), -int(slug_bonus), -int(ordering_bonus)), ev_id, ev))

        scored.sort(key=lambda x: (x[0], x[1]))
        if not scored:
            diagnostics["used_slug_fallback"] = bool(used_slug_fallback)
            diagnostics["failure_reason"] = "no_event_candidates"
            diagnostics["n_team_matched"] = len(matched_events)
            diagnostics["candidates"] = sorted(candidate_map.values(), key=lambda x: (str(x["event_id"]),))
            return (None, "no_event_candidates", diagnostics)
        top_score = scored[0][0]
        top_scored = [entry for entry in scored if entry[0] == top_score]
        cluster_by_event_id: dict[str, str] = {}
        for _score, ev_id, ev in scored:
            cluster_by_event_id[ev_id] = self._slug_prefix(_norm(str(ev.get("slug_raw") or ev.get("slug") or "")))
        top_clusters = {
            cluster_by_event_id.get(ev_id, "")
            for _score, ev_id, _ev in top_scored
            if cluster_by_event_id.get(ev_id, "")
        }
        anchor_cluster = ""
        if len(top_clusters) == 1:
            anchor_cluster = next(iter(top_clusters))
        else:
            top_slug_hint_hits = [
                entry
                for entry in top_scored
                if int(candidate_map.get(entry[1], {}).get("slug_hint_match") or 0) == 1
            ]
            if len(top_slug_hint_hits) == 1:
                anchor_cluster = cluster_by_event_id.get(str(top_slug_hint_hits[0][1]), "")
            elif len(top_slug_hint_hits) > 1:
                hit_clusters = {
                    cluster_by_event_id.get(str(entry[1]), "")
                    for entry in top_slug_hint_hits
                    if cluster_by_event_id.get(str(entry[1]), "")
                }
                if len(hit_clusters) == 1:
                    anchor_cluster = next(iter(hit_clusters))

        if not anchor_cluster:
            tie_tuple = list(top_score)
            for _score, ev_id, _ev in scored:
                if _score == top_score and ev_id in candidate_map:
                    candidate_map[ev_id]["reject_reason"] = "ambiguous_event_match"
            diagnostics["used_slug_fallback"] = bool(used_slug_fallback)
            diagnostics["failure_reason"] = "ambiguous_event_match"
            diagnostics["n_team_matched"] = len(matched_events)
            diagnostics["selected_score_tuple"] = tie_tuple
            diagnostics["candidates"] = sorted(
                candidate_map.values(),
                key=lambda x: (
                    x["score_tuple"] if isinstance(x.get("score_tuple"), list) and x["score_tuple"] else [10**12, 0, 0],
                    str(x.get("event_id") or ""),
                ),
            )
            for i, cand in enumerate(diagnostics["candidates"], start=1):
                cand["candidate_rank"] = int(i)
            return (None, "ambiguous_event_match", diagnostics)

        selected_scored = [
            entry for entry in scored if cluster_by_event_id.get(str(entry[1]), "") == anchor_cluster
        ]
        if not selected_scored:
            diagnostics["used_slug_fallback"] = bool(used_slug_fallback)
            diagnostics["failure_reason"] = "no_event_candidates"
            diagnostics["n_team_matched"] = len(matched_events)
            diagnostics["candidates"] = sorted(
                candidate_map.values(),
                key=lambda x: (
                    x["score_tuple"] if isinstance(x.get("score_tuple"), list) and x["score_tuple"] else [10**12, 0, 0],
                    str(x.get("event_id") or ""),
                ),
            )
            for i, cand in enumerate(diagnostics["candidates"], start=1):
                cand["candidate_rank"] = int(i)
            return (None, "no_event_candidates", diagnostics)

        selected_scored.sort(key=lambda x: (x[0], x[1]))
        primary_selected = selected_scored[0][2]
        selected_slug = _norm(str(primary_selected.get("slug_raw") or primary_selected.get("slug") or ""))
        selected_score_tuple = list(selected_scored[0][0])
        selected_event_id = str(primary_selected.get("event_id") or "")
        selected_event_ids = [str(entry[1]) for entry in selected_scored if str(entry[1])]
        selected_events = tuple(entry[2] for entry in selected_scored)
        selected_ids_set = set(selected_event_ids)
        for _score, ev_id, _ev in scored:
            if ev_id in selected_ids_set:
                if ev_id in candidate_map:
                    candidate_map[ev_id]["is_selected"] = 1
                    candidate_map[ev_id]["reject_reason"] = ""
                continue
            if ev_id in candidate_map and not candidate_map[ev_id]["reject_reason"]:
                candidate_map[ev_id]["reject_reason"] = "other_cluster"
        diagnostics["used_slug_fallback"] = bool(used_slug_fallback)
        diagnostics["n_team_matched"] = len(matched_events)
        diagnostics["selected_event_id"] = selected_event_id
        diagnostics["selected_event_ids"] = selected_event_ids
        diagnostics["selected_slug"] = selected_slug
        diagnostics["provider_start_ts_utc"] = provider_ts
        diagnostics["selected_kickoff_ts_utc"] = _int_or_none(primary_selected.get("kickoff_ts_utc"))
        diagnostics["selected_score_tuple"] = selected_score_tuple
        diagnostics["failure_reason"] = ""
        diagnostics["candidates"] = sorted(
            candidate_map.values(),
            key=lambda x: (
                x["score_tuple"] if isinstance(x.get("score_tuple"), list) and x["score_tuple"] else [10**12, 0, 0],
                str(x.get("event_id") or ""),
            ),
        )
        for i, cand in enumerate(diagnostics["candidates"], start=1):
            cand["candidate_rank"] = int(i)
        return (
            _EventChoice(
                event=primary_selected,
                events=selected_events,
                slug_prefix=self._slug_prefix(selected_slug),
                diagnostics=diagnostics,
            ),
            "ok",
            diagnostics,
        )

    @staticmethod
    def _slug_prefix(slug_raw: str) -> str:
        text = _norm(slug_raw)
        if not text:
            return ""
        parts = text.split("-")
        if len(parts) >= 6:
            return "-".join(parts[:6])
        return text

    @staticmethod
    def _sports_market_type_from_market(*, market_row: dict[str, Any]) -> str:
        return normalize_sports_market_type(market_row.get("sports_market_type"))

    @staticmethod
    def _resolution_state(
        *,
        binding_status: str,
        reason_code: str,
        is_tradeable: bool,
        has_target_warnings: bool,
    ) -> str:
        bs = _norm(binding_status)
        rc = _norm(reason_code)
        if bs == "exact" and is_tradeable:
            return "MATCHED_WITH_WARNINGS" if has_target_warnings else "MATCHED_CLEAN"
        if rc == "ambiguous_event_match":
            return "AMBIGUOUS_EVENT_MATCH"
        if rc == "team_set_not_found":
            return "TEAM_SET_NOT_FOUND"
        if rc == "no_event_candidates":
            return "NO_EVENT_CANDIDATES"
        if rc == "no_tradeable_targets":
            return "NO_TRADEABLE_TARGETS"
        return rc.upper() if rc else "UNRESOLVED"

    def _process_provider_league(
        self,
        *,
        provider: str,
        league: str | None = None,
        mapping: LoadedMapping,
        live_policy: LoadedLiveTradingPolicy | None = None,
        league_scope: str = "live",
        horizon_hours: float | None = None,
    ) -> _LinkBuildBatch:
        """Process games for one (provider, league) pair without writing to DB.

        Returns a _LinkBuildBatch containing all accumulated rows and stats,
        ready for persistence via _persist_link_run.
        """
        p = _norm(provider)
        scope = _norm(league_scope)
        if scope not in {"live", "all"}:
            raise ValueError("league_scope must be live|all")
        league_filter = _norm(league) if league else ""

        provider_rows = self._db.linking.load_provider_games(provider=p)

        # Filter by kickoff time window if horizon_hours is set.
        n_dropped_horizon = 0
        if horizon_hours is not None and horizon_hours > 0:
            now_ts_horizon = int(time.time())
            upper_ts = int(now_ts_horizon + (horizon_hours * 3600))
            lower_ts = int(now_ts_horizon - (6 * 3600))
            filtered = []
            for row in provider_rows:
                kickoff = _int_or_none(row.get("start_ts_utc"))
                if kickoff is None:
                    filtered.append(row)  # keep games with unknown kickoff
                    continue
                if lower_ts <= kickoff <= upper_ts:
                    filtered.append(row)
                else:
                    n_dropped_horizon += 1
            provider_rows = filtered
            if n_dropped_horizon > 0:
                log.info(
                    "link build: %s/%s: dropped %d games outside horizon window [-%dh, +%dh]",
                    p, league_filter, n_dropped_horizon, 6, int(horizon_hours),
                )

        n_league_filtered = 0
        n_games_seen = len(provider_rows)
        policy = live_policy or load_live_trading_policy()
        unknown_live_leagues = sorted(x for x in policy.live_betting_leagues if x not in mapping.leagues)
        if unknown_live_leagues:
            raise MappingValidationError(
                f"LIVE_BETTING_LEAGUES contains unknown league(s): {','.join(unknown_live_leagues)}"
            )
        unknown_policy_market_type_leagues = sorted(
            x for x in policy.live_betting_market_types_by_league if x not in mapping.leagues
        )
        if unknown_policy_market_type_leagues:
            raise MappingValidationError(
                "LIVE_BETTING_MARKET_TYPES contains unknown league(s): "
                + ",".join(unknown_policy_market_type_leagues)
            )
        valid_market_types = {_norm(x) for x in self._db.markets.load_valid_sports_market_types() if _norm(x)}
        if valid_market_types:
            unknown_market_types: dict[str, list[str]] = {}
            for lk, types in policy.live_betting_market_types_by_league.items():
                bad = sorted(t for t in types if t not in valid_market_types)
                if bad:
                    unknown_market_types[lk] = bad
            if unknown_market_types:
                parts = [f"{lk}:[{','.join(vals)}]" for lk, vals in sorted(unknown_market_types.items())]
                raise MappingValidationError(
                    "LIVE_BETTING_MARKET_TYPES contains unknown market type(s) vs pm_sports_market_types_ref: "
                    + "; ".join(parts)
                )

        team_alias_idx = self._build_provider_team_alias_index(mapping, p)
        pm_alias_idx = self._build_pm_team_alias_index(mapping)

        game_binding_rows: list[tuple[Any, ...]] = []
        event_binding_rows: list[tuple[Any, ...]] = []
        market_binding_rows: list[tuple[Any, ...]] = []
        run_provider_games: list[dict[str, Any]] = []
        run_game_reviews: list[dict[str, Any]] = []
        run_event_candidates: list[dict[str, Any]] = []
        run_market_targets: list[dict[str, Any]] = []
        unresolved_reason_counter: Counter[str] = Counter()
        candidate_debug: list[dict[str, Any]] = []

        now_ts = int(time.time())

        n_games_linked = 0
        n_games_tradeable = 0
        n_targets = 0
        n_targets_tradeable = 0

        for row in provider_rows:
            provider_game_id = str(row.get("provider_game_id") or "")
            parse_status = str(row.get("parse_status") or "")
            parse_reason = str(row.get("parse_reason") or "")
            game_label = str(row.get("game_label") or "")
            sport_raw = str(row.get("sport_raw") or "")
            league_raw = str(row.get("league_raw") or "")
            when_raw = str(row.get("when_raw") or "")
            start_ts_utc = _int_or_none(row.get("start_ts_utc"))
            game_date_et = str(row.get("game_date_et") or "")
            home_raw = str(row.get("home_raw") or "")
            away_raw = str(row.get("away_raw") or "")
            resolved, reason = self._resolve_provider_game(
                row=row,
                mapping=mapping,
                provider=p,
                team_alias_idx=team_alias_idx,
                league_scope=scope,
                live_betting_leagues=policy.live_betting_leagues,
            )
            if resolved is not None and league_filter and resolved.canonical_league != league_filter:
                n_league_filtered += 1
                continue  # skip games outside the requested league

            if resolved is None:
                unresolved_reason_counter[reason] += 1
                game_binding_status = "unresolved"
                game_reason_code = reason
                game_binding_rows.append(
                    (
                        p,
                        provider_game_id,
                        "",
                        "",
                        "",
                        "",
                        game_binding_status,
                        game_reason_code,
                        0,
                        mapping.mapping_version,
                        mapping.mapping_hash,
                        None,
                        now_ts,
                    )
                )
                run_provider_games.append(
                    {
                        "provider_game_id": provider_game_id,
                        "parse_status": parse_status,
                        "parse_reason": parse_reason,
                        "game_label": game_label,
                        "sport_raw": sport_raw,
                        "league_raw": league_raw,
                        "when_raw": when_raw,
                        "start_ts_utc": start_ts_utc,
                        "game_date_et": game_date_et,
                        "home_raw": home_raw,
                        "away_raw": away_raw,
                        "canonical_league": "",
                        "canonical_home_team": "",
                        "canonical_away_team": "",
                        "event_slug_prefix": "",
                        "binding_status": game_binding_status,
                        "reason_code": game_reason_code,
                        "is_tradeable": 0,
                    }
                )
                run_game_reviews.append(
                    {
                        "provider_game_id": provider_game_id,
                        "resolution_state": self._resolution_state(
                            binding_status=game_binding_status,
                            reason_code=game_reason_code,
                            is_tradeable=False,
                            has_target_warnings=False,
                        ),
                        "reason_code": game_reason_code,
                        "selected_event_id": "",
                        "selected_event_slug": "",
                        "used_slug_fallback": 0,
                        "kickoff_tolerance_minutes": 0,
                        "kickoff_delta_sec": None,
                        "score_tuple": [],
                        "trace_json": {
                            "canonicalization": {
                                "league_raw": _norm(row.get("league_raw") or row.get("sport_raw") or ""),
                                "home_raw": _norm(row.get("home_raw") or ""),
                                "away_raw": _norm(row.get("away_raw") or ""),
                            },
                            "failure_reason": game_reason_code,
                        },
                    }
                )
                continue

            choice, reason, selection_diag = self._select_event_for_provider_game(
                resolved=resolved,
                mapping=mapping,
                pm_alias_idx=pm_alias_idx,
            )
            selection_candidates = (
                list(selection_diag.get("candidates") or []) if isinstance(selection_diag, dict) else []
            )
            for cand in selection_candidates:
                run_event_candidates.append(
                    {
                        "provider_game_id": provider_game_id,
                        "candidate_rank": int(cand.get("candidate_rank") or 0),
                        "event_id": str(cand.get("event_id") or ""),
                        "event_slug": str(cand.get("event_slug") or ""),
                        "kickoff_ts_utc": _int_or_none(cand.get("kickoff_ts_utc")),
                        "team_set_match": 1 if int(cand.get("team_set_match") or 0) == 1 else 0,
                        "kickoff_within_tolerance": _int_or_none(cand.get("kickoff_within_tolerance")),
                        "slug_hint_match": 1 if int(cand.get("slug_hint_match") or 0) == 1 else 0,
                        "ordering_bonus": int(cand.get("ordering_bonus") or 0),
                        "kickoff_delta_sec": _int_or_none(cand.get("kickoff_delta_sec")),
                        "score_tuple": list(cand.get("score_tuple") or []),
                        "is_selected": 1 if int(cand.get("is_selected") or 0) == 1 else 0,
                        "reject_reason": str(cand.get("reject_reason") or ""),
                    }
                )
            if choice is None:
                unresolved_reason_counter[reason] += 1
                game_binding_status = "unresolved"
                game_reason_code = reason
                candidate_debug.append(
                    {
                        "provider_game_id": provider_game_id,
                        "league": resolved.canonical_league,
                        "home": resolved.canonical_home_team,
                        "away": resolved.canonical_away_team,
                        "game_date_et": resolved.game_date_et,
                        "reason": reason,
                    }
                )
                game_binding_rows.append(
                    (
                        p,
                        provider_game_id,
                        resolved.canonical_league,
                        resolved.canonical_home_team,
                        resolved.canonical_away_team,
                        resolved.slug_hints[0] if resolved.slug_hints else "",
                        game_binding_status,
                        game_reason_code,
                        0,
                        mapping.mapping_version,
                        mapping.mapping_hash,
                        None,
                        now_ts,
                    )
                )
                run_provider_games.append(
                    {
                        "provider_game_id": provider_game_id,
                        "parse_status": parse_status,
                        "parse_reason": parse_reason,
                        "game_label": game_label,
                        "sport_raw": sport_raw,
                        "league_raw": league_raw,
                        "when_raw": when_raw,
                        "start_ts_utc": start_ts_utc,
                        "game_date_et": game_date_et,
                        "home_raw": home_raw,
                        "away_raw": away_raw,
                        "canonical_league": resolved.canonical_league,
                        "canonical_home_team": resolved.canonical_home_team,
                        "canonical_away_team": resolved.canonical_away_team,
                        "event_slug_prefix": resolved.slug_hints[0] if resolved.slug_hints else "",
                        "binding_status": game_binding_status,
                        "reason_code": game_reason_code,
                        "is_tradeable": 0,
                    }
                )
                run_game_reviews.append(
                    {
                        "provider_game_id": provider_game_id,
                        "resolution_state": self._resolution_state(
                            binding_status=game_binding_status,
                            reason_code=game_reason_code,
                            is_tradeable=False,
                            has_target_warnings=False,
                        ),
                        "reason_code": game_reason_code,
                        "selected_event_id": "",
                        "selected_event_slug": "",
                        "used_slug_fallback": 1 if bool(selection_diag.get("used_slug_fallback")) else 0,
                        "kickoff_tolerance_minutes": int(selection_diag.get("kickoff_tolerance_minutes") or 0),
                        "kickoff_delta_sec": _int_or_none(selection_diag.get("selected_kickoff_delta_sec")),
                        "score_tuple": list(selection_diag.get("selected_score_tuple") or []),
                        "trace_json": {
                            "canonicalization": {
                                "league": resolved.canonical_league,
                                "home": resolved.canonical_home_team,
                                "away": resolved.canonical_away_team,
                                "slug_hints": list(resolved.slug_hints),
                            },
                            "selection": selection_diag,
                            "failure_reason": game_reason_code,
                        },
                    }
                )
                continue

            event_id = str(choice.event.get("event_id") or "")
            if not event_id:
                unresolved_reason_counter["no_event_candidates"] += 1
                game_binding_status = "unresolved"
                game_reason_code = "no_event_candidates"
                game_binding_rows.append(
                    (
                        p,
                        provider_game_id,
                        resolved.canonical_league,
                        resolved.canonical_home_team,
                        resolved.canonical_away_team,
                        choice.slug_prefix,
                        game_binding_status,
                        game_reason_code,
                        0,
                        mapping.mapping_version,
                        mapping.mapping_hash,
                        None,
                        now_ts,
                    )
                )
                run_provider_games.append(
                    {
                        "provider_game_id": provider_game_id,
                        "parse_status": parse_status,
                        "parse_reason": parse_reason,
                        "game_label": game_label,
                        "sport_raw": sport_raw,
                        "league_raw": league_raw,
                        "when_raw": when_raw,
                        "start_ts_utc": start_ts_utc,
                        "game_date_et": game_date_et,
                        "home_raw": home_raw,
                        "away_raw": away_raw,
                        "canonical_league": resolved.canonical_league,
                        "canonical_home_team": resolved.canonical_home_team,
                        "canonical_away_team": resolved.canonical_away_team,
                        "event_slug_prefix": choice.slug_prefix,
                        "binding_status": game_binding_status,
                        "reason_code": game_reason_code,
                        "is_tradeable": 0,
                    }
                )
                run_game_reviews.append(
                    {
                        "provider_game_id": provider_game_id,
                        "resolution_state": self._resolution_state(
                            binding_status=game_binding_status,
                            reason_code=game_reason_code,
                            is_tradeable=False,
                            has_target_warnings=False,
                        ),
                        "reason_code": game_reason_code,
                        "selected_event_id": "",
                        "selected_event_slug": "",
                        "used_slug_fallback": 1 if bool(selection_diag.get("used_slug_fallback")) else 0,
                        "kickoff_tolerance_minutes": int(selection_diag.get("kickoff_tolerance_minutes") or 0),
                        "kickoff_delta_sec": _int_or_none(selection_diag.get("selected_kickoff_delta_sec")),
                        "score_tuple": list(selection_diag.get("selected_score_tuple") or []),
                        "trace_json": {
                            "canonicalization": {
                                "league": resolved.canonical_league,
                                "home": resolved.canonical_home_team,
                                "away": resolved.canonical_away_team,
                                "slug_hints": list(resolved.slug_hints),
                            },
                            "selection": selection_diag,
                            "failure_reason": game_reason_code,
                        },
                    }
                )
                continue

            n_games_linked += 1
            game_targets_tradeable = 0
            game_has_target_warnings = False
            selected_events = tuple(choice.events) if choice.events else (choice.event,)
            selected_event_ids = [str(ev.get("event_id") or "") for ev in selected_events if str(ev.get("event_id") or "")]
            for selected_event_id in selected_event_ids:
                event_binding_rows.append((p, provider_game_id, selected_event_id, choice.slug_prefix, now_ts))

            markets = self._db.markets.load_markets_for_event_ids(selected_event_ids)
            allowed_market_types_raw = {
                _norm(x)
                for x in policy.live_betting_market_types_by_league.get(
                    resolved.canonical_league,
                    set(),
                )
                if _norm(x)
            }
            allowed_market_types_canonical = {
                normalize_sports_market_type(x)
                for x in allowed_market_types_raw
                if normalize_sports_market_type(x) != "other"
            }
            if allowed_market_types_raw:
                markets = [
                    m
                    for m in markets
                    if (
                        _norm(str(m.get("sports_market_type") or "")) in allowed_market_types_raw
                        or normalize_sports_market_type(m.get("sports_market_type")) in allowed_market_types_canonical
                    )
                ]
            condition_ids = [str(m.get("condition_id") or "") for m in markets if str(m.get("condition_id") or "")]
            tokens = self._db.markets.load_tokens_for_condition_ids(condition_ids)

            token_idx: dict[tuple[str, int], str] = {}
            for t in tokens:
                key = (str(t.get("condition_id") or ""), int(t.get("outcome_index") or 0))
                token_idx[key] = str(t.get("token_id") or "")

            for m in markets:
                cid = str(m.get("condition_id") or "")
                mslug = str(m.get("slug") or "")
                market_type = self._sports_market_type_from_market(market_row=m)
                for outcome_index in (0, 1):
                    token_id = token_idx.get((cid, outcome_index), "")
                    row_tradeable = bool(token_id)
                    n_targets += 1
                    if row_tradeable:
                        game_targets_tradeable += 1
                        n_targets_tradeable += 1
                    else:
                        game_has_target_warnings = True
                    market_binding_rows.append(
                        (
                            p,
                            provider_game_id,
                            cid,
                            int(outcome_index),
                            token_id,
                            mslug,
                            market_type,
                            "exact" if row_tradeable else "unresolved",
                            "ok" if row_tradeable else "missing_token",
                            1 if row_tradeable else 0,
                            mapping.mapping_version,
                            mapping.mapping_hash,
                            None,
                            now_ts,
                        )
                    )
                    run_market_targets.append(
                        {
                            "provider_game_id": provider_game_id,
                            "condition_id": cid,
                            "outcome_index": int(outcome_index),
                            "token_id": token_id,
                            "market_slug": mslug,
                            "sports_market_type": market_type,
                            "binding_status": "exact" if row_tradeable else "unresolved",
                            "reason_code": "ok" if row_tradeable else "missing_token",
                            "is_tradeable": 1 if row_tradeable else 0,
                        }
                    )

            game_is_tradeable = bool(game_targets_tradeable > 0)
            if game_is_tradeable:
                n_games_tradeable += 1
                game_binding_status = "exact"
                game_reason_code = "ok"
            else:
                game_binding_status = "unresolved"
                game_reason_code = "no_markets_for_event_slug" if not markets else "no_tradeable_targets"
                unresolved_reason_counter[game_reason_code] += 1

            game_binding_rows.append(
                (
                    p,
                    provider_game_id,
                    resolved.canonical_league,
                    resolved.canonical_home_team,
                    resolved.canonical_away_team,
                    choice.slug_prefix,
                    game_binding_status,
                    game_reason_code,
                    1 if game_is_tradeable else 0,
                    mapping.mapping_version,
                    mapping.mapping_hash,
                    None,
                    now_ts,
                )
            )
            selected_event_slug = _norm(str(choice.event.get("slug_raw") or choice.event.get("slug") or ""))
            run_provider_games.append(
                {
                    "provider_game_id": provider_game_id,
                    "parse_status": parse_status,
                    "parse_reason": parse_reason,
                    "game_label": game_label,
                    "sport_raw": sport_raw,
                    "league_raw": league_raw,
                    "when_raw": when_raw,
                    "start_ts_utc": start_ts_utc,
                    "game_date_et": game_date_et,
                    "home_raw": home_raw,
                    "away_raw": away_raw,
                    "canonical_league": resolved.canonical_league,
                    "canonical_home_team": resolved.canonical_home_team,
                    "canonical_away_team": resolved.canonical_away_team,
                    "event_slug_prefix": choice.slug_prefix,
                    "binding_status": game_binding_status,
                    "reason_code": game_reason_code,
                    "is_tradeable": 1 if game_is_tradeable else 0,
                }
            )
            run_game_reviews.append(
                {
                    "provider_game_id": provider_game_id,
                    "resolution_state": self._resolution_state(
                        binding_status=game_binding_status,
                        reason_code=game_reason_code,
                        is_tradeable=game_is_tradeable,
                        has_target_warnings=game_has_target_warnings,
                    ),
                    "reason_code": game_reason_code,
                    "selected_event_id": event_id,
                    "selected_event_slug": selected_event_slug,
                    "used_slug_fallback": 1 if bool(selection_diag.get("used_slug_fallback")) else 0,
                    "kickoff_tolerance_minutes": int(selection_diag.get("kickoff_tolerance_minutes") or 0),
                    "kickoff_delta_sec": _int_or_none(
                        (selection_diag.get("selected_score_tuple") or [None])[0]
                        if isinstance(selection_diag.get("selected_score_tuple"), list)
                        else None
                    ),
                    "score_tuple": list(selection_diag.get("selected_score_tuple") or []),
                    "trace_json": {
                        "canonicalization": {
                            "league": resolved.canonical_league,
                            "home": resolved.canonical_home_team,
                            "away": resolved.canonical_away_team,
                            "slug_hints": list(resolved.slug_hints),
                        },
                        "selection": selection_diag,
                        "market_binding": {
                            "event_markets": len(markets),
                            "tradeable_targets": int(game_targets_tradeable),
                            "total_targets": int(len(markets) * 2),
                            "has_target_warnings": bool(game_has_target_warnings),
                        },
                    },
                }
            )

        n_games_seen -= n_league_filtered

        # Post-pass dedup: if multiple games selected the same primary event, keep best match
        event_to_games: dict[str, list[tuple[str, int | None]]] = {}
        for rec in run_game_reviews:
            eid = str(rec.get("selected_event_id") or "")
            gid = str(rec.get("provider_game_id") or "")
            delta = rec.get("kickoff_delta_sec")
            if eid and gid:
                event_to_games.setdefault(eid, []).append((gid, delta))

        duplicate_event_games: set[str] = set()
        for eid, games in event_to_games.items():
            if len(games) > 1:
                sorted_games = sorted(games, key=lambda x: abs(x[1]) if x[1] is not None else 10**9)
                for gid, _ in sorted_games[1:]:
                    duplicate_event_games.add(gid)

        if duplicate_event_games:
            game_binding_rows = [r for r in game_binding_rows if str(r[1]) not in duplicate_event_games]
            event_binding_rows = [r for r in event_binding_rows if str(r[1]) not in duplicate_event_games]
            market_binding_rows = [r for r in market_binding_rows if str(r[1]) not in duplicate_event_games]
            n_games_linked -= len(duplicate_event_games)
            for gid in duplicate_event_games:
                unresolved_reason_counter["ambiguous_doubleheader"] += 1

        return _LinkBuildBatch(
            provider=p,
            league=league_filter,
            n_games_seen=n_games_seen,
            n_games_linked=n_games_linked,
            n_games_tradeable=n_games_tradeable,
            n_targets=n_targets,
            n_targets_tradeable=n_targets_tradeable,
            game_binding_rows=game_binding_rows,
            event_binding_rows=event_binding_rows,
            market_binding_rows=market_binding_rows,
            run_provider_games=run_provider_games,
            run_game_reviews=run_game_reviews,
            run_event_candidates=run_event_candidates,
            run_market_targets=run_market_targets,
            unresolved_reason_counter=dict(unresolved_reason_counter),
            candidate_debug=candidate_debug,
        )

    def _persist_link_run(
        self,
        batches: list[_LinkBuildBatch],
        *,
        mapping: LoadedMapping,
        league_scope: str,
    ) -> LinkBuildResult:
        """Persist one or more processed batches as a single link run in one transaction."""
        scope = _norm(league_scope)
        now_ts = int(time.time())

        # Aggregate stats across all batches.
        n_games_seen = sum(b.n_games_seen for b in batches)
        n_games_linked = sum(b.n_games_linked for b in batches)
        n_games_tradeable = sum(b.n_games_tradeable for b in batches)
        n_targets = sum(b.n_targets for b in batches)
        n_targets_tradeable = sum(b.n_targets_tradeable for b in batches)
        gate_result = "pass" if n_targets_tradeable > 0 else "fail"

        # Collect unique providers and leagues.
        unique_providers = sorted({b.provider for b in batches})
        unique_leagues = sorted({b.league for b in batches if b.league})

        # Merge unresolved reason counters.
        merged_unresolved: Counter[str] = Counter()
        merged_candidate_debug: list[dict[str, Any]] = []
        for b in batches:
            merged_unresolved.update(b.unresolved_reason_counter)
            merged_candidate_debug.extend(b.candidate_debug)

        # Concatenate all binding rows.
        all_game_binding_rows: list[tuple[Any, ...]] = []
        all_event_binding_rows: list[tuple[Any, ...]] = []
        all_market_binding_rows: list[tuple[Any, ...]] = []
        for b in batches:
            all_game_binding_rows.extend(b.game_binding_rows)
            all_event_binding_rows.extend(b.event_binding_rows)
            all_market_binding_rows.extend(b.market_binding_rows)

        provider_label = ",".join(unique_providers)
        league_label = ",".join(unique_leagues)

        try:
            self._db.execute("BEGIN IMMEDIATE")

            # Clear bindings for each unique provider.
            for p in unique_providers:
                self._db.linking.clear_provider_bindings(provider=p, commit=False)

            # Insert all binding rows.
            self._db.linking.upsert_game_bindings(all_game_binding_rows, commit=False)
            self._db.linking.upsert_event_bindings(all_event_binding_rows, commit=False)
            self._db.linking.upsert_market_bindings(all_market_binding_rows, commit=False)

            # Build merged report from DB state (after bindings inserted).
            report: dict[str, Any] = {
                "parent_status_counts": {},
                "target_status_tradeable_counts": {},
                "unresolved_reason_counts": {},
            }
            for p in unique_providers:
                p_report = self._db.linking.load_link_report_rows(provider=p)
                for key in ("parent_status_counts", "target_status_tradeable_counts", "unresolved_reason_counts"):
                    sub = p_report.get(key, {})
                    if isinstance(sub, dict):
                        for k, v in sub.items():
                            report[key][k] = int(report[key].get(k, 0)) + int(v or 0)
            report["n_games_seen"] = n_games_seen
            report["unresolved_reason_counts_local"] = dict(merged_unresolved)
            if merged_candidate_debug:
                report["candidate_debug_sample"] = merged_candidate_debug[:200]

            run_id = self._db.linking.insert_link_run(
                provider=provider_label,
                league=league_label,
                league_scope=scope,
                mapping_version=mapping.mapping_version,
                mapping_hash=mapping.mapping_hash,
                n_games_seen=n_games_seen,
                n_games_linked=n_games_linked,
                n_games_tradeable=n_games_tradeable,
                n_targets=n_targets,
                n_targets_tradeable=n_targets_tradeable,
                gate_result=gate_result,
                report=report,
                run_ts=int(datetime.now().timestamp()),
                commit=False,
            )

            # Stamp run_id on binding rows for each provider.
            for p in unique_providers:
                self._db.execute("UPDATE link_game_bindings SET run_id = ? WHERE provider = ?", (int(run_id), p))
                self._db.execute("UPDATE link_event_bindings SET run_id = ? WHERE provider = ?", (int(run_id), p))
                self._db.execute("UPDATE link_market_bindings SET run_id = ? WHERE provider = ?", (int(run_id), p))

            # Build and insert run detail rows from all batches.
            run_provider_rows: list[tuple[Any, ...]] = []
            run_review_rows: list[tuple[Any, ...]] = []
            run_candidate_rows: list[tuple[Any, ...]] = []
            run_target_rows: list[tuple[Any, ...]] = []

            for batch in batches:
                bp = batch.provider
                for rec in batch.run_provider_games:
                    run_provider_rows.append(
                        (
                            int(run_id),
                            bp,
                            str(rec.get("provider_game_id") or ""),
                            str(rec.get("parse_status") or ""),
                            str(rec.get("parse_reason") or ""),
                            str(rec.get("game_label") or ""),
                            str(rec.get("sport_raw") or ""),
                            str(rec.get("league_raw") or ""),
                            str(rec.get("when_raw") or ""),
                            _int_or_none(rec.get("start_ts_utc")),
                            str(rec.get("game_date_et") or ""),
                            str(rec.get("home_raw") or ""),
                            str(rec.get("away_raw") or ""),
                            str(rec.get("canonical_league") or ""),
                            str(rec.get("canonical_home_team") or ""),
                            str(rec.get("canonical_away_team") or ""),
                            str(rec.get("event_slug_prefix") or ""),
                            str(rec.get("binding_status") or ""),
                            str(rec.get("reason_code") or ""),
                            1 if int(rec.get("is_tradeable") or 0) == 1 else 0,
                            int(now_ts),
                        )
                    )
                for rec in batch.run_game_reviews:
                    run_review_rows.append(
                        (
                            int(run_id),
                            bp,
                            str(rec.get("provider_game_id") or ""),
                            str(rec.get("resolution_state") or ""),
                            str(rec.get("reason_code") or ""),
                            str(rec.get("selected_event_id") or ""),
                            str(rec.get("selected_event_slug") or ""),
                            1 if int(rec.get("used_slug_fallback") or 0) == 1 else 0,
                            int(rec.get("kickoff_tolerance_minutes") or 0),
                            _int_or_none(rec.get("kickoff_delta_sec")),
                            json.dumps(rec.get("score_tuple") or [], separators=(",", ":"), default=str),
                            json.dumps(rec.get("trace_json") or {}, separators=(",", ":"), sort_keys=True, default=str),
                            int(now_ts),
                        )
                    )
                for rec in batch.run_event_candidates:
                    run_candidate_rows.append(
                        (
                            int(run_id),
                            bp,
                            str(rec.get("provider_game_id") or ""),
                            int(rec.get("candidate_rank") or 0),
                            str(rec.get("event_id") or ""),
                            str(rec.get("event_slug") or ""),
                            _int_or_none(rec.get("kickoff_ts_utc")),
                            1 if int(rec.get("team_set_match") or 0) == 1 else 0,
                            _int_or_none(rec.get("kickoff_within_tolerance")),
                            1 if int(rec.get("slug_hint_match") or 0) == 1 else 0,
                            int(rec.get("ordering_bonus") or 0),
                            _int_or_none(rec.get("kickoff_delta_sec")),
                            json.dumps(rec.get("score_tuple") or [], separators=(",", ":"), default=str),
                            1 if int(rec.get("is_selected") or 0) == 1 else 0,
                            str(rec.get("reject_reason") or ""),
                            int(now_ts),
                        )
                    )
                for rec in batch.run_market_targets:
                    run_target_rows.append(
                        (
                            int(run_id),
                            bp,
                            str(rec.get("provider_game_id") or ""),
                            str(rec.get("condition_id") or ""),
                            int(rec.get("outcome_index") or 0),
                            str(rec.get("token_id") or ""),
                            str(rec.get("market_slug") or ""),
                            str(rec.get("sports_market_type") or ""),
                            str(rec.get("binding_status") or ""),
                            str(rec.get("reason_code") or ""),
                            1 if int(rec.get("is_tradeable") or 0) == 1 else 0,
                            int(now_ts),
                        )
                    )

            self._db.linking.upsert_run_provider_games(run_provider_rows, commit=False)
            self._db.linking.upsert_run_game_reviews(run_review_rows, commit=False)
            self._db.linking.upsert_run_event_candidates(run_candidate_rows, commit=False)
            self._db.linking.upsert_run_market_targets(run_target_rows, commit=False)
            self._db.commit()
        except Exception:
            try:
                self._db.rollback()
            except Exception:
                pass
            raise

        return LinkBuildResult(
            provider=provider_label,
            run_id=int(run_id),
            mapping_version=mapping.mapping_version,
            mapping_hash=mapping.mapping_hash,
            n_games_seen=n_games_seen,
            n_games_linked=n_games_linked,
            n_games_tradeable=n_games_tradeable,
            n_targets=n_targets,
            n_targets_tradeable=n_targets_tradeable,
            gate_result=gate_result,
            report=report,
        )

    def build_links(
        self,
        *,
        provider: str,
        league: str | None = None,
        mapping: LoadedMapping,
        live_policy: LoadedLiveTradingPolicy | None = None,
        league_scope: str = "live",
    ) -> LinkBuildResult:
        batch = self._process_provider_league(
            provider=provider, league=league, mapping=mapping,
            live_policy=live_policy, league_scope=league_scope,
        )
        return self._persist_link_run([batch], mapping=mapping, league_scope=league_scope)

    def build_links_multi(
        self,
        *,
        league_provider_pairs: list[tuple[str, str]],
        mapping: LoadedMapping,
        live_policy: LoadedLiveTradingPolicy | None = None,
        league_scope: str = "live",
        horizon_hours: float | None = None,
    ) -> LinkBuildResult:
        """Process multiple (league, provider) pairs and persist as a single run_id."""
        policy = live_policy or load_live_trading_policy()
        batches: list[_LinkBuildBatch] = []
        for league, provider in league_provider_pairs:
            # Resolve per-league default if --horizon-hours not explicitly set.
            effective_horizon = horizon_hours
            if effective_horizon is None:
                runtime_cfg = policy.hotpath_runtime_by_league.get(league, {})
                effective_horizon = runtime_cfg.get("plan_horizon_hours")
            batch = self._process_provider_league(
                provider=provider, league=league, mapping=mapping,
                live_policy=live_policy, league_scope=league_scope,
                horizon_hours=effective_horizon,
            )
            batches.append(batch)
        return self._persist_link_run(batches, mapping=mapping, league_scope=league_scope)

