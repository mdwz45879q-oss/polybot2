"""Link command handlers."""

from __future__ import annotations

import logging
import os
from typing import Any

from polybot2._cli.common import _int_or_none
from polybot2._cli.common import _runtime_from_args
from polybot2._cli.link_review_ui import _interactive_session_available
from polybot2._cli.link_review_ui import _run_session_interactive
from polybot2._cli.link_review_ui import _run_session_line_input_fallback
from polybot2.data import open_database
from polybot2.linking import LinkReviewService
from polybot2.linking import LinkService
from polybot2.linking import load_live_trading_policy
from polybot2.linking import load_mapping


def run_link_build(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)
    live_policy = load_live_trading_policy()
    mapping = load_mapping()
    league_scope = str(getattr(args, "league_scope", "live")).strip().lower()
    horizon_hours = getattr(args, "horizon_hours", None)

    live_leagues = sorted(live_policy.live_betting_leagues) if league_scope == "live" else sorted(mapping.leagues.keys())

    pairs: list[tuple[str, str]] = []
    for league in live_leagues:
        league_cfg = mapping.leagues.get(league, {})
        _raw_p = league_cfg.get("provider", "")
        providers = list(_raw_p) if isinstance(_raw_p, list) else [_raw_p] if _raw_p else []
        if not providers:
            logger.warning("league %s has no provider configured, skipping", league)
            continue
        for p in providers:
            p = str(p).strip()
            if p:
                pairs.append((league, p))

    with open_database(runtime) as db:
        svc = LinkService(db=db)
        result = svc.build_links_multi(
            league_provider_pairs=pairs,
            mapping=mapping,
            live_policy=live_policy,
            league_scope=league_scope,
            horizon_hours=horizon_hours,
        )
        logger.info(
            "link build complete: run_id=%d games_seen=%d linked=%d tradeable=%d targets=%d gate=%s",
            int(result.run_id), int(result.n_games_seen), int(result.n_games_linked),
            int(result.n_games_tradeable), int(result.n_targets), result.gate_result,
        )
    return 0


def run_link_review(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)

    rid = _int_or_none(getattr(args, "run_id", None))
    if rid is None:
        logger.error("--run-id is required")
        return 1
    scope = str(getattr(args, "scope", "mapped_pending")).strip().lower() or "mapped_pending"
    decision_filter = str(getattr(args, "decision", "")).strip().lower()
    resolution_filter = str(getattr(args, "resolution", "")).strip().upper()
    parse_status = str(getattr(args, "parse_status", "ok")).strip().lower()
    limit = int(getattr(args, "limit", 500) or 500)
    include_inactive = bool(getattr(args, "include_inactive", False))
    actor = str(os.getenv("USER") or "cli-session").strip() or "cli-session"

    with open_database(runtime) as db:
        # Look up providers from the run record
        run_row = db.execute(
            "SELECT provider FROM link_runs WHERE run_id = ?", (int(rid),)
        ).fetchone()
        if run_row is None:
            logger.error("run_id=%d not found", rid)
            return 1
        providers_str = str(run_row["provider"])
        providers = [p.strip() for p in providers_str.split(",") if p.strip()]
        if not providers:
            logger.error("run_id=%d has no providers", rid)
            return 1

        svc = LinkReviewService(db=db)
        propagate = len(providers) > 1

        # Merge queues from all providers, deduplicate by canonical game.
        # For each canonical game, prefer the row from the league's primary
        # provider (first in the config list). Sort by league for grouping.
        merged_rows: list[dict[str, Any]] = []
        if propagate:
            from polybot2.linking import load_mapping as _load_mapping_review
            _mapping = _load_mapping_review()
            primary_by_league: dict[str, str] = {}
            for lk, lcfg in _mapping.leagues.items():
                raw_p = lcfg.get("provider", "")
                if isinstance(raw_p, list) and raw_p:
                    primary_by_league[lk] = str(raw_p[0]).strip().lower()
                elif raw_p:
                    primary_by_league[lk] = str(raw_p).strip().lower()

            canonical_best: dict[tuple[str, str, str], dict[str, Any]] = {}
            for p in providers:
                rows = svc.get_queue(
                    provider=p, run_id=rid, scope=scope,
                    decision_filter=decision_filter, resolution_filter=resolution_filter,
                    parse_status=parse_status, limit=limit,
                    include_inactive=bool(include_inactive),
                )
                for row in rows:
                    key = (
                        str(row.get("canonical_home_team") or ""),
                        str(row.get("canonical_away_team") or ""),
                        str(row.get("game_date_et") or ""),
                    )
                    league = str(row.get("canonical_league") or "")
                    is_primary = (p == primary_by_league.get(league, ""))
                    if key not in canonical_best or is_primary:
                        canonical_best[key] = row
            merged_rows = list(canonical_best.values())
            merged_rows.sort(key=lambda r: (str(r.get("canonical_league") or ""), str(r.get("game_date_et") or "")))

        primary_provider = providers[0]
        interactive = _interactive_session_available()

        if not propagate:
            if interactive:
                return _run_session_interactive(
                    svc=svc, provider=primary_provider, rid=rid, scope=scope,
                    decision_filter=decision_filter, resolution_filter=resolution_filter,
                    parse_status=parse_status, limit=limit, actor=actor,
                    include_inactive=include_inactive,
                )
            return _run_session_line_input_fallback(
                svc=svc, provider=primary_provider, rid=rid, scope=scope,
                decision_filter=decision_filter, resolution_filter=resolution_filter,
                parse_status=parse_status, limit=limit, actor=actor, logger=logger,
                include_inactive=include_inactive,
            )

        # Group by league — each league gets its own review session
        league_groups: dict[str, list[dict[str, Any]]] = {}
        for row in merged_rows:
            lg = str(row.get("canonical_league") or "")
            league_groups.setdefault(lg, []).append(row)

        for league_key in sorted(league_groups):
            league_rows = league_groups[league_key]
            league_provider = primary_by_league.get(league_key, primary_provider)
            if interactive:
                rc = _run_session_interactive(
                    svc=svc, provider=league_provider, rid=rid, scope=scope,
                    decision_filter=decision_filter, resolution_filter=resolution_filter,
                    parse_status=parse_status, limit=limit, actor=actor,
                    include_inactive=include_inactive,
                    propagate_to_siblings=propagate,
                    preloaded_rows=league_rows,
                )
            else:
                rc = _run_session_line_input_fallback(
                    svc=svc, provider=league_provider, rid=rid, scope=scope,
                    decision_filter=decision_filter, resolution_filter=resolution_filter,
                    parse_status=parse_status, limit=limit, actor=actor, logger=logger,
                    include_inactive=include_inactive,
                    propagate_to_siblings=propagate,
                    preloaded_rows=league_rows,
                )
            if rc != 0:
                return rc
        return 0


__all__ = [
    "run_link_build",
    "run_link_review",
]
