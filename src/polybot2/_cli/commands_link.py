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
        provider = str(league_cfg.get("provider", "")).strip()
        if not provider:
            logger.warning("league %s has no provider configured, skipping", league)
            continue
        pairs.append((league, provider))

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

        # For multi-provider runs, merge queues from all providers into one session.
        # The review UI takes a single provider, so we pick the first for display
        # but pre-merge the queue across all providers.
        svc = LinkReviewService(db=db)
        primary_provider = providers[0]

        if len(providers) == 1:
            if _interactive_session_available():
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

        # Multi-provider: review each provider's games sequentially
        for provider in providers:
            logger.info("reviewing provider=%s run_id=%d", provider, rid)
            if _interactive_session_available():
                code = _run_session_interactive(
                    svc=svc, provider=provider, rid=rid, scope=scope,
                    decision_filter=decision_filter, resolution_filter=resolution_filter,
                    parse_status=parse_status, limit=limit, actor=actor,
                    include_inactive=include_inactive,
                )
            else:
                code = _run_session_line_input_fallback(
                    svc=svc, provider=provider, rid=rid, scope=scope,
                    decision_filter=decision_filter, resolution_filter=resolution_filter,
                    parse_status=parse_status, limit=limit, actor=actor, logger=logger,
                    include_inactive=include_inactive,
                )
            if code != 0:
                return code
        return 0


__all__ = [
    "run_link_build",
    "run_link_review",
]
