"""Mapping/link command handlers."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from polybot2._cli.common import _color
from polybot2._cli.common import _int_or_none
from polybot2._cli.common import _render_table
from polybot2._cli.common import _resolve_provider_name
from polybot2._cli.common import _runtime_from_args
from polybot2._cli.link_review_ui import _interactive_session_available
from polybot2._cli.link_review_ui import _render_game_card_text
from polybot2._cli.link_review_ui import _run_session_interactive
from polybot2._cli.link_review_ui import _run_session_line_input_fallback
from polybot2.data import open_database
from polybot2.linking import LinkReviewService
from polybot2.linking import LinkService
from polybot2.linking import load_live_trading_policy
from polybot2.linking import load_mapping

def run_mapping_validate(args: Any, *, logger: logging.Logger) -> int:
    del args
    loaded = load_mapping()
    policy = load_live_trading_policy()
    logger.info(
        "mapping validation ok: mapping_path=%s mapping_version=%s mapping_hash=%s live_policy_path=%s live_policy_version=%s live_leagues=%d live_market_types=%d",
        loaded.path,
        loaded.mapping_version,
        loaded.mapping_hash,
        policy.path,
        policy.policy_version,
        len(policy.live_betting_leagues),
        len(policy.live_betting_market_types),
    )
    return 0


def run_link_build(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)
    live_policy = load_live_trading_policy()
    provider = _resolve_provider_name(args=args, logger=logger, context="link build", live_policy=live_policy)
    if provider is None:
        return 1
    mapping = load_mapping()
    league_scope = str(getattr(args, "league_scope", "live")).strip().lower()
    with open_database(runtime) as db:
        svc = LinkService(db=db)
        result = svc.build_links(provider=provider, mapping=mapping, live_policy=live_policy, league_scope=league_scope)
    logger.info(
        "link build complete: provider=%s run_id=%d mapping=%s hash=%s games_seen=%d linked=%d tradeable=%d targets=%d target_tradeable=%d gate=%s",
        result.provider,
        int(result.run_id),
        result.mapping_version,
        result.mapping_hash,
        int(result.n_games_seen),
        int(result.n_games_linked),
        int(result.n_games_tradeable),
        int(result.n_targets),
        int(result.n_targets_tradeable),
        result.gate_result,
    )
    return 0


def run_link_report(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)
    provider = _resolve_provider_name(args=args, logger=logger, context="link report")
    if provider is None:
        return 1
    with open_database(runtime) as db:
        svc = LinkService(db=db)
        report = svc.report(provider=provider)
    logger.info("link report (%s): %s", provider, json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0


def run_link_review(args: Any, *, logger: logging.Logger) -> int:
    runtime = _runtime_from_args(args)
    provider = _resolve_provider_name(args=args, logger=logger, context="link review")
    if provider is None:
        return 1
    command = str(getattr(args, "link_review_command", "")).strip().lower()
    fmt = str(getattr(args, "format", "table")).strip().lower()

    with open_database(runtime) as db:
        svc = LinkReviewService(db=db)

        if command == "card":
            rid = _int_or_none(getattr(args, "run_id", None))
            if rid is None:
                logger.error("--run-id is required for link review card")
                return 1
            provider_game_id = str(getattr(args, "provider_game_id", "")).strip()
            payload = svc.get_game_card(provider=provider, run_id=rid, provider_game_id=provider_game_id)
            if fmt == "json":
                logger.info("%s", json.dumps(payload, indent=2, sort_keys=True, default=str))
            else:
                logger.info("%s", _render_game_card_text(payload))
            return 0

        if command == "candidates":
            rid = _int_or_none(getattr(args, "run_id", None))
            if rid is None:
                logger.error("--run-id is required for link review candidates")
                return 1
            provider_game_id = str(getattr(args, "provider_game_id", "")).strip()
            rows = svc.get_candidate_comparison(provider=provider, run_id=rid, provider_game_id=provider_game_id)
            payload = {"provider": provider, "run_id": rid, "provider_game_id": provider_game_id, "rows": rows}
            if fmt == "json":
                logger.info("%s", json.dumps(payload, indent=2, sort_keys=True, default=str))
            else:
                display_rows = []
                for row in rows:
                    display_rows.append(
                        {
                            **row,
                            "is_selected": _color("yes", "32") if int(row.get("is_selected") or 0) == 1 else "",
                        }
                    )
                logger.info(
                    "Candidate Comparison (provider=%s run_id=%s provider_game_id=%s)\n%s",
                    provider,
                    rid,
                    provider_game_id,
                    _render_table(
                        rows=display_rows,
                        columns=[
                            ("candidate_rank", "rank"),
                            ("event_id", "event_id"),
                            ("event_slug", "event_slug"),
                            ("team_set_match", "team_set"),
                            ("kickoff_within_tolerance", "kickoff_ok"),
                            ("slug_hint_match", "slug_hint"),
                            ("ordering_bonus", "order_bonus"),
                            ("kickoff_delta_sec", "kickoff_delta_sec"),
                            ("score_tuple", "score_tuple"),
                            ("is_selected", "selected"),
                            ("reject_reason", "reject_reason"),
                        ],
                    ),
                )
            return 0

        if command == "decide":
            rid = _int_or_none(getattr(args, "run_id", None))
            provider_game_id = str(getattr(args, "provider_game_id", "")).strip()
            decision = str(getattr(args, "decision", "")).strip().lower()
            note = str(getattr(args, "note", "")).strip()
            actor = str(getattr(args, "actor", "cli")).strip() or "cli"
            if rid is None:
                logger.error("--run-id is required for link review decide")
                return 1
            try:
                payload = svc.record_decision(
                    provider=provider,
                    run_id=rid,
                    provider_game_id=provider_game_id,
                    decision=decision,
                    note=note,
                    actor=actor,
                )
            except ValueError as exc:
                logger.error("decision failed: %s", str(exc))
                return 1
            if fmt == "json":
                logger.info("%s", json.dumps(payload, indent=2, sort_keys=True, default=str))
            else:
                progress = payload.get("progress") if isinstance(payload.get("progress"), dict) else {}
                logger.info(
                    "Decision recorded: provider=%s run_id=%s provider_game_id=%s decision=%s actor=%s note=%s\nprogress: approved=%s rejected=%s skipped=%s pending=%s total=%s all_approved=%s",
                    provider,
                    rid,
                    provider_game_id,
                    decision,
                    actor,
                    note,
                    progress.get("n_approved"),
                    progress.get("n_rejected"),
                    progress.get("n_skipped"),
                    progress.get("n_pending"),
                    progress.get("total_in_scope"),
                    progress.get("all_approved"),
                )
            return 0

        if command == "session":
            rid = _int_or_none(getattr(args, "run_id", None))
            if rid is None:
                logger.error("--run-id is required for link review session")
                return 1
            scope = str(getattr(args, "scope", "mapped_pending")).strip().lower() or "mapped_pending"
            decision_filter = str(getattr(args, "decision", "")).strip().lower()
            resolution_filter = str(getattr(args, "resolution", "")).strip().upper()
            parse_status = str(getattr(args, "parse_status", "ok")).strip().lower()
            limit = int(getattr(args, "limit", 500) or 500)
            include_inactive = bool(getattr(args, "include_inactive", False))
            actor = str(os.getenv("USER") or "cli-session").strip() or "cli-session"
            if _interactive_session_available():
                return _run_session_interactive(
                    svc=svc,
                    provider=provider,
                    rid=rid,
                    scope=scope,
                    decision_filter=decision_filter,
                    resolution_filter=resolution_filter,
                    parse_status=parse_status,
                    limit=limit,
                    actor=actor,
                    include_inactive=include_inactive,
                )
            logger.info("Interactive session unavailable (requires TTY + rich). Falling back to line-input mode.")
            return _run_session_line_input_fallback(
                svc=svc,
                provider=provider,
                rid=rid,
                scope=scope,
                decision_filter=decision_filter,
                resolution_filter=resolution_filter,
                parse_status=parse_status,
                limit=limit,
                actor=actor,
                logger=logger,
                include_inactive=include_inactive,
            )

    logger.error("Unsupported link review command")
    return 1


__all__ = [
    "run_mapping_validate",
    "run_link_build",
    "run_link_report",
    "run_link_review",
]
