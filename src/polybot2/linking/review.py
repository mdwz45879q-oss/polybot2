"""Review and approval workflows for deterministic link runs."""

from __future__ import annotations

import json
import time
from typing import Any

from polybot2.linking.actionable import actionable_game_ids
from polybot2.linking.mapping_loader import LoadedLiveTradingPolicy, load_live_trading_policy
from polybot2.market_types import normalize_sports_market_type


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


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_json_obj(value: Any) -> dict[str, Any]:
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _parse_json_list(value: Any) -> list[Any]:
    text = str(value or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


def _kickoff_delta_min(*, provider_start_ts_utc: Any, event_kickoff_ts_utc: Any) -> int | None:
    p = _int_or_none(provider_start_ts_utc)
    e = _int_or_none(event_kickoff_ts_utc)
    if p is None or e is None:
        return None
    return int(round((int(e) - int(p)) / 60.0))


_MARKET_TYPE_ORDER: dict[str, int] = {
    "moneyline": 0,
    "totals": 1,
    "nrfi": 2,
    "spread": 3,
}


def _format_line_value(value: Any) -> str:
    parsed = _float_or_none(value)
    if parsed is None:
        return ""
    rounded = round(parsed)
    if abs(parsed - rounded) < 1e-9:
        return str(int(rounded))
    return f"{parsed:.4f}".rstrip("0").rstrip(".")


def _normalize_market_type_info(
    *,
    sports_market_type: Any,
    market_question: Any,
    market_slug: Any,
) -> tuple[str, str, bool]:
    raw_type = _norm(str(sports_market_type or ""))
    normalized_type = normalize_sports_market_type(raw_type)
    if normalized_type != "other":
        return normalized_type, normalized_type.upper(), False
    if raw_type:
        return "other", "OTHER", False

    question = _norm(str(market_question or ""))
    slug = _norm(str(market_slug or ""))
    combined = " ".join(x for x in [question, slug] if x)

    inferred = "other"
    if "nfri" in combined or "nrfi" in combined or ("first inning" in combined and "run" in combined):
        inferred = "nrfi"
    elif "moneyline" in combined:
        inferred = "moneyline"
    elif "total" in combined or ("over" in combined and "under" in combined):
        inferred = "totals"
    elif "spread" in combined or "run line" in combined:
        inferred = "spread"

    return inferred, f"{inferred.upper()} (inf)", True


def _line_display_for_market(*, market_type_key: str, line: Any) -> str:
    line_text = _format_line_value(line)
    if not line_text:
        return ""
    key = _norm(market_type_key)
    if key == "totals":
        return f"O/U {line_text}"
    if key == "spread":
        return f"line {line_text}"
    return line_text


def _market_sort_key(market: dict[str, Any]) -> tuple[Any, ...]:
    key = _norm(str(market.get("market_type_key") or ""))
    order = _MARKET_TYPE_ORDER.get(key, 99)
    line_sort = _float_or_none(market.get("line"))
    if key not in {"totals", "spread"} or line_sort is None:
        line_sort = float("inf")
    question = _norm(str(market.get("market_question") or ""))
    condition = str(market.get("condition_id") or "")
    return (order, line_sort, question, condition)


class LinkReviewService:
    """Read/write review surface on top of immutable link-run snapshots."""

    VALID_DECISIONS = {"approve", "reject", "skip"}
    VALID_QUEUE_SCOPES = {"all", "mapped_pending", "mapped", "unresolved"}

    def __init__(self, *, db: Any):
        self._db = db

    def _resolve_run(self, *, provider: str, run_id: int | None = None) -> dict[str, Any] | None:
        p = _norm(provider)
        rid = _int_or_none(run_id)
        return self._db.linking.load_link_run(provider=p, run_id=rid)

    def _latest_decisions(self, *, provider: str, run_id: int) -> list[dict[str, Any]]:
        p = _norm(provider)
        rows = self._db.execute(
            """
            SELECT d.*
            FROM link_review_decisions d
            INNER JOIN (
                SELECT run_id, provider, provider_game_id, MAX(decision_id) AS max_decision_id
                FROM link_review_decisions
                WHERE run_id = ? AND provider = ?
                GROUP BY run_id, provider, provider_game_id
            ) latest
              ON latest.max_decision_id = d.decision_id
            ORDER BY d.provider_game_id
            """,
            (int(run_id), p),
        ).fetchall()
        return [dict(r) for r in rows]

    def _actionable_game_ids(
        self,
        *,
        provider: str,
        run_id: int,
        live_policy: LoadedLiveTradingPolicy | None = None,
        now_ts_utc: int | None = None,
    ) -> set[str]:
        p = _norm(provider)
        rid = int(run_id)
        policy = live_policy or load_live_trading_policy()
        effective_now_ts = now_ts_utc
        if effective_now_ts is None:
            run_row = self._resolve_run(provider=p, run_id=rid)
            if run_row is not None:
                effective_now_ts = _int_or_none(run_row.get("run_ts"))
        runtime_cfgs = list((policy.hotpath_runtime_by_league or {}).values())
        max_age_seconds = 600
        if runtime_cfgs:
            max_age_seconds = max(
                int(dict(cfg or {}).get("provider_catalog_max_age_seconds", 600))
                for cfg in runtime_cfgs
            )
        return actionable_game_ids(
            db=self._db,
            provider=p,
            run_id=rid,
            max_age_seconds=max_age_seconds,
            now_ts_utc=effective_now_ts,
            league=None,
            require_open_targets=False,
        )

    def get_decision_progress(
        self,
        *,
        provider: str,
        run_id: int,
        include_inactive: bool = False,
        live_policy: LoadedLiveTradingPolicy | None = None,
        now_ts_utc: int | None = None,
    ) -> dict[str, Any]:
        p = _norm(provider)
        rid = int(run_id)
        actionable_ids = (
            set()
            if bool(include_inactive)
            else self._actionable_game_ids(provider=p, run_id=rid, live_policy=live_policy, now_ts_utc=now_ts_utc)
        )
        if not bool(include_inactive) and not actionable_ids:
            total_in_scope = 0
        elif bool(include_inactive):
            total_row = self._db.execute(
                """
                SELECT COUNT(*) AS n
                FROM link_run_provider_games
                WHERE run_id = ? AND provider = ? AND parse_status = 'ok'
                """,
                (rid, p),
            ).fetchone()
            total_in_scope = int((dict(total_row or {}).get("n")) or 0)
        else:
            placeholders = ",".join("?" for _ in actionable_ids)
            total_row = self._db.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM link_run_provider_games
                WHERE run_id = ? AND provider = ? AND parse_status = 'ok'
                  AND provider_game_id IN ({placeholders})
                """,
                (rid, p, *sorted(actionable_ids)),
            ).fetchone()
            total_in_scope = int((dict(total_row or {}).get("n")) or 0)
        latest_rows = self._latest_decisions(provider=p, run_id=rid)
        if not bool(include_inactive):
            latest_rows = [r for r in latest_rows if str(r.get("provider_game_id") or "") in actionable_ids]
        by_game = {str(r.get("provider_game_id") or ""): str(r.get("decision") or "") for r in latest_rows}
        approved = 0
        rejected = 0
        skipped = 0
        for decision in by_game.values():
            if decision == "approve":
                approved += 1
            elif decision == "reject":
                rejected += 1
            elif decision == "skip":
                skipped += 1
        pending = max(0, total_in_scope - len(by_game))
        return {
            "provider": p,
            "run_id": rid,
            "total_in_scope": total_in_scope,
            "n_approved": approved,
            "n_rejected": rejected,
            "n_skipped": skipped,
            "n_pending": pending,
            "all_reviewed": bool(pending == 0),
            "all_approved": bool(total_in_scope >= 0 and approved == total_in_scope and rejected == 0 and skipped == 0 and pending == 0),
            "latest_decisions": latest_rows,
        }

    def get_run_status(
        self,
        *,
        provider: str,
        run_id: int | None = None,
        include_inactive: bool = False,
        live_policy: LoadedLiveTradingPolicy | None = None,
        now_ts_utc: int | None = None,
    ) -> dict[str, Any]:
        p = _norm(provider)
        requested_run_id = _int_or_none(run_id)
        run_row = self._resolve_run(provider=p, run_id=requested_run_id)
        if run_row is None:
            return {
                "provider": p,
                "requested_run_id": requested_run_id,
                "run_found": False,
                "run_id": None,
                "gate_result": "",
                "mapping_version": "",
                "mapping_hash": "",
                "report": {},
                "decision_progress": {
                    "provider": p,
                    "run_id": None,
                    "total_in_scope": 0,
                    "n_approved": 0,
                    "n_rejected": 0,
                    "n_skipped": 0,
                    "n_pending": 0,
                    "all_reviewed": False,
                    "all_approved": False,
                    "latest_decisions": [],
                },
                "warning_required": True,
                "warning_codes": ["no_link_run"],
            }
        rid = int(run_row["run_id"])
        progress = self.get_decision_progress(
            provider=p,
            run_id=rid,
            include_inactive=include_inactive,
            live_policy=live_policy,
            now_ts_utc=now_ts_utc,
        )
        if bool(include_inactive):
            unresolved_row = self._db.execute(
                """
                SELECT COUNT(*) AS n
                FROM link_run_game_reviews
                WHERE run_id = ? AND provider = ? AND resolution_state NOT IN ('MATCHED_CLEAN', 'MATCHED_WITH_WARNINGS')
                """,
                (rid, p),
            ).fetchone()
            n_unresolved = int((dict(unresolved_row or {}).get("n")) or 0)
        else:
            actionable_ids = self._actionable_game_ids(provider=p, run_id=rid, live_policy=live_policy, now_ts_utc=now_ts_utc)
            if not actionable_ids:
                n_unresolved = 0
            else:
                placeholders = ",".join("?" for _ in actionable_ids)
                unresolved_row = self._db.execute(
                    f"""
                    SELECT COUNT(*) AS n
                    FROM link_run_game_reviews
                    WHERE run_id = ? AND provider = ? AND resolution_state NOT IN ('MATCHED_CLEAN', 'MATCHED_WITH_WARNINGS')
                      AND provider_game_id IN ({placeholders})
                    """,
                    (rid, p, *sorted(actionable_ids)),
                ).fetchone()
                n_unresolved = int((dict(unresolved_row or {}).get("n")) or 0)
        report = _parse_json_obj(run_row.get("report_json"))
        gate_result = str(run_row.get("gate_result") or "")
        warning_codes: list[str] = []
        if gate_result != "pass":
            warning_codes.append("gate_not_pass")
        if n_unresolved > 0:
            warning_codes.append("has_unresolved_games")
        if int(progress.get("n_pending") or 0) > 0:
            warning_codes.append("pending_reviews")
        if int(progress.get("n_rejected") or 0) > 0:
            warning_codes.append("has_rejected")
        if int(progress.get("n_skipped") or 0) > 0:
            warning_codes.append("has_skipped")
        return {
            "provider": p,
            "requested_run_id": requested_run_id,
            "run_found": True,
            "run_id": rid,
            "run_ts": _int_or_none(run_row.get("run_ts")),
            "league_scope": str(run_row.get("league_scope") or ""),
            "mapping_version": str(run_row.get("mapping_version") or ""),
            "mapping_hash": str(run_row.get("mapping_hash") or ""),
            "gate_result": gate_result,
            "n_games_seen": int(run_row.get("n_games_seen") or 0),
            "n_games_linked": int(run_row.get("n_games_linked") or 0),
            "n_games_tradeable": int(run_row.get("n_games_tradeable") or 0),
            "n_targets": int(run_row.get("n_targets") or 0),
            "n_targets_tradeable": int(run_row.get("n_targets_tradeable") or 0),
            "n_unresolved_games": n_unresolved,
            "report": report,
            "decision_progress": progress,
            "warning_required": bool(warning_codes),
            "warning_codes": warning_codes,
        }

    def get_queue(
        self,
        *,
        provider: str,
        run_id: int,
        scope: str | None = "all",
        decision_filter: str | None = None,
        resolution_filter: str | None = None,
        parse_status: str = "ok",
        limit: int = 500,
        include_inactive: bool = False,
        live_policy: LoadedLiveTradingPolicy | None = None,
        now_ts_utc: int | None = None,
    ) -> list[dict[str, Any]]:
        p = _norm(provider)
        rid = int(run_id)
        scope_norm = _norm(scope or "all")
        if scope_norm not in self.VALID_QUEUE_SCOPES:
            scope_norm = "all"
        decision_filter_norm = _norm(decision_filter or "")
        resolution_filter_norm = str(resolution_filter or "").strip().upper()
        parse_status_norm = _norm(parse_status or "ok")
        lim = max(1, int(limit or 500))
        if not bool(include_inactive):
            actionable_ids = self._actionable_game_ids(provider=p, run_id=rid, live_policy=live_policy, now_ts_utc=now_ts_utc)
            if not actionable_ids:
                return []
            actionable_placeholders = ",".join("?" for _ in actionable_ids)
            actionable_clause = f" AND pg.provider_game_id IN ({actionable_placeholders})"
            actionable_params: tuple[Any, ...] = tuple(sorted(actionable_ids))
        else:
            actionable_clause = ""
            actionable_params = tuple()
        rows = self._db.execute(
            f"""
            WITH latest_decisions AS (
                SELECT d.*
                FROM link_review_decisions d
                INNER JOIN (
                    SELECT run_id, provider, provider_game_id, MAX(decision_id) AS max_decision_id
                    FROM link_review_decisions
                    WHERE run_id = ? AND provider = ?
                    GROUP BY run_id, provider, provider_game_id
                ) x
                  ON x.max_decision_id = d.decision_id
            )
            SELECT
                pg.run_id,
                pg.provider,
                pg.provider_game_id,
                pg.game_date_et,
                pg.start_ts_utc,
                pg.home_raw,
                pg.away_raw,
                pg.canonical_league,
                pg.canonical_home_team,
                pg.canonical_away_team,
                pg.parse_status,
                pg.binding_status,
                pg.reason_code,
                pg.is_tradeable,
                gr.resolution_state,
                gr.selected_event_id,
                COALESCE(ld.decision, '') AS decision,
                ld.note AS decision_note,
                ld.actor AS decision_actor,
                ld.decided_at AS decision_ts
            FROM link_run_provider_games pg
            LEFT JOIN link_run_game_reviews gr
              ON gr.run_id = pg.run_id
             AND gr.provider = pg.provider
             AND gr.provider_game_id = pg.provider_game_id
            LEFT JOIN latest_decisions ld
              ON ld.run_id = pg.run_id
             AND ld.provider = pg.provider
             AND ld.provider_game_id = pg.provider_game_id
            WHERE pg.run_id = ?
              AND pg.provider = ?
              {actionable_clause}
              AND (? = '' OR pg.parse_status = ?)
              AND (
                ? = 'all'
                OR (
                    ? = 'mapped'
                    AND COALESCE(gr.resolution_state, '') IN ('MATCHED_CLEAN', 'MATCHED_WITH_WARNINGS')
                )
                OR (
                    ? = 'mapped_pending'
                    AND COALESCE(gr.resolution_state, '') IN ('MATCHED_CLEAN', 'MATCHED_WITH_WARNINGS')
                    AND COALESCE(ld.decision, '') = ''
                )
                OR (
                    ? = 'unresolved'
                    AND COALESCE(gr.resolution_state, '') NOT IN ('MATCHED_CLEAN', 'MATCHED_WITH_WARNINGS')
                )
              )
              AND (? = '' OR COALESCE(ld.decision, '') = ?)
              AND (? = '' OR COALESCE(gr.resolution_state, '') = ?)
            ORDER BY
              CASE WHEN ld.decision IS NULL THEN 0 ELSE 1 END ASC,
              COALESCE(pg.game_date_et, '') ASC,
              pg.provider_game_id ASC
            LIMIT ?
            """,
            (
                rid,
                p,
                rid,
                p,
                *actionable_params,
                parse_status_norm,
                parse_status_norm,
                scope_norm,
                scope_norm,
                scope_norm,
                scope_norm,
                decision_filter_norm,
                decision_filter_norm,
                resolution_filter_norm,
                resolution_filter_norm,
                lim,
            ),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_candidate_comparison(self, *, provider: str, run_id: int, provider_game_id: str) -> list[dict[str, Any]]:
        p = _norm(provider)
        rid = int(run_id)
        gid = str(provider_game_id or "").strip()
        rows = self._db.execute(
            """
            SELECT
                candidate_rank,
                event_id,
                event_slug,
                kickoff_ts_utc,
                team_set_match,
                kickoff_within_tolerance,
                slug_hint_match,
                ordering_bonus,
                kickoff_delta_sec,
                score_tuple,
                is_selected,
                reject_reason
            FROM link_run_event_candidates
            WHERE run_id = ? AND provider = ? AND provider_game_id = ?
            ORDER BY candidate_rank ASC, event_id ASC
            """,
            (rid, p, gid),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            d["score_tuple"] = _parse_json_list(d.get("score_tuple"))
            out.append(d)
        return out

    def get_game_card(self, *, provider: str, run_id: int, provider_game_id: str) -> dict[str, Any]:
        p = _norm(provider)
        rid = int(run_id)
        gid = str(provider_game_id or "").strip()
        row = self._db.execute(
            """
            WITH latest_decision AS (
                SELECT d.*
                FROM link_review_decisions d
                WHERE d.run_id = ? AND d.provider = ? AND d.provider_game_id = ?
                ORDER BY d.decision_id DESC
                LIMIT 1
            )
            SELECT
                pg.*,
                gr.resolution_state,
                gr.selected_event_id,
                gr.selected_event_slug,
                gr.used_slug_fallback,
                gr.kickoff_tolerance_minutes,
                gr.kickoff_delta_sec,
                gr.score_tuple,
                gr.trace_json,
                pe.title AS selected_event_title,
                pe.kickoff_ts_utc AS selected_event_kickoff_ts_utc,
                pe.status AS selected_event_status,
                ld.decision AS latest_decision,
                ld.note AS latest_decision_note,
                ld.actor AS latest_decision_actor,
                ld.decided_at AS latest_decision_ts
            FROM link_run_provider_games pg
            LEFT JOIN link_run_game_reviews gr
              ON gr.run_id = pg.run_id
             AND gr.provider = pg.provider
             AND gr.provider_game_id = pg.provider_game_id
            LEFT JOIN pm_events pe
              ON pe.event_id = gr.selected_event_id
            LEFT JOIN latest_decision ld
              ON 1 = 1
            WHERE pg.run_id = ? AND pg.provider = ? AND pg.provider_game_id = ?
            LIMIT 1
            """,
            (rid, p, gid, rid, p, gid),
        ).fetchone()
        if row is None:
            return {
                "provider": p,
                "run_id": rid,
                "provider_game_id": gid,
                "found": False,
                "card": None,
            }

        game = dict(row)
        game["score_tuple"] = _parse_json_list(game.get("score_tuple"))
        selected_event_rows = self._db.execute(
            """
            SELECT
                c.candidate_rank,
                c.event_id,
                c.event_slug,
                c.kickoff_ts_utc,
                c.score_tuple,
                pe.title AS event_title,
                pe.status AS event_status
            FROM link_run_event_candidates c
            LEFT JOIN pm_events pe
              ON pe.event_id = c.event_id
            WHERE c.run_id = ? AND c.provider = ? AND c.provider_game_id = ? AND c.is_selected = 1
            ORDER BY c.candidate_rank ASC, c.event_id ASC
            """,
            (rid, p, gid),
        ).fetchall()
        selected_events = []
        primary_event_id = str(game.get("selected_event_id") or "")
        for row_selected in selected_event_rows:
            selected_events.append(
                {
                    "candidate_rank": int(row_selected["candidate_rank"] or 0),
                    "event_id": str(row_selected["event_id"] or ""),
                    "event_slug": str(row_selected["event_slug"] or ""),
                    "event_title": str(row_selected["event_title"] or ""),
                    "status": str(row_selected["event_status"] or ""),
                    "kickoff_ts_utc": _int_or_none(row_selected["kickoff_ts_utc"]),
                    "score_tuple": _parse_json_list(row_selected["score_tuple"]),
                    "is_primary": bool(str(row_selected["event_id"] or "") == primary_event_id),
                }
            )
        targets = [
            dict(r)
            for r in self._db.execute(
                """
                SELECT
                    t.condition_id,
                    t.outcome_index,
                    t.token_id,
                    t.market_slug,
                    t.sports_market_type,
                    t.binding_status,
                    t.reason_code,
                    t.is_tradeable,
                    m.event_id,
                    m.question AS market_question,
                    m.line,
                    tok.outcome_label
                FROM link_run_market_targets t
                LEFT JOIN pm_markets m
                  ON m.condition_id = t.condition_id
                LEFT JOIN pm_market_tokens tok
                  ON tok.condition_id = t.condition_id
                 AND tok.outcome_index = t.outcome_index
                WHERE t.run_id = ? AND t.provider = ? AND t.provider_game_id = ?
                ORDER BY t.sports_market_type ASC, t.condition_id ASC, t.outcome_index ASC
                """,
                (rid, p, gid),
            ).fetchall()
        ]

        selected_event_ids = sorted(
            {
                str(e.get("event_id") or "")
                for e in selected_events
                if str(e.get("event_id") or "")
            }
            | ({str(game.get("selected_event_id") or "")} if str(game.get("selected_event_id") or "") else set())
        )
        trace = _parse_json_obj(game.get("trace_json"))
        reason_notes = sorted({str(t.get("reason_code") or "") for t in targets if str(t.get("reason_code") or "") and str(t.get("reason_code") or "") != "ok"})

        event_ids = sorted(
            {
                str(e.get("event_id") or "")
                for e in selected_events
                if str(e.get("event_id") or "")
            }
            | {
                str(t.get("event_id") or "")
                for t in targets
                if str(t.get("event_id") or "")
            }
        )
        event_team_rows = []
        if event_ids:
            placeholders = ",".join("?" for _ in event_ids)
            event_team_rows = [
                dict(r)
                for r in self._db.execute(
                    f"""
                    SELECT event_id, team_index, name
                    FROM pm_event_teams
                    WHERE event_id IN ({placeholders})
                    ORDER BY event_id, team_index
                    """,
                    tuple(event_ids),
                ).fetchall()
            ]
        team_names_by_event: dict[str, list[str]] = {}
        for tr in event_team_rows:
            eid = str(tr.get("event_id") or "")
            name = str(tr.get("name") or "").strip()
            if not eid or not name:
                continue
            team_names_by_event.setdefault(eid, []).append(name)

        markets_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for t in targets:
            condition_id = str(t.get("condition_id") or "")
            event_id = str(t.get("event_id") or "")
            if not condition_id:
                continue
            key = (event_id, condition_id)
            mk = markets_by_key.get(key)
            if mk is None:
                market_type_key, display_market_type, market_type_inferred = _normalize_market_type_info(
                    sports_market_type=t.get("sports_market_type"),
                    market_question=t.get("market_question"),
                    market_slug=t.get("market_slug"),
                )
                mk = {
                    "event_id": event_id,
                    "condition_id": condition_id,
                    "market_slug": str(t.get("market_slug") or ""),
                    "market_question": str(t.get("market_question") or ""),
                    "sports_market_type": str(t.get("sports_market_type") or ""),
                    "display_market_type": display_market_type,
                    "market_type_key": market_type_key,
                    "market_type_inferred": 1 if market_type_inferred else 0,
                    "line": _float_or_none(t.get("line")),
                    "line_display": _line_display_for_market(market_type_key=market_type_key, line=t.get("line")),
                    "binding_status": "exact",
                    "is_tradeable": True,
                    "is_selected": 1,
                    "outcomes": [],
                }
                markets_by_key[key] = mk
            row_tradeable = int(t.get("is_tradeable") or 0) == 1
            if not row_tradeable:
                mk["is_tradeable"] = False
                if str(mk.get("binding_status") or "") == "exact":
                    mk["binding_status"] = "unresolved"
            outcome_label = str(t.get("outcome_label") or "").strip()
            outcome_idx = int(t.get("outcome_index") or 0)
            if (
                _norm(str(mk.get("market_type_key") or "")) == "moneyline"
            ) and (
                not outcome_label
                or outcome_label.lower() in {"yes", "no"}
            ):
                team_names = team_names_by_event.get(event_id, [])
                if outcome_idx < len(team_names):
                    outcome_label = str(team_names[outcome_idx])
            if not outcome_label:
                outcome_label = f"outcome_{outcome_idx}"
            mk["outcomes"].append(
                {
                    "outcome_index": outcome_idx,
                    "outcome_label": outcome_label,
                    "token_id": str(t.get("token_id") or ""),
                    "binding_status": str(t.get("binding_status") or ""),
                    "reason_code": str(t.get("reason_code") or ""),
                    "is_tradeable": 1 if row_tradeable else 0,
                }
            )
        semantic_markets = list(markets_by_key.values())
        for market in semantic_markets:
            market["outcomes"] = sorted(
                list(market.get("outcomes") or []),
                key=lambda x: int(x.get("outcome_index") or 0),
            )
        semantic_markets.sort(
            key=lambda x: (
                str(x.get("event_id") or ""),
                *_market_sort_key(x),
            )
        )

        selected_keys = {
            (str(m.get("event_id") or ""), str(m.get("condition_id") or ""))
            for m in semantic_markets
            if str(m.get("event_id") or "") and str(m.get("condition_id") or "")
        }
        unselected_markets: list[dict[str, Any]] = []
        if selected_event_ids:
            placeholders = ",".join("?" for _ in selected_event_ids)
            all_market_rows = [
                dict(r)
                for r in self._db.execute(
                    f"""
                    SELECT
                        condition_id,
                        event_id,
                        slug AS market_slug,
                        question AS market_question,
                        sports_market_type,
                        line
                    FROM pm_markets
                    WHERE event_id IN ({placeholders})
                    ORDER BY event_id, condition_id
                    """,
                    tuple(selected_event_ids),
                ).fetchall()
            ]
            condition_ids = sorted({str(r.get("condition_id") or "") for r in all_market_rows if str(r.get("condition_id") or "")})
            tokens_by_condition: dict[str, list[dict[str, Any]]] = {}
            if condition_ids:
                cond_placeholders = ",".join("?" for _ in condition_ids)
                token_rows = [
                    dict(r)
                    for r in self._db.execute(
                        f"""
                        SELECT condition_id, outcome_index, token_id, outcome_label
                        FROM pm_market_tokens
                        WHERE condition_id IN ({cond_placeholders})
                        ORDER BY condition_id, outcome_index
                        """,
                        tuple(condition_ids),
                    ).fetchall()
                ]
                for token_row in token_rows:
                    cid = str(token_row.get("condition_id") or "")
                    if not cid:
                        continue
                    tokens_by_condition.setdefault(cid, []).append(token_row)

            for market_row in all_market_rows:
                condition_id = str(market_row.get("condition_id") or "")
                event_id = str(market_row.get("event_id") or "")
                if not condition_id or not event_id:
                    continue
                key = (event_id, condition_id)
                if key in selected_keys:
                    continue
                market_type_key, display_market_type, market_type_inferred = _normalize_market_type_info(
                    sports_market_type=market_row.get("sports_market_type"),
                    market_question=market_row.get("market_question"),
                    market_slug=market_row.get("market_slug"),
                )
                outcomes: list[dict[str, Any]] = []
                for token_row in tokens_by_condition.get(condition_id, []):
                    outcome_idx = int(token_row.get("outcome_index") or 0)
                    outcome_label = str(token_row.get("outcome_label") or "").strip()
                    if (
                        market_type_key == "moneyline"
                        and (not outcome_label or outcome_label.lower() in {"yes", "no"})
                    ):
                        team_names = team_names_by_event.get(event_id, [])
                        if outcome_idx < len(team_names):
                            outcome_label = str(team_names[outcome_idx])
                    if not outcome_label:
                        outcome_label = f"outcome_{outcome_idx}"
                    outcomes.append(
                        {
                            "outcome_index": outcome_idx,
                            "outcome_label": outcome_label,
                            "token_id": str(token_row.get("token_id") or ""),
                            "binding_status": "not_selected",
                            "reason_code": "not_selected",
                            "is_tradeable": 0,
                        }
                    )
                outcomes.sort(key=lambda x: int(x.get("outcome_index") or 0))
                unselected_markets.append(
                    {
                        "event_id": event_id,
                        "condition_id": condition_id,
                        "market_slug": str(market_row.get("market_slug") or ""),
                        "market_question": str(market_row.get("market_question") or ""),
                        "sports_market_type": str(market_row.get("sports_market_type") or ""),
                        "display_market_type": display_market_type,
                        "market_type_key": market_type_key,
                        "market_type_inferred": 1 if market_type_inferred else 0,
                        "line": _float_or_none(market_row.get("line")),
                        "line_display": _line_display_for_market(market_type_key=market_type_key, line=market_row.get("line")),
                        "binding_status": "not_selected",
                        "is_tradeable": False,
                        "is_selected": 0,
                        "outcomes": outcomes,
                    }
                )
        unselected_markets.sort(
            key=lambda x: (
                str(x.get("event_id") or ""),
                *_market_sort_key(x),
            )
        )

        total_market_keys = {
            (str(m.get("event_id") or ""), str(m.get("condition_id") or ""))
            for m in (semantic_markets + unselected_markets)
            if str(m.get("event_id") or "") and str(m.get("condition_id") or "")
        }
        _provider_tz_map = {"boltodds": "ET", "kalstrop_v1": "UTC", "kalstrop_v2": "UTC"}
        return {
            "provider": p,
            "run_id": rid,
            "provider_game_id": gid,
            "found": True,
            "card": {
                "provider_game": {
                    "provider": p,
                    "provider_game_id": game.get("provider_game_id"),
                    "parse_status": game.get("parse_status"),
                    "parse_reason": game.get("parse_reason"),
                    "league_raw": game.get("league_raw"),
                    "sport_raw": game.get("sport_raw"),
                    "game_label": game.get("game_label"),
                    "when_raw": game.get("when_raw"),
                    "provider_timezone": _provider_tz_map.get(p, "UTC"),
                    "game_date_et": game.get("game_date_et"),
                    "kickoff_ts_utc": game.get("start_ts_utc"),
                    "home_raw": game.get("home_raw"),
                    "away_raw": game.get("away_raw"),
                },
                "canonicalization": {
                    "canonical_league": game.get("canonical_league"),
                    "canonical_home_team": game.get("canonical_home_team"),
                    "canonical_away_team": game.get("canonical_away_team"),
                    "event_slug_prefix": game.get("event_slug_prefix"),
                },
                "event_resolution": {
                    "resolution_state": game.get("resolution_state"),
                    "reason_code": game.get("reason_code"),
                    "selected_event_id": game.get("selected_event_id"),
                    "selected_events": selected_events,
                    "selected_event_slug": game.get("selected_event_slug"),
                    "selected_event_title": game.get("selected_event_title"),
                    "selected_event_kickoff_ts_utc": game.get("selected_event_kickoff_ts_utc"),
                    "selected_event_status": str(game.get("selected_event_status") or ""),
                    "kickoff_tolerance_minutes": game.get("kickoff_tolerance_minutes"),
                    "kickoff_delta_sec": game.get("kickoff_delta_sec"),
                    "score_tuple": game.get("score_tuple") if isinstance(game.get("score_tuple"), list) else [],
                    "used_slug_fallback": bool(int(game.get("used_slug_fallback") or 0)),
                },
                "market_bindings": {
                    "targets": targets,
                    "markets": semantic_markets,
                    "unselected_markets": unselected_markets,
                    "n_targets": len(targets),
                    "n_tradeable_targets": sum(1 for t in targets if int(t.get("is_tradeable") or 0) == 1),
                    "n_selected_markets": len(selected_keys),
                    "n_total_markets": len(total_market_keys),
                    "is_tradeable": bool(int(game.get("is_tradeable") or 0)),
                },
                "notes": {
                    "reason_notes": reason_notes,
                    "trace": trace,
                },
                "latest_decision": {
                    "decision": str(game.get("latest_decision") or ""),
                    "note": str(game.get("latest_decision_note") or ""),
                    "actor": str(game.get("latest_decision_actor") or ""),
                    "decided_at": _int_or_none(game.get("latest_decision_ts")),
                },
                "provider_siblings": self._get_provider_siblings(
                    run_id=rid,
                    canonical_home_team=str(game.get("canonical_home_team") or ""),
                    canonical_away_team=str(game.get("canonical_away_team") or ""),
                    game_date_et=str(game.get("game_date_et") or ""),
                    exclude_provider=p,
                    exclude_game_id=gid,
                ),
            },
        }

    def _get_provider_siblings(
        self,
        *,
        run_id: int,
        canonical_home_team: str,
        canonical_away_team: str,
        game_date_et: str,
        exclude_provider: str,
        exclude_game_id: str,
    ) -> list[dict[str, Any]]:
        """Return other provider games for the same canonical game."""
        if not canonical_home_team or not canonical_away_team or not game_date_et:
            return []
        rows = self._db.execute(
            """
            SELECT provider, provider_game_id, home_raw, away_raw, when_raw,
                   league_raw, sport_raw, game_label, parse_status, binding_status
            FROM link_run_provider_games
            WHERE run_id = ?
              AND canonical_home_team = ?
              AND canonical_away_team = ?
              AND game_date_et = ?
              AND NOT (provider = ? AND provider_game_id = ?)
            ORDER BY provider, provider_game_id
            """,
            (run_id, canonical_home_team, canonical_away_team, game_date_et,
             exclude_provider, exclude_game_id),
        ).fetchall()
        return [dict(r) for r in rows]

    def record_decision(
        self,
        *,
        provider: str,
        run_id: int,
        provider_game_id: str,
        decision: str,
        note: str = "",
        actor: str = "cli",
    ) -> dict[str, Any]:
        p = _norm(provider)
        rid = int(run_id)
        gid = str(provider_game_id or "").strip()
        d = _norm(decision)
        if d not in self.VALID_DECISIONS:
            raise ValueError(f"decision must be one of: {','.join(sorted(self.VALID_DECISIONS))}")
        exists = self._db.execute(
            """
            SELECT 1
            FROM link_run_provider_games
            WHERE run_id = ? AND provider = ? AND provider_game_id = ? AND parse_status = 'ok'
            LIMIT 1
            """,
            (rid, p, gid),
        ).fetchone()
        if exists is None:
            raise ValueError("provider_game_id is not in review scope for this run")
        decision_id = self._db.linking.insert_review_decision(
            run_id=rid,
            provider=p,
            provider_game_id=gid,
            decision=d,
            note=str(note or ""),
            actor=str(actor or "cli"),
            decided_at=int(time.time()),
            commit=True,
        )
        progress = self.get_decision_progress(provider=p, run_id=rid)
        return {
            "decision_id": int(decision_id),
            "provider": p,
            "run_id": rid,
            "provider_game_id": gid,
            "decision": d,
            "note": str(note or ""),
            "actor": str(actor or "cli"),
            "progress": progress,
        }

    # Backward-compat wrappers for v1 callers.
    def run_summary(self, *, provider: str, run_id: int | None = None) -> dict[str, Any]:
        return self.get_run_status(provider=provider, run_id=run_id)

    def unresolved_games(
        self,
        *,
        provider: str,
        run_id: int | None = None,
        reason: str | None = None,
        limit: int | None = None,
        include_inactive: bool = False,
        live_policy: LoadedLiveTradingPolicy | None = None,
        now_ts_utc: int | None = None,
    ) -> list[dict[str, Any]]:
        status = self.get_run_status(
            provider=provider,
            run_id=run_id,
            include_inactive=include_inactive,
            live_policy=live_policy,
            now_ts_utc=now_ts_utc,
        )
        if not bool(status.get("run_found")):
            return []
        rid = int(status["run_id"])
        reason_norm = _norm(reason or "")
        lim = max(1, int(limit or 200))
        rows = self.get_queue(
            provider=provider,
            run_id=rid,
            scope="unresolved",
            parse_status="ok",
            limit=lim * 5,
            include_inactive=include_inactive,
            live_policy=live_policy,
            now_ts_utc=now_ts_utc,
        )
        out = []
        for row in rows:
            rs = str(row.get("resolution_state") or "")
            if rs in {"MATCHED_CLEAN", "MATCHED_WITH_WARNINGS"}:
                continue
            rc = _norm(str(row.get("reason_code") or ""))
            if reason_norm and rc != reason_norm:
                continue
            out.append(row)
            if len(out) >= lim:
                break
        return out

    def matched_games(
        self,
        *,
        provider: str,
        run_id: int | None = None,
        tradeable_only: bool = False,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int | None = None,
        include_inactive: bool = False,
        live_policy: LoadedLiveTradingPolicy | None = None,
        now_ts_utc: int | None = None,
    ) -> list[dict[str, Any]]:
        status = self.get_run_status(
            provider=provider,
            run_id=run_id,
            include_inactive=include_inactive,
            live_policy=live_policy,
            now_ts_utc=now_ts_utc,
        )
        if not bool(status.get("run_found")):
            return []
        rid = int(status["run_id"])
        df = str(date_from or "").strip()
        dt = str(date_to or "").strip()
        lim = max(1, int(limit or 500))
        rows = self.get_queue(
            provider=provider,
            run_id=rid,
            scope="mapped",
            parse_status="ok",
            limit=lim * 10,
            include_inactive=include_inactive,
            live_policy=live_policy,
            now_ts_utc=now_ts_utc,
        )
        agg_rows = self._db.execute(
            """
            SELECT
                provider_game_id,
                COUNT(DISTINCT condition_id) AS n_markets,
                SUM(CASE WHEN is_tradeable = 1 THEN 1 ELSE 0 END) AS n_tradeable_targets
            FROM link_run_market_targets
            WHERE run_id = ? AND provider = ?
            GROUP BY provider_game_id
            """,
            (rid, _norm(provider)),
        ).fetchall()
        agg_idx = {
            str(r["provider_game_id"]): {
                "n_markets": int(r["n_markets"] or 0),
                "n_tradeable_targets": int(r["n_tradeable_targets"] or 0),
            }
            for r in agg_rows
        }
        market_types_rows = self._db.execute(
            """
            SELECT provider_game_id, sports_market_type
            FROM link_run_market_targets
            WHERE run_id = ? AND provider = ?
            ORDER BY provider_game_id, sports_market_type
            """,
            (rid, _norm(provider)),
        ).fetchall()
        market_types_idx: dict[str, set[str]] = {}
        for r in market_types_rows:
            gid = str(r["provider_game_id"] or "")
            market_type = normalize_sports_market_type(r["sports_market_type"])
            if not gid or market_type == "other":
                continue
            market_types_idx.setdefault(gid, set()).add(market_type)
        kickoff_rows = self._db.execute(
            """
            SELECT c.provider_game_id, c.event_id
            FROM link_run_event_candidates c
            WHERE c.run_id = ? AND c.provider = ? AND c.is_selected = 1
            ORDER BY c.provider_game_id, c.candidate_rank, c.event_id
            """,
            (rid, _norm(provider)),
        ).fetchall()
        event_ids_by_game: dict[str, list[str]] = {}
        for row in kickoff_rows:
            gid = str(row["provider_game_id"] or "")
            eid = str(row["event_id"] or "")
            if not gid or not eid:
                continue
            event_ids_by_game.setdefault(gid, []).append(eid)
        primary_rows = self._db.execute(
            """
            SELECT provider_game_id, selected_event_id
            FROM link_run_game_reviews
            WHERE run_id = ? AND provider = ?
            """,
            (rid, _norm(provider)),
        ).fetchall()
        event_by_game = {str(r["provider_game_id"] or ""): str(r["selected_event_id"] or "") for r in primary_rows}
        event_ids = sorted({eid for ids in event_ids_by_game.values() for eid in ids if eid} | {eid for eid in event_by_game.values() if eid})
        kickoff_idx: dict[str, int | None] = {}
        if event_ids:
            placeholders = ",".join("?" for _ in event_ids)
            rows_ev = self._db.execute(
                f"""
                SELECT event_id, kickoff_ts_utc
                FROM pm_events
                WHERE event_id IN ({placeholders})
                """,
                tuple(event_ids),
            ).fetchall()
            kickoff_idx = {str(r["event_id"] or ""): _int_or_none(r["kickoff_ts_utc"]) for r in rows_ev}
        out: list[dict[str, Any]] = []
        for row in rows:
            rs = str(row.get("resolution_state") or "")
            if rs not in {"MATCHED_CLEAN", "MATCHED_WITH_WARNINGS"}:
                continue
            if tradeable_only and int(row.get("is_tradeable") or 0) != 1:
                continue
            gd = str(row.get("game_date_et") or "")
            if df and gd < df:
                continue
            if dt and gd > dt:
                continue
            gid = str(row.get("provider_game_id") or "")
            selected_event_ids = list(event_ids_by_game.get(gid, []))
            event_id = str(row.get("selected_event_id") or event_by_game.get(gid) or (selected_event_ids[0] if selected_event_ids else ""))
            start_ts = _int_or_none(row.get("start_ts_utc"))
            event_kickoff = kickoff_idx.get(event_id)
            agg = agg_idx.get(gid, {"n_markets": 0, "n_tradeable_targets": 0})
            out.append(
                {
                    **row,
                    "event_id": event_id,
                    "selected_event_ids": selected_event_ids,
                    "n_markets": int(agg["n_markets"]),
                    "n_tradeable_targets": int(agg["n_tradeable_targets"]),
                    "market_types_csv": ",".join(sorted(market_types_idx.get(gid, set()))),
                    "kickoff_delta_min": _kickoff_delta_min(
                        provider_start_ts_utc=start_ts,
                        event_kickoff_ts_utc=event_kickoff,
                    ),
                }
            )
            if len(out) >= lim:
                break
        return out

    def game_drilldown(
        self,
        *,
        provider: str,
        provider_game_id: str,
        run_id: int | None = None,
        include_inactive: bool = False,
        live_policy: LoadedLiveTradingPolicy | None = None,
        now_ts_utc: int | None = None,
    ) -> dict[str, Any]:
        status = self.get_run_status(
            provider=provider,
            run_id=run_id,
            include_inactive=include_inactive,
            live_policy=live_policy,
            now_ts_utc=now_ts_utc,
        )
        if not bool(status.get("run_found")):
            return {
                "provider": _norm(provider),
                "provider_game_id": str(provider_game_id or "").strip(),
                "run_id": None,
                "game": None,
                "event": None,
                "markets": [],
            }
        rid = int(status["run_id"])
        card = self.get_game_card(provider=provider, run_id=rid, provider_game_id=provider_game_id)
        if not bool(card.get("found")):
            return {
                "provider": _norm(provider),
                "provider_game_id": str(provider_game_id or "").strip(),
                "run_id": rid,
                "game": None,
                "event": None,
                "markets": [],
            }
        c = card["card"]
        event = c["event_resolution"]
        game = {
            **c["provider_game"],
            **c["canonicalization"],
            "binding_status": event.get("resolution_state"),
            "reason_code": event.get("reason_code"),
            "is_tradeable": 1 if c["market_bindings"].get("is_tradeable") else 0,
            "event_id": event.get("selected_event_id"),
        }
        return {
            "provider": _norm(provider),
            "provider_game_id": str(provider_game_id or "").strip(),
            "run_id": rid,
            "game": game,
            "event": {
                "event_id": event.get("selected_event_id"),
                "selected_events": event.get("selected_events") if isinstance(event.get("selected_events"), list) else [],
                "event_title": event.get("selected_event_title"),
                "event_slug": event.get("selected_event_slug"),
                "kickoff_ts_utc": event.get("selected_event_kickoff_ts_utc"),
                "kickoff_delta_min": _kickoff_delta_min(
                    provider_start_ts_utc=c["provider_game"].get("kickoff_ts_utc"),
                    event_kickoff_ts_utc=event.get("selected_event_kickoff_ts_utc"),
                ),
            },
            "markets": c["market_bindings"].get("targets") or [],
        }
