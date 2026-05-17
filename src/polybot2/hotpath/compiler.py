"""Control-plane compiler for scoped hotpath plans."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import time
from typing import Any

from polybot2.hotpath.contracts import CompiledGamePlan, CompiledMarket, CompiledPlan, CompiledTarget
from polybot2.linking.actionable import actionable_game_ids
from polybot2.linking.mapping_loader import LoadedLiveTradingPolicy, load_live_trading_policy
from polybot2.market_types import is_totals_market_type, normalize_sports_market_type


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _line_key(value: float | None) -> str:
    if value is None:
        return ""
    text = f"{float(value):.6f}".rstrip("0").rstrip(".")
    return text or "0"


def _parse_exact_score_from_slug(slug_norm: str) -> tuple[int, int] | None:
    """Parse predicted score from exact-score market slug.

    Slug format: "...-exact-score-{home}-{away}" (e.g., "-exact-score-1-3")
    Returns (home_pred, away_pred) or None for "any-other" / unparseable.
    """
    idx = slug_norm.find("-exact-score-")
    if idx < 0:
        return None
    suffix = slug_norm[idx + len("-exact-score-"):]
    if suffix.startswith("any"):
        return None  # "any-other" — not a specific score prediction
    parts = suffix.split("-")
    if len(parts) != 2:
        return None
    try:
        return (int(parts[0]), int(parts[1]))
    except ValueError:
        return None


def _spread_side_from_slug(slug_norm: str) -> str:
    """Determine spread side from slug.

    Slug format: "...-spread-home-{line}" or "...-spread-away-{line}"
    e.g. "epl-sun-mun-2026-05-09-spread-home-2pt5"
    Returns "home", "away", or "unknown".
    """
    if "-spread-home-" in slug_norm or slug_norm.endswith("-spread-home"):
        return "home"
    if "-spread-away-" in slug_norm or slug_norm.endswith("-spread-away"):
        return "away"
    return "unknown"


def _three_way_side_from_slug(
    slug_norm: str,
    home_code: str,
    away_code: str,
) -> str:
    """Determine which side a three-way market refers to from its slug.

    Polymarket slug conventions:
      Moneyline:  {event_slug}-{team_code}  or  {event_slug}-draw
      Halftime:   {event_slug}-halftime-result-home / -away / -draw

    Returns "home", "away", "draw", or "unknown".
    """
    if not slug_norm:
        return "unknown"
    # Check slug suffix
    if slug_norm.endswith("-draw"):
        return "draw"
    if slug_norm.endswith("-home"):
        return "home"
    if slug_norm.endswith("-away"):
        return "away"
    # Moneyline: slug ends with the polymarket team code
    if home_code and slug_norm.endswith(f"-{home_code}"):
        return "home"
    if away_code and slug_norm.endswith(f"-{away_code}"):
        return "away"
    return "unknown"


def _parse_outcome_semantic(
    *,
    outcome_label: str,
    question: str,
    market_slug: str,
    outcome_index: int,
    sports_market_type: str,
    canonical_home_team: str,
    canonical_away_team: str,
    home_polymarket_code: str = "",
    away_polymarket_code: str = "",
) -> str:
    """Dispatch on sports_market_type. Each branch uses only its reliable signal.

    No cross-type label matching — spreads never see the "over"/"under" check,
    totals never see the team-name check, etc.
    """
    label = _norm(outcome_label)
    slug_norm = _norm(market_slug)
    sports_type = _norm(sports_market_type)
    home_norm = _norm(canonical_home_team)
    away_norm = _norm(canonical_away_team)
    home_code = _norm(home_polymarket_code)
    away_code = _norm(away_polymarket_code)
    idx = int(outcome_index)

    # ── Totals / corners: labels "Over" / "Under" (exact match) ──────
    if sports_type in {"totals", "total_corners"} or is_totals_market_type(sports_type):
        if label == "over":
            return "over"
        if label == "under":
            return "under"
        # Fallback: outcome index (0=over, 1=under)
        if idx == 0:
            return "over"
        if idx == 1:
            return "under"
        return "unknown"

    # ── Spreads: slug determines side, index determines covers/not-covers ─
    if sports_type in {"spread", "spreads"}:
        side = _spread_side_from_slug(slug_norm)
        if side == "unknown":
            return "unknown"
        if idx == 0:
            return f"{side}_covers"          # e.g. "home_covers", "away_covers"
        return f"{side}_not_covers"          # e.g. "home_not_covers", "away_not_covers"

    # ── Moneyline: team-name labels (baseball) or slug (soccer 3-way) ─
    if sports_type == "moneyline":
        # Baseball: outcome labels are team names
        if home_norm and (home_norm in label or (len(label) >= 4 and label in home_norm)):
            return "home"
        if away_norm and (away_norm in label or (len(label) >= 4 and label in away_norm)):
            return "away"
        # Soccer: slug determines side, index determines yes/no
        side = _three_way_side_from_slug(slug_norm, home_code, away_code)
        if side != "unknown":
            return f"{side}_yes" if idx == 0 else f"{side}_no"
        return "unknown"

    # ── Halftime result: slug determines side ─────────────────────────
    if sports_type in {"soccer_halftime_result", "halftime_result", "ht_result"}:
        side = _three_way_side_from_slug(slug_norm, home_code, away_code)
        if side != "unknown":
            return f"{side}_yes" if idx == 0 else f"{side}_no"
        return "unknown"

    # ── BTTS: index 0 = yes, 1 = no ──────────────────────────────────
    if sports_type == "btts":
        return "yes" if idx == 0 else "no"

    # ── NRFI: index 0 = yes, 1 = no ──────────────────────────────────
    if sports_type == "nrfi":
        return "yes" if idx == 0 else "no"

    # ── Exact score: slug determines score, index determines yes/no ───
    if sports_type == "soccer_exact_score":
        score = _parse_exact_score_from_slug(slug_norm)
        if score is not None:
            return "exact_yes" if idx == 0 else "exact_no"
        # "any other score" market
        if "-exact-score-any" in slug_norm:
            return "any_other_yes" if idx == 0 else "any_other_no"
        return "unknown"

    return "unknown"


def _json_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ScopeGameRow:
    provider_game_id: str
    canonical_league: str
    canonical_home_team: str
    canonical_away_team: str
    kickoff_ts_utc: int | None
    decision: str


@dataclass(frozen=True, slots=True)
class ScopedLaunchCheck:
    provider: str
    league: str
    run_id: int
    run_found: bool
    in_scope_games: int
    approved_games: int
    pending_games: int
    rejected_games: int
    skipped_games: int
    tradeable_targets: int
    blockers: tuple[str, ...]
    scope_rows: tuple[ScopeGameRow, ...]

    @property
    def eligible_game_ids(self) -> tuple[str, ...]:
        return tuple(r.provider_game_id for r in self.scope_rows if r.decision != "reject")


class HotPathPlanError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = str(code)


def evaluate_hotpath_scope(
    *,
    db: Any,
    provider: str,
    league: str,
    run_id: int,
    live_policy: LoadedLiveTradingPolicy | None = None,
    now_ts_utc: int | None = None,
    include_inactive: bool = False,
) -> ScopedLaunchCheck:
    p = _norm(provider)
    lk = _norm(league)
    rid = int(run_id)

    run_row = db.linking.load_link_run(provider=p, run_id=rid)
    if run_row is None:
        return ScopedLaunchCheck(
            provider=p,
            league=lk,
            run_id=rid,
            run_found=False,
            in_scope_games=0,
            approved_games=0,
            pending_games=0,
            rejected_games=0,
            skipped_games=0,
            tradeable_targets=0,
            blockers=("no_link_run",),
            scope_rows=tuple(),
        )

    rows = db.execute(
        """
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
            pg.provider_game_id,
            pg.canonical_league,
            pg.canonical_home_team,
            pg.canonical_away_team,
            pg.start_ts_utc,
            COALESCE(ld.decision, '') AS decision
        FROM link_run_provider_games pg
        LEFT JOIN latest_decisions ld
          ON ld.run_id = pg.run_id
         AND ld.provider = pg.provider
         AND ld.provider_game_id = pg.provider_game_id
        WHERE pg.run_id = ?
          AND pg.provider = ?
          AND pg.parse_status = 'ok'
          AND pg.canonical_league = ?
        ORDER BY pg.game_date_et ASC, pg.provider_game_id ASC
        """,
        (rid, p, rid, p, lk),
    ).fetchall()

    scope_rows_all = tuple(
        ScopeGameRow(
            provider_game_id=str(r["provider_game_id"] or ""),
            canonical_league=str(r["canonical_league"] or ""),
            canonical_home_team=str(r["canonical_home_team"] or ""),
            canonical_away_team=str(r["canonical_away_team"] or ""),
            kickoff_ts_utc=(None if r["start_ts_utc"] is None else int(r["start_ts_utc"])),
            decision=str(r["decision"] or "").strip().lower(),
        )
        for r in rows
    )
    if bool(include_inactive):
        scope_rows = scope_rows_all
    else:
        policy = live_policy or load_live_trading_policy()
        runtime_cfg = dict((policy.hotpath_runtime_by_league or {}).get(lk, {}) or {})
        max_age_seconds = int(runtime_cfg.get("provider_catalog_max_age_seconds", 600))
        effective_now_ts: int | None = None
        try:
            if run_row.get("run_ts") is not None:
                effective_now_ts = int(run_row.get("run_ts"))
        except (TypeError, ValueError):
            effective_now_ts = None
        if now_ts_utc is not None:
            effective_now_ts = int(now_ts_utc)
        active_ids = actionable_game_ids(
            db=db,
            provider=p,
            run_id=rid,
            max_age_seconds=max_age_seconds,
            now_ts_utc=effective_now_ts,
            league=lk,
        )
        scope_rows = tuple(r for r in scope_rows_all if r.provider_game_id in active_ids)

    approved = sum(1 for r in scope_rows if r.decision == "approve")
    rejected = sum(1 for r in scope_rows if r.decision == "reject")
    skipped = sum(1 for r in scope_rows if r.decision == "skip")
    pending = sum(1 for r in scope_rows if r.decision not in {"approve", "reject", "skip"})

    tradeable_targets = 0
    eligible_ids = [r.provider_game_id for r in scope_rows if r.provider_game_id and r.decision != "reject"]
    if eligible_ids:
        placeholders = ",".join("?" for _ in eligible_ids)
        status_closed = ("closed", "resolved", "ended", "finished", "final", "complete", "completed", "cancelled", "canceled")
        status_placeholders = ",".join("?" for _ in status_closed)
        row = db.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM link_run_market_targets t
            LEFT JOIN pm_markets m
              ON m.condition_id = t.condition_id
            LEFT JOIN link_run_game_reviews gr
              ON gr.run_id = t.run_id
             AND gr.provider = t.provider
             AND gr.provider_game_id = t.provider_game_id
            LEFT JOIN pm_events pe
              ON pe.event_id = gr.selected_event_id
            WHERE t.run_id = ?
              AND t.provider = ?
              AND t.is_tradeable = 1
              AND m.condition_id IS NOT NULL
              AND COALESCE(m.resolved, 0) = 0
              AND COALESCE(TRIM(gr.selected_event_id), '') <> ''
              AND pe.event_id IS NOT NULL
              AND LOWER(TRIM(COALESCE(pe.status, ''))) NOT IN ({status_placeholders})
              AND t.provider_game_id IN ({placeholders})
            """,
            (rid, p, *status_closed, *eligible_ids),
        ).fetchone()
        tradeable_targets = int((dict(row or {}).get("n")) or 0)

    blockers: list[str] = []
    if not scope_rows:
        blockers.append("no_in_scope_games")
    if scope_rows and tradeable_targets <= 0:
        blockers.append("no_tradeable_targets")

    return ScopedLaunchCheck(
        provider=p,
        league=lk,
        run_id=rid,
        run_found=True,
        in_scope_games=len(scope_rows),
        approved_games=approved,
        pending_games=pending,
        rejected_games=rejected,
        skipped_games=skipped,
        tradeable_targets=tradeable_targets,
        blockers=tuple(blockers),
        scope_rows=scope_rows,
    )


def _load_allowed_market_types(*, policy: LoadedLiveTradingPolicy, league: str) -> set[str]:
    lk = _norm(league)
    allowed = policy.live_betting_market_types_by_league.get(lk)
    if not allowed:
        raise HotPathPlanError(
            "missing_policy_market_types",
            f"LIVE_BETTING_MARKET_TYPES does not define league={lk}",
        )
    return {normalize_sports_market_type(x) for x in allowed if normalize_sports_market_type(x) != "other"}


def compile_hotpath_plan(
    *,
    db: Any,
    provider: str,
    league: str,
    run_id: int,
    live_policy: LoadedLiveTradingPolicy | None = None,
    now_ts_utc: int | None = None,
    plan_horizon_hours: int | None = None,
    exclude_strategy_keys: set[str] | None = None,
    include_inactive: bool = False,
) -> CompiledPlan:
    policy = live_policy or load_live_trading_policy()
    scope = evaluate_hotpath_scope(
        db=db,
        provider=provider,
        league=league,
        run_id=run_id,
        live_policy=policy,
        now_ts_utc=now_ts_utc,
        include_inactive=include_inactive,
    )
    if not scope.run_found:
        raise HotPathPlanError("no_link_run", f"link run not found for provider={provider} run_id={run_id}")
    blockers = list(scope.blockers)
    if blockers:
        raise HotPathPlanError(
            "scope_blocked",
            f"scope blockers={','.join(blockers)} provider={scope.provider} league={scope.league} run_id={scope.run_id}",
        )

    allowed_market_types = _load_allowed_market_types(policy=policy, league=scope.league)

    selected_ids = [x for x in scope.eligible_game_ids if x]
    dropped_missing_kickoff = 0
    dropped_outside_window = 0
    if plan_horizon_hours is not None:
        horizon_hours = int(plan_horizon_hours)
        if horizon_hours <= 0:
            raise HotPathPlanError("invalid_plan_horizon", "plan_horizon_hours must be > 0")
        now_ts = int(time.time()) if now_ts_utc is None else int(now_ts_utc)
        upper_ts = int(now_ts + (horizon_hours * 3600))
        lower_ts = int(now_ts - (6 * 3600))
        meta_by_id = {r.provider_game_id: r for r in scope.scope_rows if r.provider_game_id}
        filtered_ids: list[str] = []
        for gid in selected_ids:
            meta = meta_by_id.get(str(gid))
            kickoff = None if meta is None else meta.kickoff_ts_utc
            if kickoff is None:
                dropped_missing_kickoff += 1
                continue
            kickoff_ts = int(kickoff)
            if kickoff_ts < lower_ts or kickoff_ts > upper_ts:
                dropped_outside_window += 1
                continue
            filtered_ids.append(str(gid))
        selected_ids = filtered_ids
    if not selected_ids:
        if plan_horizon_hours is not None:
            raise HotPathPlanError(
                "no_window_games",
                "no in-scope games selected within kickoff window "
                f"(missing_kickoff={dropped_missing_kickoff},outside_window={dropped_outside_window})",
            )
        raise HotPathPlanError("no_selected_games", "no in-scope games selected for compilation")

    placeholders = ",".join("?" for _ in selected_ids)
    rows = db.execute(
        f"""
        SELECT
            t.provider_game_id,
            t.condition_id,
            t.outcome_index,
            t.token_id,
            t.sports_market_type,
            m.market_id,
            m.event_id,
            m.question,
            m.line,
            m.slug,
            tok.outcome_label
        FROM link_run_market_targets t
        LEFT JOIN link_run_game_reviews gr
          ON gr.run_id = t.run_id
         AND gr.provider = t.provider
         AND gr.provider_game_id = t.provider_game_id
        LEFT JOIN pm_events pe
          ON pe.event_id = gr.selected_event_id
        LEFT JOIN pm_markets m
          ON m.condition_id = t.condition_id
        LEFT JOIN pm_market_tokens tok
          ON tok.condition_id = t.condition_id
         AND tok.outcome_index = t.outcome_index
        WHERE t.run_id = ?
          AND t.provider = ?
          AND t.is_tradeable = 1
          AND m.condition_id IS NOT NULL
          AND COALESCE(m.resolved, 0) = 0
          AND COALESCE(TRIM(gr.selected_event_id), '') <> ''
          AND pe.event_id IS NOT NULL
          AND LOWER(TRIM(COALESCE(pe.status, ''))) NOT IN (
            'closed','resolved','ended','finished','final','complete','completed','cancelled','canceled'
          )
          AND t.provider_game_id IN ({placeholders})
        ORDER BY
            t.provider_game_id ASC,
            LOWER(COALESCE(t.sports_market_type, '')) ASC,
            t.condition_id ASC,
            t.outcome_index ASC,
            t.token_id ASC
        """,
        (scope.run_id, scope.provider, *selected_ids),
    ).fetchall()
    if not rows:
        raise HotPathPlanError(
            "no_actionable_targets",
            "no unresolved/open tradeable targets remain for selected games",
        )

    scope_meta = {
        r.provider_game_id: r
        for r in scope.scope_rows
        if r.provider_game_id and r.decision != "reject"
    }
    by_game: dict[str, dict[str, Any]] = {}

    # Build canonical-team → polymarket-code lookup for slug-based disambiguation.
    _pm_code_by_canonical: dict[str, str] = {}
    try:
        from polybot2.linking.mapping_loader import load_mapping as _load_mapping
        _mapping = _load_mapping()
        _team_map = _mapping.team_map.get(league, {})
        for _canonical, _meta in _team_map.items():
            _code = _norm(str(_meta.get("polymarket_code", "")))
            if _code:
                _pm_code_by_canonical[_norm(_canonical)] = _code
    except Exception:
        pass

    dropped_totals_rows = 0
    dropped_policy_rows = 0
    totals_seen_by_game: dict[str, int] = {}
    totals_valid_by_game: dict[str, int] = {}
    strategy_keys_seen: set[str] = set()

    for row in rows:
        gid = str(row["provider_game_id"] or "")
        if gid not in scope_meta:
            continue
        sports_market_type = normalize_sports_market_type(row["sports_market_type"])
        if sports_market_type not in allowed_market_types:
            dropped_policy_rows += 1
            continue

        condition_id = str(row["condition_id"] or "")
        if not condition_id:
            continue

        line = row["line"]
        line_val = None if line is None else float(line)

        outcome_index = int(row["outcome_index"] or 0)
        outcome_label = str(row["outcome_label"] or "")
        question = str(row["question"] or "")
        market_slug = str(row["slug"] or "")

        _home_canonical = _norm(str(scope_meta[gid].canonical_home_team))
        _away_canonical = _norm(str(scope_meta[gid].canonical_away_team))
        outcome_semantic = _parse_outcome_semantic(
            outcome_label=outcome_label,
            question=question,
            market_slug=market_slug,
            outcome_index=outcome_index,
            sports_market_type=sports_market_type,
            canonical_home_team=_home_canonical,
            canonical_away_team=_away_canonical,
            home_polymarket_code=_pm_code_by_canonical.get(_home_canonical, ""),
            away_polymarket_code=_pm_code_by_canonical.get(_away_canonical, ""),
        )

        if is_totals_market_type(sports_market_type):
            totals_seen_by_game[gid] = int(totals_seen_by_game.get(gid, 0)) + 1
            if line_val is None:
                dropped_totals_rows += 1
                continue
            if outcome_semantic not in {"over", "under"}:
                dropped_totals_rows += 1
                continue
            totals_valid_by_game[gid] = int(totals_valid_by_game.get(gid, 0)) + 1

        # Compute effective_line before strategy key assignment.
        effective_line = line_val
        if sports_market_type == "soccer_exact_score" and effective_line is None:
            exact_score = _parse_exact_score_from_slug(_norm(market_slug))
            if exact_score is not None:
                h_pred, a_pred = exact_score
                effective_line = float(h_pred) + float(a_pred) / 10.0

        if is_totals_market_type(sports_market_type) and outcome_semantic in {"over", "under"} and line_val is not None:
            line_key = _line_key(line_val)
            strategy_key = f"{gid}:TOTAL:{outcome_semantic.upper()}:{line_key}"
        elif sports_market_type == "nrfi" and outcome_semantic in {"yes", "no"}:
            strategy_key = f"{gid}:NRFI:{outcome_semantic.upper()}"
        elif sports_market_type == "moneyline" and outcome_semantic in {
            "home", "away", "home_yes", "home_no", "away_yes", "away_no", "draw_yes", "draw_no",
        }:
            strategy_key = f"{gid}:MONEYLINE:{outcome_semantic.upper()}"
        elif sports_market_type == "spread" and outcome_semantic in {
            "home_covers", "home_not_covers", "away_covers", "away_not_covers",
        } and line_val is not None:
            line_key = _line_key(line_val)
            strategy_key = f"{gid}:SPREAD:{outcome_semantic.upper()}:{line_key}"
        elif sports_market_type == "soccer_halftime_result" and outcome_semantic in {
            "home_yes", "home_no", "away_yes", "away_no", "draw_yes", "draw_no",
        }:
            strategy_key = f"{gid}:SOCCER_HALFTIME_RESULT:{outcome_semantic.upper()}"
        elif sports_market_type == "btts" and outcome_semantic in {"yes", "no"}:
            strategy_key = f"{gid}:BTTS:{outcome_semantic.upper()}"
        elif sports_market_type == "total_corners" and outcome_semantic in {"over", "under"} and line_val is not None:
            line_key = _line_key(line_val)
            strategy_key = f"{gid}:TOTAL_CORNERS:{outcome_semantic.upper()}:{line_key}"
        elif sports_market_type == "soccer_exact_score" and outcome_semantic in {"exact_yes", "exact_no"} and effective_line is not None:
            home_pred = int(effective_line)
            away_pred = int(round((effective_line - int(effective_line)) * 10))
            yes_no = "YES" if outcome_semantic == "exact_yes" else "NO"
            strategy_key = f"{gid}:EXACT_SCORE:{home_pred}_{away_pred}:{yes_no}"
        elif sports_market_type == "soccer_exact_score" and outcome_semantic in {"any_other_yes", "any_other_no"}:
            yes_no = "YES" if outcome_semantic == "any_other_yes" else "NO"
            strategy_key = f"{gid}:EXACT_SCORE:ANY_OTHER:{yes_no}"
        else:
            strategy_key = f"{gid}:{sports_market_type.upper()}:{condition_id}:{outcome_index}"

        if (
            (is_totals_market_type(sports_market_type) and outcome_semantic in {"over", "under"})
            or (sports_market_type == "nrfi" and outcome_semantic in {"yes", "no"})
            or (sports_market_type == "moneyline" and outcome_semantic in {
                "home", "away", "home_yes", "home_no", "away_yes", "away_no", "draw_yes", "draw_no",
            })
            or (sports_market_type == "spread" and outcome_semantic in {
                "home_covers", "home_not_covers", "away_covers", "away_not_covers",
            })
            or (sports_market_type == "soccer_halftime_result" and outcome_semantic.endswith(("_yes", "_no")))
            or (sports_market_type == "btts" and outcome_semantic in {"yes", "no"})
            or (sports_market_type == "total_corners" and outcome_semantic in {"over", "under"})
            or (sports_market_type == "soccer_exact_score" and outcome_semantic in {
                "exact_yes", "exact_no", "any_other_yes", "any_other_no",
            })
        ):
            # Some live snapshots can contain duplicated logical markets (same game+family+side+line).
            # Keep the first deterministic candidate and skip later duplicates instead of hard-failing compile.
            if strategy_key in strategy_keys_seen:
                continue
            strategy_keys_seen.add(strategy_key)

        if exclude_strategy_keys and strategy_key in exclude_strategy_keys:
            continue

        game_bucket = by_game.setdefault(gid, {"markets": {}, "meta": scope_meta[gid]})
        market_bucket = game_bucket["markets"].setdefault(
            condition_id,
            {
                "condition_id": condition_id,
                "market_id": str(row["market_id"] or ""),
                "event_id": str(row["event_id"] or ""),
                "sports_market_type": sports_market_type,
                "line": line_val,
                "question": question,
                "targets": [],
            },
        )

        market_bucket["targets"].append(
            CompiledTarget(
                condition_id=condition_id,
                outcome_index=outcome_index,
                token_id=str(row["token_id"] or ""),
                sports_market_type=sports_market_type,
                line=effective_line,
                outcome_label=outcome_label,
                outcome_semantic=outcome_semantic,
                strategy_key=strategy_key,
            )
        )

    for gid, seen in totals_seen_by_game.items():
        if int(seen) > 0 and int(totals_valid_by_game.get(gid, 0)) == 0:
            raise HotPathPlanError(
                "all_totals_invalid",
                f"all totals rows invalid for provider_game_id={gid}",
            )

    # Build cross-provider alternate game IDs lookup.
    # For each game in the plan, find provider_game_ids from other providers
    # that map to the same canonical game (same teams + date).
    _alt_ids_by_key: dict[tuple[str, str, int | None], list[tuple[str, str]]] = {}
    try:
        _alt_rows = db.execute(
            """
            SELECT provider, provider_game_id, canonical_home_team, canonical_away_team, start_ts_utc
            FROM link_run_provider_games
            WHERE run_id = ?
              AND provider != ?
              AND canonical_league = ?
              AND parse_status = 'ok'
              AND binding_status != ''
            """,
            (scope.run_id, scope.provider, scope.league),
        ).fetchall()
        for r in _alt_rows:
            _key = (
                str(r["canonical_home_team"] or "").strip().lower(),
                str(r["canonical_away_team"] or "").strip().lower(),
                r["start_ts_utc"],
            )
            _alt_ids_by_key.setdefault(_key, []).append(
                (str(r["provider"] or ""), str(r["provider_game_id"] or ""))
            )
    except Exception:
        pass  # Non-critical: plan works without alternates

    compiled_games: list[CompiledGamePlan] = []
    for gid in sorted(by_game.keys()):
        game_entry = by_game[gid]
        meta = game_entry["meta"]
        market_values = list(game_entry["markets"].values())
        compiled_markets: list[CompiledMarket] = []
        for market in sorted(
            market_values,
            key=lambda m: (
                str(m["sports_market_type"]),
                "" if m["line"] is None else _line_key(m["line"]),
                str(m["condition_id"]),
            ),
        ):
            targets = tuple(sorted(market["targets"], key=lambda t: (int(t.outcome_index), str(t.token_id))))
            compiled_markets.append(
                CompiledMarket(
                    condition_id=str(market["condition_id"]),
                    market_id=str(market["market_id"]),
                    event_id=str(market["event_id"]),
                    sports_market_type=str(market["sports_market_type"]),
                    line=(None if market["line"] is None else float(market["line"])),
                    question=str(market["question"]),
                    targets=targets,
                )
            )

        if not compiled_markets:
            continue

        # Look up alternate provider game IDs for this game.
        _game_key = (
            str(meta.canonical_home_team or "").strip().lower(),
            str(meta.canonical_away_team or "").strip().lower(),
            meta.kickoff_ts_utc,
        )
        _alternates = tuple(
            (p, pid) for p, pid in _alt_ids_by_key.get(_game_key, [])
            if pid and pid != str(meta.provider_game_id)
        )

        compiled_games.append(
            CompiledGamePlan(
                provider_game_id=str(meta.provider_game_id),
                canonical_league=str(meta.canonical_league),
                canonical_home_team=str(meta.canonical_home_team),
                canonical_away_team=str(meta.canonical_away_team),
                kickoff_ts_utc=(None if meta.kickoff_ts_utc is None else int(meta.kickoff_ts_utc)),
                markets=tuple(compiled_markets),
                alternate_provider_game_ids=_alternates,
            )
        )

    if not compiled_games:
        extra = ""
        if plan_horizon_hours is not None:
            extra = f" (missing_kickoff={dropped_missing_kickoff},outside_window={dropped_outside_window})"
        raise HotPathPlanError(
            "empty_plan",
            "no eligible tradeable targets remain after policy/validation filters" + extra,
        )

    canonical_payload = {
        "provider": scope.provider,
        "league": scope.league,
        "run_id": int(scope.run_id),
        "games": [
            {
                "provider_game_id": g.provider_game_id,
                "canonical_league": g.canonical_league,
                "canonical_home_team": g.canonical_home_team,
                "canonical_away_team": g.canonical_away_team,
                "kickoff_ts_utc": g.kickoff_ts_utc,
                "markets": [
                    {
                        "condition_id": m.condition_id,
                        "market_id": m.market_id,
                        "event_id": m.event_id,
                        "sports_market_type": m.sports_market_type,
                        "line": m.line,
                        "question": m.question,
                        "targets": [
                            {
                                "outcome_index": int(t.outcome_index),
                                "token_id": t.token_id,
                                "outcome_label": t.outcome_label,
                                "outcome_semantic": t.outcome_semantic,
                                "strategy_key": t.strategy_key,
                            }
                            for t in m.targets
                        ],
                    }
                    for m in g.markets
                ],
            }
            for g in compiled_games
        ],
    }
    plan_hash = _json_hash(canonical_payload)

    return CompiledPlan(
        provider=scope.provider,
        league=scope.league,
        run_id=int(scope.run_id),
        plan_hash=plan_hash,
        compiled_at=int(time.time()),
        games=tuple(compiled_games),
    )


def compile_multi_league_plan(
    *,
    db: Any,
    leagues: list[tuple[str, str]],
    run_id: int,
    live_policy: LoadedLiveTradingPolicy | None = None,
    now_ts_utc: int | None = None,
    plan_horizon_hours: int | None = None,
    exclude_strategy_keys: set[str] | None = None,
) -> CompiledPlan:
    """Compile plans for multiple leagues and merge into one.

    Args:
        leagues: List of (league_key, provider_name) pairs.
        Other args are passed through to compile_hotpath_plan per league.

    Skips leagues that have no in-scope games (HotPathPlanError with
    code 'scope_blocked'). Raises only if ALL leagues fail.
    """
    all_games: list[CompiledGamePlan] = []
    seen_game_ids: set[str] = set()
    successes = 0

    for league_key, provider_name in leagues:
        try:
            plan = compile_hotpath_plan(
                db=db,
                provider=provider_name,
                league=league_key,
                run_id=run_id,
                live_policy=live_policy,
                now_ts_utc=now_ts_utc,
                plan_horizon_hours=plan_horizon_hours,
                exclude_strategy_keys=exclude_strategy_keys,
            )
            for game in plan.games:
                if game.provider_game_id not in seen_game_ids:
                    all_games.append(game)
                    seen_game_ids.add(game.provider_game_id)
            successes += 1
        except HotPathPlanError as exc:
            if exc.code == "scope_blocked":
                continue
            raise

    if successes == 0:
        raise HotPathPlanError(
            "scope_blocked",
            f"no in-scope games for any of {len(leagues)} leagues",
        )

    canonical_payload = {
        "games": sorted(
            [
                {
                    "provider_game_id": g.provider_game_id,
                    "markets": sorted(
                        [m.condition_id for m in g.markets],
                    ),
                }
                for g in all_games
            ],
            key=lambda x: x["provider_game_id"],
        ),
    }
    plan_hash = _json_hash(canonical_payload)

    return CompiledPlan(
        provider="multi",
        league="soccer",
        run_id=int(run_id),
        plan_hash=plan_hash,
        compiled_at=int(time.time()),
        games=tuple(all_games),
    )


__all__ = [
    "HotPathPlanError",
    "ScopedLaunchCheck",
    "compile_hotpath_plan",
    "compile_multi_league_plan",
    "evaluate_hotpath_scope",
]
