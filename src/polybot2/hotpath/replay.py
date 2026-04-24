"""Offline hotpath replay for captured provider score streams."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
from typing import Any

from polybot2.hotpath.compiler import compile_hotpath_plan
from polybot2.hotpath.contracts import HotPathConfig
from polybot2.hotpath.mlb import MlbOrderPolicy
from polybot2.hotpath.native_engine import NativeMlbEngineBridge
from polybot2.linking.mapping_loader import LoadedLiveTradingPolicy, load_live_trading_policy
from polybot2.sports.contracts import ScoreUpdateEvent


def _norm(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def _sanitize_component(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    out = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("_")
    sanitized = "".join(out).strip("_")
    return sanitized or "unknown"


def _line_key(value: float | None) -> str:
    if value is None:
        return ""
    text = f"{float(value):.6f}".rstrip("0").rstrip(".")
    return text or "0"


def _extract_unresolved(reason: str) -> int:
    text = str(reason or "")
    marker = "unresolved="
    idx = text.find(marker)
    if idx < 0:
        return 0
    tail = text[idx + len(marker) :]
    digits = []
    for ch in tail:
        if ch.isdigit():
            digits.append(ch)
            continue
        break
    if not digits:
        return 0
    try:
        return int("".join(digits))
    except (TypeError, ValueError):
        return 0


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    provider: str
    league: str
    run_id: int
    capture_manifest: str
    out_dir: str = ""
    universal_ids: tuple[str, ...] = ()
    mode: str = "as_fast"
    speed_multiplier: float = 1.0
    timestamp_mode: str = "captured"
    burst_interval_ms: int = 1
    profiling_enabled: bool = False
    decision_cooldown_seconds: float = 0.5
    decision_debounce_seconds: float = 0.1


@dataclass(frozen=True, slots=True)
class ReplayIntentResult:
    universal_id: str
    strategy_key: str
    condition_id: str
    sports_market_type: str
    outcome_semantic: str
    line: float | None
    source_reason: str
    event_seq: int
    gate_status: str
    correctness: str = "unknown"
    correctness_reason: str = ""


@dataclass(frozen=True, slots=True)
class ReplayEventRow:
    seq: int
    universal_id: str
    received_ts: int
    recv_monotonic_ns: int
    action: str
    material_change: bool
    decision: str
    decision_reason: str
    dropped_unresolved_targets: int
    intents_attempted: tuple[str, ...] = ()
    intents_emitted: tuple[str, ...] = ()
    gate_drops: tuple[str, ...] = ()
    state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    provider: str
    league: str
    run_id: int
    mode: str
    speed_multiplier: float
    capture_manifest: str
    output_dir: str
    timeline_path: str
    summary_path: str
    replay_manifest_path: str
    n_events_total: int
    n_events_material: int
    n_events_non_material: int
    n_intents_attempted: int
    n_intents_emitted: int
    n_drops_cooldown: int
    n_drops_debounce: int
    n_drops_one_shot: int
    n_correct: int
    n_incorrect: int
    n_unknown: int
    per_game: dict[str, Any] = field(default_factory=dict)
    counts_by_market_type: dict[str, dict[str, int]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class _ReplayScoreRow:
    seq_hint: int
    received_ts: int
    source_file: str
    source_line: int
    universal_id: str
    event: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _StrategyMeta:
    strategy_key: str
    condition_id: str
    sports_market_type: str
    outcome_semantic: str
    line: float | None


@dataclass(slots=True)
class _NrfiTruthState:
    saw_inning_signal: bool = False
    first_inning_run_seen: bool = False
    first_inning_complete_seen: bool = False
    total_at_first_inning_complete: int | None = None


@dataclass(frozen=True, slots=True)
class _ReplayDelta:
    goal_delta_home: int = 0
    goal_delta_away: int = 0


def _capture_scores_rows(
    *,
    capture_manifest: Path,
    provider: str,
    selected_ids: set[str],
) -> list[_ReplayScoreRow]:
    payload = json.loads(capture_manifest.read_text(encoding="utf-8"))
    parsed_dir = str(payload.get("parsed_dir") or "").strip()
    if not parsed_dir:
        raise ValueError("capture manifest missing parsed_dir")
    root = Path(parsed_dir).expanduser().resolve()
    scores_root = root / f"provider={_sanitize_component(provider)}" / "stream=scores"
    if not scores_root.exists():
        raise ValueError(f"parsed scores stream directory not found: {scores_root}")

    rows: list[_ReplayScoreRow] = []
    seq = 0
    for jsonl in sorted(scores_root.rglob("*.jsonl"), key=lambda p: str(p)):
        with jsonl.open("r", encoding="utf-8") as f:
            for line_no, raw_line in enumerate(f, start=1):
                line = str(raw_line or "").strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                uid = str(item.get("universal_id") or "").strip()
                if not uid:
                    continue
                if selected_ids and uid not in selected_ids:
                    continue
                event = item.get("event") if isinstance(item.get("event"), dict) else {}
                received_ts = int(item.get("received_ts") or 0)
                rows.append(
                    _ReplayScoreRow(
                        seq_hint=seq,
                        received_ts=received_ts,
                        source_file=str(jsonl),
                        source_line=int(line_no),
                        universal_id=uid,
                        event=dict(event),
                    )
                )
                seq += 1

    rows.sort(key=lambda r: (int(r.received_ts), str(r.source_file), int(r.source_line), int(r.seq_hint)))
    return rows


def _to_score_event(payload: dict[str, Any]) -> ScoreUpdateEvent:
    return ScoreUpdateEvent(
        provider=str(payload.get("provider") or ""),
        universal_id=str(payload.get("universal_id") or ""),
        action=str(payload.get("action") or ""),
        provider_timestamp=str(payload.get("provider_timestamp") or ""),
        game=str(payload.get("game") or ""),
        home_team=str(payload.get("home_team") or ""),
        away_team=str(payload.get("away_team") or ""),
        period=str(payload.get("period") or ""),
        elapsed_time_seconds=payload.get("elapsed_time_seconds"),
        pre_match=payload.get("pre_match"),
        match_completed=payload.get("match_completed"),
        clock_running_now=payload.get("clock_running_now"),
        clock_running=payload.get("clock_running"),
        home_score=payload.get("home_score"),
        away_score=payload.get("away_score"),
        home_corners=payload.get("home_corners"),
        away_corners=payload.get("away_corners"),
        home_yellow_cards=payload.get("home_yellow_cards"),
        away_yellow_cards=payload.get("away_yellow_cards"),
        home_red_cards=payload.get("home_red_cards"),
        away_red_cards=payload.get("away_red_cards"),
        home_first_half_goals=payload.get("home_first_half_goals"),
        away_first_half_goals=payload.get("away_first_half_goals"),
        home_second_half_goals=payload.get("home_second_half_goals"),
        away_second_half_goals=payload.get("away_second_half_goals"),
        var_referral_in_progress=payload.get("var_referral_in_progress"),
        raw_payload=(payload.get("raw_payload") if isinstance(payload.get("raw_payload"), dict) else {}),
    )


def _order_policy_for_league(*, live_policy: LoadedLiveTradingPolicy, league: str) -> MlbOrderPolicy:
    lk = _norm(league)
    cfg = dict((live_policy.hotpath_execution_by_league or {}).get(lk, {}) or {})
    return MlbOrderPolicy(
        amount_usdc=float(cfg.get("amount_usdc", 5.0)),
        limit_price=float(cfg.get("limit_price", 0.52)),
        time_in_force=str(cfg.get("time_in_force", "FAK") or "FAK"),
    )


def _strategy_meta_from_plan(plan: Any) -> dict[str, _StrategyMeta]:
    out: dict[str, _StrategyMeta] = {}
    for game in tuple(plan.games):
        for market in tuple(game.markets):
            for target in tuple(market.targets):
                key = str(target.strategy_key or "").strip()
                if not key:
                    continue
                out[key] = _StrategyMeta(
                    strategy_key=key,
                    condition_id=str(target.condition_id or ""),
                    sports_market_type=str(target.sports_market_type or ""),
                    outcome_semantic=str(target.outcome_semantic or ""),
                    line=(None if target.line is None else float(target.line)),
                )
    return out


def _passes_decision_gates(
    *,
    intent: OrderIntent,
    event_ns: int,
    last_emit_ns: dict[str, int],
    last_signature: dict[str, tuple[Any, ...]],
    cooldown_seconds: float,
    debounce_seconds: float,
) -> str:
    key = str(intent.strategy_key or "").strip()
    if not key:
        return "invalid_strategy"
    last_emit = int(last_emit_ns.get(key, 0))
    cooldown_ns = int(max(0.0, float(cooldown_seconds)) * 1_000_000_000)
    if cooldown_ns > 0 and last_emit > 0 and (event_ns - last_emit) < cooldown_ns:
        return "dropped_cooldown"
    sig = (
        str(intent.strategy_key),
        str(intent.token_id),
        str(intent.side),
        float(intent.amount_usdc),
        float(intent.limit_price),
        str(intent.time_in_force),
        int(intent.expire_ts) if intent.expire_ts is not None else None,
    )
    debounce_ns = int(max(0.0, float(debounce_seconds)) * 1_000_000_000)
    if debounce_ns > 0 and sig == last_signature.get(key) and last_emit > 0 and (event_ns - last_emit) < debounce_ns:
        return "dropped_debounce"
    last_emit_ns[key] = int(event_ns)
    last_signature[key] = sig
    return "emit_candidate"


def _update_nrfi_truth(
    *,
    uid: str,
    delta: Any,
    state: dict[str, Any] | None,
    nrfi_truth: dict[str, _NrfiTruthState],
) -> None:
    if not isinstance(state, dict):
        return
    row = nrfi_truth.get(uid) or _NrfiTruthState()
    inning_num = state.get("inning_number")
    inning_half = _norm(state.get("inning_half"))
    total_now = state.get("total")
    try:
        total_int = None if total_now is None else int(total_now)
    except (TypeError, ValueError):
        total_int = None
    if inning_num is not None or inning_half:
        row.saw_inning_signal = True
    try:
        in1 = int(inning_num) == 1
    except (TypeError, ValueError):
        in1 = False
    goal_delta = int(delta.goal_delta_home or 0) + int(delta.goal_delta_away or 0)
    if in1 and goal_delta > 0:
        row.first_inning_run_seen = True
    first_complete = False
    try:
        if inning_num is not None and int(inning_num) > 1:
            first_complete = True
    except (TypeError, ValueError):
        first_complete = False
    if in1 and inning_half == "end":
        first_complete = True
    if bool(state.get("match_completed")) and in1:
        first_complete = True
    if first_complete:
        row.first_inning_complete_seen = True
        if row.total_at_first_inning_complete is None and total_int is not None:
            row.total_at_first_inning_complete = int(total_int)
    nrfi_truth[uid] = row


def _evaluate_intent_correctness(
    *,
    item: ReplayIntentResult,
    final_state: dict[str, dict[str, Any]],
    nrfi_truth: dict[str, _NrfiTruthState],
) -> tuple[str, str]:
    uid = str(item.universal_id)
    state = final_state.get(uid) or {}
    market_type = _norm(item.sports_market_type)
    semantic = _norm(item.outcome_semantic)
    match_completed = bool(state.get("match_completed"))
    home = state.get("home")
    away = state.get("away")
    total = state.get("total")

    def _final_scores() -> tuple[int | None, int | None]:
        try:
            return (
                (None if home is None else int(home)),
                (None if away is None else int(away)),
            )
        except (TypeError, ValueError):
            return (None, None)

    if item.gate_status != "attempted":
        return ("unknown", "not_attempted")

    if semantic in {"over", "under"}:
        if not match_completed or total is None or item.line is None:
            return ("unknown", "missing_final_total_or_line")
        try:
            final_total = int(total)
        except (TypeError, ValueError):
            return ("unknown", "invalid_final_total")
        if semantic == "over":
            return ("correct", "final_total_gt_line") if float(final_total) > float(item.line) else ("incorrect", "final_total_not_gt_line")
        return ("correct", "final_total_lt_line") if float(final_total) < float(item.line) else ("incorrect", "final_total_not_lt_line")

    if market_type == "nrfi" and semantic in {"yes", "no"}:
        truth = nrfi_truth.get(uid) or _NrfiTruthState()
        if truth.first_inning_run_seen:
            return ("correct", "nrfi_yes_first_inning_run") if semantic == "yes" else ("incorrect", "nrfi_yes_first_inning_run")
        if truth.first_inning_complete_seen:
            if truth.total_at_first_inning_complete is None:
                return ("unknown", "nrfi_first_inning_complete_missing_total")
            if int(truth.total_at_first_inning_complete) == 0:
                return ("correct", "nrfi_no_first_inning_zero") if semantic == "no" else ("incorrect", "nrfi_no_first_inning_zero")
            return ("correct", "nrfi_yes_first_inning_nonzero") if semantic == "yes" else ("incorrect", "nrfi_yes_first_inning_nonzero")
        return ("unknown", "nrfi_partial_missing_first_inning_truth")

    if semantic in {"home", "away"} and market_type == "moneyline":
        if not match_completed:
            return ("unknown", "missing_final_state")
        h, a = _final_scores()
        if h is None or a is None or h == a:
            return ("unknown", "invalid_or_tied_final_score")
        winner = "home" if h > a else "away"
        return ("correct", "moneyline_winner_match") if semantic == winner else ("incorrect", "moneyline_winner_mismatch")

    if semantic in {"home", "away"} and market_type in {"spread", "spreads"}:
        if not match_completed or item.line is None:
            return ("unknown", "missing_final_state_or_line")
        h, a = _final_scores()
        if h is None or a is None:
            return ("unknown", "invalid_final_score")
        margin_home = int(h - a)
        margin_away = -margin_home
        covered = (float(margin_home) > float(item.line)) if semantic == "home" else (float(margin_away) > float(item.line))
        return ("correct", "spread_cover_match") if covered else ("incorrect", "spread_not_covered")

    return ("unknown", "unsupported_market_type")


def _write_timeline(*, path: Path, rows: list[ReplayEventRow]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(asdict(row), sort_keys=True, default=str))
            f.write("\n")


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_bolt_period(period: str) -> tuple[int | None, str]:
    src = str(period or "").strip().upper()
    if not src:
        return (None, "")
    m = re.search(r"(?:TOP|BOT|END)_([0-9]+)(?:ST|ND|RD|TH)?_INNING", src)
    if not m:
        return (None, "")
    inning = _safe_int(m.group(1))
    if "_TOP_" in src:
        return (inning, "top")
    if "_BOT_" in src:
        return (inning, "bottom")
    if "_END_" in src:
        return (inning, "end")
    return (inning, "")


def _state_from_score_event(
    *,
    event: ScoreUpdateEvent,
    previous_state: dict[str, Any] | None,
) -> dict[str, Any]:
    prev = dict(previous_state or {})
    raw_payload = event.raw_payload if isinstance(event.raw_payload, dict) else {}
    provider = _norm(str(event.provider or ""))

    home = _safe_int(event.home_score)
    away = _safe_int(event.away_score)
    if home is None:
        home = _safe_int(prev.get("home"))
    if away is None:
        away = _safe_int(prev.get("away"))
    total = None if home is None or away is None else int(home + away)
    if total is None:
        total = _safe_int(prev.get("total"))

    inning_number = None
    inning_half = ""
    match_completed = bool(event.match_completed) if event.match_completed is not None else bool(prev.get("match_completed", False))
    if provider == "boltodds":
        state = raw_payload.get("state") if isinstance(raw_payload.get("state"), dict) else {}
        inning_number = _safe_int(state.get("inning"))
        top_of_inning = state.get("topOfInning")
        if isinstance(top_of_inning, bool):
            inning_half = "top" if top_of_inning else "bottom"
        from_state = state.get("matchCompleted")
        if isinstance(from_state, bool):
            match_completed = bool(from_state)
        period_inning, period_half = _parse_bolt_period(str(event.period or ""))
        if inning_number is None:
            inning_number = period_inning
        if not inning_half:
            inning_half = period_half
    elif provider == "kalstrop":
        cached = raw_payload.get("_hotpath_baseball")
        if isinstance(cached, dict):
            inning_number = _safe_int(cached.get("inning_number"))
            inning_half = _norm(str(cached.get("inning_half") or ""))
            cached_completed = cached.get("match_completed")
            if isinstance(cached_completed, bool):
                match_completed = bool(cached_completed)

    if inning_number is None:
        inning_number = _safe_int(prev.get("inning_number"))
    if not inning_half:
        inning_half = str(prev.get("inning_half") or "")

    return {
        "home": home,
        "away": away,
        "total": total,
        "inning_number": inning_number,
        "inning_half": inning_half,
        "match_completed": bool(match_completed),
    }


def run_hotpath_replay(
    *,
    db: Any,
    config: ReplayConfig,
    live_policy: LoadedLiveTradingPolicy | None = None,
) -> ReplaySummary:
    provider = _norm(config.provider)
    league = _norm(config.league)
    if league != "mlb":
        raise ValueError("hotpath replay v1 currently supports league=mlb only")
    mode = _norm(config.mode or "as_fast")
    if mode not in {"as_fast", "timed"}:
        raise ValueError("mode must be one of: as_fast,timed")
    speed = float(config.speed_multiplier)
    if mode == "timed" and speed <= 0.0:
        raise ValueError("speed_multiplier must be > 0 for timed mode")
    timestamp_mode = _norm(config.timestamp_mode or "captured")
    if timestamp_mode not in {"captured", "burst"}:
        raise ValueError("timestamp_mode must be one of: captured,burst")
    burst_interval_ms = max(1, int(config.burst_interval_ms))

    capture_manifest = Path(str(config.capture_manifest)).expanduser().resolve()
    if not capture_manifest.exists():
        raise ValueError(f"capture manifest not found: {capture_manifest}")

    policy = live_policy or load_live_trading_policy()
    compiled = compile_hotpath_plan(
        db=db,
        provider=provider,
        league=league,
        run_id=int(config.run_id),
        live_policy=policy,
        require_all_approved=False,
        include_all_scope_games=True,
    )
    plan_game_ids = {str(g.provider_game_id) for g in tuple(compiled.games) if str(g.provider_game_id or "").strip()}
    selected_cfg = {str(x).strip() for x in tuple(config.universal_ids or ()) if str(x).strip()}
    captured_rows = _capture_scores_rows(capture_manifest=capture_manifest, provider=provider, selected_ids=selected_cfg)
    captured_ids = {str(r.universal_id) for r in captured_rows}
    universe = set(plan_game_ids).intersection(set(captured_ids))
    if not universe:
        raise ValueError("no overlapping universal_ids between compiled plan and captured scores")
    rows = [r for r in captured_rows if str(r.universal_id) in universe]

    now_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if str(config.out_dir or "").strip():
        out_dir = Path(str(config.out_dir)).expanduser().resolve()
    else:
        out_dir = capture_manifest.parent / f"replay_{provider}_{league}_{now_tag}"
    out_dir.mkdir(parents=True, exist_ok=True)
    timeline_path = out_dir / "timeline.jsonl"
    summary_path = out_dir / "summary.json"
    replay_manifest_path = out_dir / "replay_manifest.json"

    order_policy = _order_policy_for_league(live_policy=policy, league=league)
    engine = NativeMlbEngineBridge(
        config=HotPathConfig(
            dedup_ttl_seconds=float(HotPathConfig().dedup_ttl_seconds),
            decision_cooldown_seconds=float(config.decision_cooldown_seconds),
            decision_debounce_seconds=float(config.decision_debounce_seconds),
        ),
        order_policy=order_policy,
        required=True,
    )
    engine.load_plan(compiled)
    engine.reset_runtime_state()

    strategy_meta = _strategy_meta_from_plan(compiled)
    timeline: list[ReplayEventRow] = []
    intents: list[ReplayIntentResult] = []
    final_state: dict[str, dict[str, Any]] = {}
    nrfi_truth: dict[str, _NrfiTruthState] = {}
    previous_state_by_uid: dict[str, dict[str, Any]] = {}

    n_material = 0
    n_non_material = 0
    drop_cooldown = 0
    drop_debounce = 0
    drop_one_shot = 0
    last_recv_ns: int | None = None
    burst_interval_ns = int(burst_interval_ms) * 1_000_000
    burst_base_recv_ns: int | None = None

    for seq, row in enumerate(rows, start=1):
        if timestamp_mode == "burst":
            if burst_base_recv_ns is None:
                burst_base_recv_ns = int(row.received_ts) * 1_000_000_000
            recv_ns = int(burst_base_recv_ns + ((int(seq) - 1) * burst_interval_ns))
        else:
            recv_ns = int(row.received_ts) * 1_000_000_000 + int(seq)
        if mode == "timed" and last_recv_ns is not None:
            dt_ns = max(0, int(recv_ns - int(last_recv_ns)))
            if dt_ns > 0:
                time.sleep((float(dt_ns) / 1_000_000_000.0) / float(speed))
        last_recv_ns = int(recv_ns)
        effective_received_ts = int(recv_ns // 1_000_000_000)

        score_event = _to_score_event(dict(row.event))
        uid = str(score_event.universal_id).strip()
        if not uid or uid not in universe:
            continue

        prev_state = previous_state_by_uid.get(uid)
        state = _state_from_score_event(event=score_event, previous_state=prev_state)
        previous_state_by_uid[uid] = dict(state)
        final_state[uid] = dict(state)
        prev_home = _safe_int((prev_state or {}).get("home"))
        prev_away = _safe_int((prev_state or {}).get("away"))
        cur_home = _safe_int(state.get("home"))
        cur_away = _safe_int(state.get("away"))
        delta = _ReplayDelta(
            goal_delta_home=(
                int(cur_home - prev_home)
                if cur_home is not None and prev_home is not None
                else 0
            ),
            goal_delta_away=(
                int(cur_away - prev_away)
                if cur_away is not None and prev_away is not None
                else 0
            ),
        )
        _update_nrfi_truth(uid=uid, delta=delta, state=state, nrfi_truth=nrfi_truth)

        decision_started_ns = time.perf_counter_ns()
        native_out = engine.process_score_event(score_event, recv_monotonic_ns=int(recv_ns))
        _ = max(0, int(time.perf_counter_ns() - decision_started_ns))
        unresolved = _extract_unresolved(str(native_out.get("reason") or ""))
        intents_all = tuple(native_out.get("intents") or ())

        attempted_keys: list[str] = []
        emitted_keys: list[str] = []
        gate_drops: list[str] = []
        route_started_ns = time.perf_counter_ns()
        for intent in intents_all:
            intent_dict = dict(intent or {}) if isinstance(intent, dict) else {}
            strategy_key = str(intent_dict.get("strategy_key") or "").strip()
            if not strategy_key:
                continue
            meta = strategy_meta.get(strategy_key)
            attempted_keys.append(strategy_key)
            emitted_keys.append(strategy_key)
            intents.append(
                ReplayIntentResult(
                    universal_id=uid,
                    strategy_key=str(strategy_key),
                    condition_id=("" if meta is None else str(meta.condition_id)),
                    sports_market_type=("" if meta is None else str(meta.sports_market_type)),
                    outcome_semantic=("" if meta is None else str(meta.outcome_semantic)),
                    line=(None if meta is None else meta.line),
                    source_reason=str(intent_dict.get("reason") or ""),
                    event_seq=int(seq),
                    gate_status="attempted",
                )
            )
        _ = int(max(0, time.perf_counter_ns() - route_started_ns))

        msg_drop_cooldown = int(native_out.get("drops_cooldown") or 0)
        msg_drop_debounce = int(native_out.get("drops_debounce") or 0)
        msg_drop_one_shot = int(native_out.get("drops_one_shot") or 0)
        drop_cooldown += msg_drop_cooldown
        drop_debounce += msg_drop_debounce
        drop_one_shot += msg_drop_one_shot
        if msg_drop_cooldown > 0:
            gate_drops.append("cooldown")
        if msg_drop_debounce > 0:
            gate_drops.append("debounce")
        if msg_drop_one_shot > 0:
            gate_drops.append("one_shot")
        material_change = int(native_out.get("decision_non_material") or 0) <= 0
        if material_change:
            n_material += 1
        else:
            n_non_material += 1

        timeline.append(
            ReplayEventRow(
                seq=int(seq),
                universal_id=uid,
                received_ts=int(effective_received_ts),
                recv_monotonic_ns=int(recv_ns),
                action=str(score_event.action),
                material_change=bool(material_change),
                decision=str(native_out.get("decision") or "no_action"),
                decision_reason=str(native_out.get("reason") or ""),
                dropped_unresolved_targets=int(unresolved),
                intents_attempted=tuple(attempted_keys),
                intents_emitted=tuple(emitted_keys),
                gate_drops=tuple(gate_drops),
                state=dict(state),
            )
        )

    evaluated_intents: list[ReplayIntentResult] = []
    n_correct = 0
    n_incorrect = 0
    n_unknown = 0
    by_market: dict[str, dict[str, int]] = {}
    per_game: dict[str, dict[str, Any]] = {}
    for item in intents:
        correctness, reason = _evaluate_intent_correctness(item=item, final_state=final_state, nrfi_truth=nrfi_truth)
        if correctness == "correct":
            n_correct += 1
        elif correctness == "incorrect":
            n_incorrect += 1
        else:
            n_unknown += 1
        mt = _norm(item.sports_market_type) or "unknown"
        bucket = by_market.setdefault(mt, {"attempted": 0, "correct": 0, "incorrect": 0, "unknown": 0})
        if str(item.gate_status) == "attempted":
            bucket["attempted"] = int(bucket.get("attempted", 0)) + 1
        bucket[str(correctness)] = int(bucket.get(str(correctness), 0)) + 1

        game_bucket = per_game.setdefault(
            str(item.universal_id),
            {
                "attempted": 0,
                "correct": 0,
                "incorrect": 0,
                "unknown": 0,
                "drops_cooldown": 0,
                "drops_debounce": 0,
                "drops_one_shot": 0,
                "final_state": dict(final_state.get(str(item.universal_id), {})),
            },
        )
        if str(item.gate_status) == "attempted":
            game_bucket["attempted"] = int(game_bucket.get("attempted", 0)) + 1
        if str(item.gate_status) == "dropped_cooldown":
            game_bucket["drops_cooldown"] = int(game_bucket.get("drops_cooldown", 0)) + 1
        if str(item.gate_status) == "dropped_debounce":
            game_bucket["drops_debounce"] = int(game_bucket.get("drops_debounce", 0)) + 1
        if str(item.gate_status) == "dropped_one_shot":
            game_bucket["drops_one_shot"] = int(game_bucket.get("drops_one_shot", 0)) + 1
        game_bucket[str(correctness)] = int(game_bucket.get(str(correctness), 0)) + 1

        evaluated_intents.append(
            ReplayIntentResult(
                universal_id=item.universal_id,
                strategy_key=item.strategy_key,
                condition_id=item.condition_id,
                sports_market_type=item.sports_market_type,
                outcome_semantic=item.outcome_semantic,
                line=item.line,
                source_reason=item.source_reason,
                event_seq=item.event_seq,
                gate_status=item.gate_status,
                correctness=correctness,
                correctness_reason=reason,
            )
        )

    _write_timeline(path=timeline_path, rows=timeline)
    summary_payload = {
        "provider": provider,
        "league": league,
        "run_id": int(config.run_id),
        "mode": mode,
        "speed_multiplier": float(speed),
        "capture_manifest": str(capture_manifest),
        "output_dir": str(out_dir),
        "counts": {
            "events_total": int(len(timeline)),
            "events_material": int(n_material),
            "events_non_material": int(n_non_material),
            "intents_total": int(len(evaluated_intents)),
            "intents_attempted": int(sum(1 for x in evaluated_intents if str(x.gate_status) == "attempted")),
            "drops_cooldown": int(drop_cooldown),
            "drops_debounce": int(drop_debounce),
            "drops_one_shot": int(drop_one_shot),
            "correct": int(n_correct),
            "incorrect": int(n_incorrect),
            "unknown": int(n_unknown),
        },
        "counts_by_market_type": by_market,
        "universe": sorted(universe),
        "per_game": per_game,
        "timeline_path": str(timeline_path),
        "evaluated_intents": [asdict(x) for x in evaluated_intents],
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")

    replay_manifest_payload = {
        "provider": provider,
        "league": league,
        "run_id": int(config.run_id),
        "mode": mode,
        "speed_multiplier": float(speed),
        "timestamp_mode": str(timestamp_mode),
        "burst_interval_ms": int(burst_interval_ms),
        "capture_manifest": str(capture_manifest),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "paths": {
            "timeline": str(timeline_path),
            "summary": str(summary_path),
        },
        "filters": {
            "requested_universal_ids": sorted(selected_cfg),
            "effective_universe": sorted(universe),
        },
    }
    replay_manifest_path.write_text(
        json.dumps(replay_manifest_payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )

    return ReplaySummary(
        provider=provider,
        league=league,
        run_id=int(config.run_id),
        mode=mode,
        speed_multiplier=float(speed),
        capture_manifest=str(capture_manifest),
        output_dir=str(out_dir),
        timeline_path=str(timeline_path),
        summary_path=str(summary_path),
        replay_manifest_path=str(replay_manifest_path),
        n_events_total=int(len(timeline)),
        n_events_material=int(n_material),
        n_events_non_material=int(n_non_material),
        n_intents_attempted=int(sum(1 for x in evaluated_intents if str(x.gate_status) == "attempted")),
        n_intents_emitted=int(sum(1 for x in evaluated_intents if str(x.gate_status) == "attempted")),
        n_drops_cooldown=int(drop_cooldown),
        n_drops_debounce=int(drop_debounce),
        n_drops_one_shot=int(drop_one_shot),
        n_correct=int(n_correct),
        n_incorrect=int(n_incorrect),
        n_unknown=int(n_unknown),
        per_game=per_game,
        counts_by_market_type=by_market,
    )


__all__ = [
    "ReplayConfig",
    "ReplayEventRow",
    "ReplayIntentResult",
    "ReplaySummary",
    "run_hotpath_replay",
]
