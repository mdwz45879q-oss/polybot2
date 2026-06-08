"""Tests for guardian overturn detection — C1, C2, C3 fixes.

Covers:
- C1: Execution retry after failure
- C2: Goal event matching with intervening opposing goals
- C3: VAR signal instant-confirm and disarm
"""

import asyncio
import time
from unittest.mock import AsyncMock

import pytest

from polybot2.guardian.overturn import OverturnDetector
from polybot2.guardian.state import GameState, OverturnAlert, ScoreEvent, TrackedOrder


def _make_game(game_id: str = "g1") -> GameState:
    return GameState(game_id=game_id)


def _make_order(
    strategy_key: str = "g1:TOTAL:OVER:1.5",
    token_id: str = "tok_abc",
    exchange_id: str = "eid_123",
    triggered_by: ScoreEvent | None = None,
) -> TrackedOrder:
    return TrackedOrder(
        ts=1000,
        strategy_key=strategy_key,
        token_id=token_id,
        exchange_id=exchange_id,
        condition_id="cond_1",
        time_in_force="FAK",
        ok=True,
        triggered_by=triggered_by,
    )


def _arm_simple_alert(detector: OverturnDetector, game: GameState) -> OverturnAlert:
    """Simulate 0-0 → 1-0 → 0-0: arm an alert from a home goal reversal."""
    # Record the goal event (0-0 → 1-0)
    goal_event = ScoreEvent(
        ts=1000, home=1, away=0, half="1H", game_state="LIVE",
        prev_home=0, prev_away=0,
    )
    game.score_timeline.append(goal_event)
    game.current_home = 1
    game.current_away = 0

    # Add an order triggered by that goal
    order = _make_order(triggered_by=goal_event)
    game.orders.append(order)

    # Score reversal: 1-0 → 0-0
    alert = detector.on_score_change(game, 1, 0, 0, 0, ts=2000)
    assert alert is not None
    assert alert.game_id == game.game_id
    assert not alert.signal1_confirmed
    assert not alert.signal2_confirmed
    return alert


# ── C1: Execution retry ─────────────────────────────────────────────


def test_c1_execution_retry_after_failure():
    """After execution failure (acted=False, both signals True), check_confirmations retries."""
    call_count = 0

    def sync_callback(alert):
        nonlocal call_count
        call_count += 1

    detector = OverturnDetector(
        confirmation_window_s=0.0,  # instant Signal 1
        on_overturn_triggered=sync_callback,
    )
    game = _make_game()
    alert = _arm_simple_alert(detector, game)

    # Confirm Signal 2
    detector._token_to_game["tok_abc"] = game.game_id
    detector.on_best_bid_ask({"asset_id": "tok_abc", "best_bid": "0.10"})
    assert alert.signal2_confirmed

    # First trigger via check_confirmations (Signal 1 timer fires)
    triggered = detector.check_confirmations()
    assert call_count == 1
    assert alert.acted  # sync callback marks acted

    # Simulate execution failure: reset acted but keep signals confirmed
    alert.acted = False

    # Retry: both signals True, acted=False → should re-trigger
    triggered2 = detector.check_confirmations()
    assert call_count == 2  # retried
    assert alert.acted


# ── C2: Goal event matching ─────────────────────────────────────────


def test_c2_find_original_goal_simple_reversal():
    """0-0 → 1-0 → 0-0: finds the 0-0→1-0 event."""
    detector = OverturnDetector()
    game = _make_game()
    game.score_timeline.append(ScoreEvent(
        ts=1000, home=1, away=0, half="1H", game_state="LIVE",
        prev_home=0, prev_away=0,
    ))

    result = detector._find_original_goal_event(game, 1, 0, 0, 0)
    assert result is not None
    assert result.home == 1 and result.prev_home == 0


def test_c2_find_original_goal_with_intervening_goal():
    """0-0 → 1-0 → 1-1 → 0-1: finds the 0-0→1-0 event (home reversed)."""
    detector = OverturnDetector()
    game = _make_game()
    # Home scores
    game.score_timeline.append(ScoreEvent(
        ts=1000, home=1, away=0, half="1H", game_state="LIVE",
        prev_home=0, prev_away=0,
    ))
    # Away scores
    game.score_timeline.append(ScoreEvent(
        ts=2000, home=1, away=1, half="1H", game_state="LIVE",
        prev_home=1, prev_away=0,
    ))

    # Reversal: 1-1 → 0-1 (home goal overturned)
    result = detector._find_original_goal_event(game, 1, 1, 0, 1)
    assert result is not None
    assert result.home == 1 and result.prev_home == 0
    assert result.ts == 1000


def test_c2_find_second_goal_reversed():
    """0-0 → 1-0 → 2-0 → 1-0: finds the 1-0→2-0 event (second goal)."""
    detector = OverturnDetector()
    game = _make_game()
    game.score_timeline.append(ScoreEvent(
        ts=1000, home=1, away=0, half="1H", game_state="LIVE",
        prev_home=0, prev_away=0,
    ))
    game.score_timeline.append(ScoreEvent(
        ts=2000, home=2, away=0, half="1H", game_state="LIVE",
        prev_home=1, prev_away=0,
    ))

    # Reversal: 2-0 → 1-0
    result = detector._find_original_goal_event(game, 2, 0, 1, 0)
    assert result is not None
    assert result.home == 2 and result.prev_home == 1
    assert result.ts == 2000


# ── C3: VAR instant-confirm ─────────────────────────────────────────


def test_c3_var_nogoal_instant_confirms_signal1():
    """VarEnded:NoGoal with armed alert → Signal 1 instantly confirmed."""
    detector = OverturnDetector(confirmation_window_s=10.0)
    game = _make_game()
    alert = _arm_simple_alert(detector, game)

    assert not alert.signal1_confirmed

    # VAR overturn signal (arrives with or after score reversal)
    detector.on_var_action(game, "VarEnded", "NoGoal", ts=2001)

    assert alert.signal1_confirmed
    # Signal 2 not yet confirmed → no execution yet
    assert not alert.acted


def test_c3_var_nopenalty_instant_confirms_signal1():
    """VarEnded:NoPenalty with armed alert → Signal 1 instantly confirmed."""
    detector = OverturnDetector(confirmation_window_s=10.0)
    game = _make_game()
    alert = _arm_simple_alert(detector, game)

    detector.on_var_action(game, "VarEnded", "NoPenalty", ts=2001)

    assert alert.signal1_confirmed


def test_c3_var_nogoal_with_signal2_triggers_execution():
    """VarEnded:NoGoal + Signal 2 already confirmed → immediate execution."""
    executed = []

    def sync_callback(alert):
        executed.append(alert.game_id)

    detector = OverturnDetector(
        confirmation_window_s=10.0,
        on_overturn_triggered=sync_callback,
    )
    game = _make_game()
    alert = _arm_simple_alert(detector, game)

    # Confirm Signal 2 first (market bid dropped)
    detector._token_to_game["tok_abc"] = game.game_id
    detector.on_best_bid_ask({"asset_id": "tok_abc", "best_bid": "0.10"})
    assert alert.signal2_confirmed
    # Signal 1 not yet → no execution
    assert not executed

    # VAR signal instant-confirms Signal 1 → execution fires
    detector.on_var_action(game, "VarEnded", "NoGoal", ts=2001)

    assert alert.signal1_confirmed
    assert alert.acted
    assert executed == [game.game_id]


def test_c3_var_goalawarded_disarms_alert():
    """VarEnded:GoalAwarded with armed alert → alert disarmed (wobble)."""
    detector = OverturnDetector(confirmation_window_s=10.0)
    game = _make_game()
    alert = _arm_simple_alert(detector, game)
    assert game.game_id in detector.alerts

    # VAR confirms the goal — the score reversal was a data wobble
    detector.on_var_action(game, "VarEnded", "GoalAwarded", ts=2001)

    assert game.game_id not in detector.alerts


def test_c3_var_penaltyawarded_disarms_alert():
    """VarEnded:PenaltyAwarded with armed alert → alert disarmed."""
    detector = OverturnDetector(confirmation_window_s=10.0)
    game = _make_game()
    alert = _arm_simple_alert(detector, game)

    detector.on_var_action(game, "VarEnded", "PenaltyAwarded", ts=2001)

    assert game.game_id not in detector.alerts


def test_c3_no_var_fallback_timer():
    """Without VAR signal, Signal 1 still confirms via the 10s timer."""
    detector = OverturnDetector(confirmation_window_s=0.0)  # instant for test
    game = _make_game()
    alert = _arm_simple_alert(detector, game)

    # No on_var_action call — rely on timer
    triggered = detector.check_confirmations()

    assert alert.signal1_confirmed
    assert len(triggered) == 0  # Signal 2 not confirmed yet, so not triggered


def test_c3_old_subtype_goalnotawarded_does_not_trigger():
    """The old (incorrect) subtype 'GoalNotAwarded' should NOT instant-confirm."""
    detector = OverturnDetector(confirmation_window_s=10.0)
    game = _make_game()
    alert = _arm_simple_alert(detector, game)

    # This was the old assumed subtype — it's not a real BetGenius value
    detector.on_var_action(game, "VarEnded", "GoalNotAwarded", ts=2001)

    assert not alert.signal1_confirmed  # should NOT have been confirmed


def test_c3_var_without_armed_alert_is_noop():
    """VarEnded:NoGoal without an armed alert → logged but no crash."""
    detector = OverturnDetector()
    game = _make_game()

    # No armed alert — should not crash
    detector.on_var_action(game, "VarEnded", "NoGoal", ts=1000)
    assert len(detector.alerts) == 0


def test_c3_var_review_started_logged():
    """Var:Goal and Var:Penalty log warnings but don't change state."""
    detector = OverturnDetector()
    game = _make_game()
    alert = None

    # These are early warnings, not triggers
    detector.on_var_action(game, "Var", "Goal", ts=1000)
    detector.on_var_action(game, "Var", "Penalty", ts=1001)

    assert len(detector.alerts) == 0


# ── Wobble disarm (sanity check) ────────────────────────────────────


def test_wobble_disarm_unaffected_by_c2():
    """Score restore (wobble) still disarms alert — C2 fix doesn't break this."""
    detector = OverturnDetector()
    game = _make_game()
    _arm_simple_alert(detector, game)
    assert game.game_id in detector.alerts

    # Score restored to original (wobble)
    detector.on_score_change(game, 0, 0, 1, 0, ts=3000)

    assert game.game_id not in detector.alerts
