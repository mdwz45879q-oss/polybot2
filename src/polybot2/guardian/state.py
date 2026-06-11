"""In-memory state model for order tracking.

Tracks score timelines, order fill states, and causal mappings
between score changes and the orders they triggered.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ScoreEvent:
    """A single score change observed from the hotpath log."""
    ts: int  # milliseconds since epoch
    home: int
    away: int
    half: str
    game_state: str
    prev_home: int | None = None
    prev_away: int | None = None
    var_type: str = ""     # V2 match action: "Var", "VarEnded", "DangerStateChanged", etc.
    var_subtype: str = ""  # V2 match action: "Goal", "GoalAwarded", "NoGoal", "Penalty", "NoPenalty", etc.


@dataclass
class TrackedOrder:
    """An order from the hotpath log, enriched with CLOB fill data."""
    ts: int  # milliseconds since epoch (from log)
    strategy_key: str  # e.g., "gid:TOTAL:OVER:1.5"
    token_id: str
    exchange_id: str  # hex order ID returned by CLOB (empty if failed)
    condition_id: str  # market condition ID (for WS subscription)
    time_in_force: str  # "FAK" or "GTC" (derived from order policy)
    ok: bool  # accepted by CLOB
    error: str = ""  # error message if not ok

    # Causal mapping: the score change that triggered this order
    triggered_by: ScoreEvent | None = None

    # Filled by CLOB query (None until queried):
    fill_amount: float | None = None  # filled size in shares
    fill_price: float | None = None  # average fill price
    order_status: str = ""  # "MATCHED", "OPEN", "CANCELLED", etc.
    clob_queried: bool = False  # whether we've queried CLOB for this order


@dataclass
class GameState:
    """All tracked state for a single game."""
    game_id: str
    score_timeline: list[ScoreEvent] = field(default_factory=list)
    orders: list[TrackedOrder] = field(default_factory=list)
    current_home: int = 0
    current_away: int = 0
    current_half: str = ""
    current_game_state: str = ""


@dataclass
class OverturnAlert:
    """An active overturn alert for a game where a score reversal was detected."""
    game_id: str
    # The original goal that was scored (the score change we traded on)
    original_score_event: ScoreEvent
    # When the reversal was first detected
    reversal_ts: int  # milliseconds since epoch
    # The score after reversal (e.g., back to 1-0 from 2-0)
    reversed_home: int
    reversed_away: int
    # Dual-signal confirmation
    signal1_confirmed: bool = False  # score held reversed for >N seconds
    signal2_confirmed: bool = False  # market bid dropped below threshold
    acted: bool = False  # sell/cancel already triggered
    execution_in_progress: bool = False  # async execution task is running
    # Orders triggered by the now-reversed goal
    affected_orders: list[TrackedOrder] = field(default_factory=list)
    # Token IDs from affected orders (for market WS monitoring)
    affected_token_ids: set[str] = field(default_factory=set)


@dataclass
class TrackerState:
    """Top-level tracker state across all games."""
    games: dict[str, GameState] = field(default_factory=dict)
    # Exchange IDs that need CLOB fill queries
    pending_fak_queries: list[str] = field(default_factory=list)
    pending_gtc_ids: set[str] = field(default_factory=set)
    # Map exchange_id → TrackedOrder for quick lookup
    orders_by_eid: dict[str, TrackedOrder] = field(default_factory=dict)

    def get_or_create_game(self, game_id: str) -> GameState:
        if game_id not in self.games:
            self.games[game_id] = GameState(game_id=game_id)
        return self.games[game_id]
