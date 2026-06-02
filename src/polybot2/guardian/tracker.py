"""Order state tracker: tails hotpath log, receives fills via Polymarket WS.

Associates orders with the score changes that triggered them,
receives real-time fill notifications via the Polymarket user WebSocket,
and maintains an in-memory model of our current inventory per game.
Falls back to REST polling when WS is unavailable.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from polybot2.guardian.clob_client import ClobClient
from polybot2.guardian.log_tailer import tail_log, tail_log_async, read_log_snapshot
from polybot2.guardian.market_ws import PolymarketMarketWS
from polybot2.guardian.overturn import OverturnDetector
from polybot2.guardian.polymarket_ws import PolymarketUserWS
from polybot2.guardian.state import (
    GameState,
    OverturnAlert,
    ScoreEvent,
    TrackedOrder,
    TrackerState,
)

logger = logging.getLogger("polybot2.guardian")

# How often to re-query GTC orders that are still OPEN (seconds) — REST fallback
GTC_POLL_INTERVAL_S = 10.0


class OrderStateTracker:
    """Tracks order fill state by tailing the hotpath log and receiving WS events."""

    def __init__(
        self,
        log_path: str,
        clob: ClobClient | None = None,
        ws: PolymarketUserWS | None = None,
        market_ws: PolymarketMarketWS | None = None,
        detector: OverturnDetector | None = None,
        *,
        order_policy_config: dict[str, Any] | None = None,
        token_to_condition: dict[str, str] | None = None,
        game_id_map: dict[str, str] | None = None,
        startup_token_ids: list[str] | None = None,
        guardian_logger: Any = None,
        session_header: dict[str, Any] | None = None,
    ):
        self.log_path = log_path
        self.clob = clob
        self.ws = ws
        self.market_ws = market_ws
        self.detector = detector
        self.glog = guardian_logger
        self._session_header = session_header
        self.state = TrackerState()
        self._prev_scores: dict[str, tuple[int, int]] = {}
        self._last_gtc_poll: float = 0.0
        self._order_policy = order_policy_config or {}
        self._token_to_condition = token_to_condition or {}
        self._game_id_map: dict[str, str] = game_id_map or {}
        self._startup_token_ids: set[str] = set(startup_token_ids or [])
        self._subscribed_conditions: set[str] = set()
        self._subscribed_market_tokens: set[str] = set()
        self._on_update_callback: Any = None
        self._on_overturn_callback: Any = None
        self._pending_ws_subscribe: set[str] = set()
        self._stop_requested = False

    def set_on_update(self, callback: Any) -> None:
        """Set a callback invoked after each event is processed."""
        self._on_update_callback = callback

    def update_mappings(
        self,
        token_to_condition: dict[str, str],
        game_id_map: dict[str, str],
    ) -> None:
        """Merge new mappings into the tracker (thread-safe for CPython GIL)."""
        self._token_to_condition.update(token_to_condition)
        self._game_id_map.update(game_id_map)

    def request_stop(self) -> None:
        """Signal the run_watch loop to stop."""
        self._stop_requested = True

    def _extract_game_id_from_sk(self, strategy_key: str) -> str:
        """Extract game_id from strategy key like 'gid:MARKET_TYPE:SIDE:LINE'."""
        parts = strategy_key.split(":")
        return parts[0] if parts else ""

    def _extract_market_type_from_sk(self, strategy_key: str) -> str:
        """Extract market type from strategy key like 'gid:TOTAL:OVER:1.5'."""
        parts = strategy_key.split(":")
        return parts[1] if len(parts) > 1 else ""

    def _determine_tif(self, strategy_key: str) -> str:
        """Determine FAK or GTC from order policy config."""
        market_type = self._extract_market_type_from_sk(strategy_key).lower()
        overrides = self._order_policy.get("market_overrides", {})
        if market_type in overrides:
            return str(overrides[market_type].get("time_in_force", "")).upper() or "FAK"
        return str(self._order_policy.get("time_in_force", "FAK")).upper()

    def _resolve_condition_id(self, token_id: str) -> str:
        """Look up condition_id for a token_id from the compiled plan mapping."""
        return self._token_to_condition.get(token_id, "")

    # ---- Log event handlers ----

    def _on_tick(self, ev: dict[str, Any]) -> None:
        """Process a tick event from the log."""
        gid = str(ev.get("gid", ""))
        if not gid:
            return
        # Normalize alternate provider ID → canonical game ID (strategy key prefix)
        gid = self._game_id_map.get(gid, gid)
        # V2: sport-specific score fields; V1 fallback: h/a
        home = ev.get("goals_home", ev.get("h"))
        away = ev.get("goals_away", ev.get("a"))
        half = str(ev.get("half", ""))
        gs = str(ev.get("gs", ""))
        ts = int(ev.get("ts", 0))
        var_type = str(ev.get("var_type", ""))
        var_subtype = str(ev.get("var_sub", ""))

        if home is None or away is None:
            return

        home = int(home)
        away = int(away)

        game = self.state.get_or_create_game(gid)
        game.current_home = home
        game.current_away = away
        game.current_half = half
        game.current_game_state = gs

        prev = self._prev_scores.get(gid)
        if prev is None or prev != (home, away):
            prev_home, prev_away = prev if prev else (0, 0)
            score_event = ScoreEvent(
                ts=ts, home=home, away=away, half=half, game_state=gs,
                prev_home=prev_home, prev_away=prev_away,
                var_type=var_type, var_subtype=var_subtype,
            )
            game.score_timeline.append(score_event)
            self._prev_scores[gid] = (home, away)

            # Log score change with prices
            if prev is not None and self.glog:
                self.glog.log_score_change(
                    gid, home, away, prev_home, prev_away,
                    half, var_type, var_subtype, self._get_prices(),
                )

            # Overturn detection: check for score reversal
            if self.detector and prev is not None:
                self.detector.on_score_change(
                    game, prev_home, prev_away, home, away, ts,
                )

        # Notify detector of VAR events (even without score change)
        if var_type and self.detector:
            self.detector.on_var_action(game, var_type, var_subtype, ts)
        if var_type and self.glog:
            self.glog.log_var_action(gid, var_type, var_subtype)

    def _on_order(self, ev: dict[str, Any]) -> None:
        """Process an order event from the log."""
        sk = str(ev.get("sk", ""))
        tok = str(ev.get("tok", ""))
        ok = bool(ev.get("ok", False))
        eid = str(ev.get("eid", ""))
        err = str(ev.get("err", ""))
        ts = int(ev.get("ts", 0))

        if not sk:
            return

        # V2 logs include gid directly; fall back to extracting from sk
        gid = str(ev.get("gid", "")) or self._extract_game_id_from_sk(sk)
        tif = str(ev.get("tif", "")).upper() or self._determine_tif(sk)
        cid = self._resolve_condition_id(tok)

        game = self.state.get_or_create_game(gid)
        triggered_by: ScoreEvent | None = None
        for score_ev in reversed(game.score_timeline):
            if score_ev.ts <= ts:
                triggered_by = score_ev
                break

        order = TrackedOrder(
            ts=ts, strategy_key=sk, token_id=tok, exchange_id=eid,
            condition_id=cid, time_in_force=tif, ok=ok, error=err,
            triggered_by=triggered_by,
        )
        game.orders.append(order)

        # Log the order attempt with its trigger context
        if self.glog:
            trigger_score = (triggered_by.home, triggered_by.away) if triggered_by else None
            trigger_prev = (triggered_by.prev_home, triggered_by.prev_away) if triggered_by and triggered_by.prev_home is not None else None
            self.glog.log_order_attempted(gid, sk, tok, eid, ok, tif, trigger_score, trigger_prev)

        if ok and eid and eid != "noop":
            self.state.orders_by_eid[eid] = order
            # Queue for WS subscription if we have a condition_id
            if cid and cid not in self._subscribed_conditions:
                self._pending_ws_subscribe.add(cid)
            # Queue for REST fallback
            if tif == "FAK":
                self.state.pending_fak_queries.append(eid)
            else:
                self.state.pending_gtc_ids.add(eid)

            # Proactive market WS subscription for overturn-relevant tokens
            # (Over and BTTS YES are the only markets bought mid-game on goals)
            if tok and self.market_ws:
                sk_upper = sk.upper()
                is_overturn_relevant = (
                    ":TOTAL:OVER:" in sk_upper
                    or ":BTTS:YES" in sk_upper
                    or ":TOTAL_CORNERS:OVER:" in sk_upper
                )
                if is_overturn_relevant:
                    new_tokens = {tok} - self._subscribed_market_tokens
                    if new_tokens:
                        self._subscribed_market_tokens.update(new_tokens)
                        asyncio.create_task(
                            self.market_ws.subscribe(list(new_tokens))
                        )

    # ---- WebSocket event handlers ----

    def _on_ws_trade(self, data: dict[str, Any]) -> None:
        """Handle a trade event from the Polymarket user WS."""
        asset_id = str(data.get("asset_id", ""))
        size = data.get("size")
        price = data.get("price")
        status = str(data.get("status", ""))
        taker_order_id = str(data.get("taker_order_id", ""))

        # Try to match by taker_order_id (our order ID)
        order = self.state.orders_by_eid.get(taker_order_id)
        if order:
            if size is not None:
                try:
                    order.fill_amount = float(size)
                except (TypeError, ValueError):
                    pass
            if price is not None:
                try:
                    order.fill_price = float(price)
                except (TypeError, ValueError):
                    pass
            if status:
                order.order_status = status
            order.clob_queried = True
            # Remove from REST polling queues
            self.state.pending_gtc_ids.discard(taker_order_id)
            logger.debug("WS trade: eid=%s size=%s price=%s status=%s", taker_order_id, size, price, status)
            # Log the fill
            if self.glog and order.fill_amount and order.fill_amount > 0:
                self.glog.log_order_filled(
                    order.token_id, taker_order_id,
                    order.fill_price or 0.0, order.fill_amount, status,
                )

    def _on_ws_order(self, data: dict[str, Any]) -> None:
        """Handle an order event from the Polymarket user WS."""
        order_id = str(data.get("id", ""))
        order_type = str(data.get("type", ""))
        size_matched = data.get("size_matched")

        order = self.state.orders_by_eid.get(order_id)
        if not order:
            return

        if order_type == "CANCELLATION":
            order.order_status = "CANCELLED"
            order.clob_queried = True
            self.state.pending_gtc_ids.discard(order_id)
            logger.debug("WS order cancelled: eid=%s", order_id)
        elif order_type == "UPDATE":
            if size_matched is not None:
                try:
                    order.fill_amount = float(size_matched)
                except (TypeError, ValueError):
                    pass
            price = data.get("price")
            if price is not None:
                try:
                    order.fill_price = float(price)
                except (TypeError, ValueError):
                    pass
            order.clob_queried = True
            logger.debug("WS order update: eid=%s size_matched=%s", order_id, size_matched)

    # ---- REST fallback ----

    async def _query_pending_fak_orders(self) -> None:
        """Query CLOB REST for FAK orders (fallback when WS missed them)."""
        if not self.clob or not self.state.pending_fak_queries:
            return

        batch = list(self.state.pending_fak_queries)
        self.state.pending_fak_queries.clear()

        for eid in batch:
            order = self.state.orders_by_eid.get(eid)
            if not order or order.clob_queried:
                continue
            resp = await self.clob.get_order(eid)
            if resp:
                _apply_clob_response(order, resp)
            order.clob_queried = True

    async def _poll_gtc_orders(self) -> None:
        """Re-query CLOB REST for GTC orders still OPEN (fallback)."""
        if not self.clob or not self.state.pending_gtc_ids:
            return

        now = time.time()
        if now - self._last_gtc_poll < GTC_POLL_INTERVAL_S:
            return
        self._last_gtc_poll = now

        resolved: list[str] = []
        for eid in list(self.state.pending_gtc_ids):
            order = self.state.orders_by_eid.get(eid)
            if not order:
                resolved.append(eid)
                continue
            if order.clob_queried:
                # Already updated by WS
                status = order.order_status.upper()
                if status in ("MATCHED", "CANCELLED", "EXPIRED", "CONFIRMED"):
                    resolved.append(eid)
                continue
            resp = await self.clob.get_order(eid)
            if resp:
                _apply_clob_response(order, resp)
                order.clob_queried = True
                status = order.order_status.upper()
                if status in ("MATCHED", "CANCELLED", "EXPIRED", "CONFIRMED"):
                    resolved.append(eid)

        for eid in resolved:
            self.state.pending_gtc_ids.discard(eid)

    # ---- Main loops ----

    def process_event(self, ev: dict[str, Any]) -> None:
        """Process a single log event (synchronous, no CLOB queries)."""
        ev_type = ev.get("ev", "")
        if ev_type == "tick":
            self._on_tick(ev)
        elif ev_type == "order":
            self._on_order(ev)

    async def _subscribe_pending_conditions(self) -> None:
        """Subscribe new condition IDs on the WS."""
        if not self.ws or not self._pending_ws_subscribe:
            return
        new_ids = list(self._pending_ws_subscribe)
        self._pending_ws_subscribe.clear()
        await self.ws.subscribe(new_ids)
        self._subscribed_conditions.update(new_ids)

    def _get_prices(self) -> dict[str, dict[str, float]]:
        """Build a prices dict from detector's best bids/asks."""
        if not self.detector:
            return {}
        prices: dict[str, dict[str, float]] = {}
        for tok, bid in self.detector._best_bids.items():
            if bid > 0:
                prices.setdefault(tok, {})["bid"] = round(bid, 4)
        for tok, ask in self.detector._best_asks.items():
            if ask > 0:
                prices.setdefault(tok, {})["ask"] = round(ask, 4)
        return prices

    async def _price_snapshot_loop(self) -> None:
        """Log a price snapshot every second for market dynamics analysis."""
        while not self._stop_requested:
            await asyncio.sleep(1.0)
            if self.glog:
                self.glog.log_price_snapshot(self._get_prices())

    async def run_watch(self) -> None:
        """Tail the log file and continuously update state. Blocks forever."""
        tasks: list[asyncio.Task] = []

        # Subscribe to all plan tokens at startup (before WS connects)
        if self._startup_token_ids and self.market_ws:
            await self.market_ws.subscribe(list(self._startup_token_ids))
            self._subscribed_market_tokens.update(self._startup_token_ids)
            logger.info("guardian: subscribed %d startup tokens to market WS", len(self._startup_token_ids))

        # Start user WS in background if available
        if self.ws:
            tasks.append(asyncio.create_task(
                self.ws.run(on_trade=self._on_ws_trade, on_order=self._on_ws_order)
            ))

        # Start market WS in background if available (for overturn Signal 2)
        if self.market_ws and self.detector:
            tasks.append(asyncio.create_task(
                self.market_ws.run(on_best_bid_ask=self.detector.on_best_bid_ask)
            ))

        # Periodic confirmation check for overturn alerts
        if self.detector:
            tasks.append(asyncio.create_task(self._confirmation_check_loop()))

        # Emit session_start header
        if self.glog and self._session_header:
            self.glog.log_session_start(self._session_header)

        # Periodic price snapshot logging
        if self.detector and self.glog:
            tasks.append(asyncio.create_task(self._price_snapshot_loop()))

        try:
            async for event in tail_log_async(self.log_path):
                if self._stop_requested:
                    break
                self.process_event(event)

                # Subscribe new condition IDs on user WS
                await self._subscribe_pending_conditions()

                # REST fallback for orders WS hasn't reported on
                if self.state.pending_fak_queries and not self.ws:
                    await self._query_pending_fak_orders()
                if not self.ws:
                    await self._poll_gtc_orders()

                if self._on_update_callback:
                    self._on_update_callback(self.state)
        finally:
            if self.ws:
                self.ws.stop()
            if self.market_ws:
                self.market_ws.stop()
            for task in tasks:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    async def _confirmation_check_loop(self) -> None:
        """Periodically check overturn alert confirmations (Signal 1 timer)."""
        logger.info("confirmation check loop started")
        try:
            while True:
                await asyncio.sleep(1.0)
                if self.detector:
                    try:
                        self.detector.check_confirmations()
                    except Exception:
                        logger.exception("check_confirmations crashed")
        except asyncio.CancelledError:
            pass

    def run_snapshot(self) -> TrackerState:
        """Read all events from the log (non-blocking) and return current state."""
        for event in read_log_snapshot(self.log_path):
            self.process_event(event)
        return self.state


def _apply_clob_response(order: TrackedOrder, resp: dict[str, Any]) -> None:
    """Update a TrackedOrder with data from a CLOB GET /order response."""
    order.order_status = str(resp.get("status", ""))
    size_matched = resp.get("size_matched") or resp.get("sizeMatched") or resp.get("filled_size")
    if size_matched is not None:
        try:
            order.fill_amount = float(size_matched)
        except (TypeError, ValueError):
            pass
    price = resp.get("price") or resp.get("avg_fill_price") or resp.get("avgFillPrice")
    if price is not None:
        try:
            order.fill_price = float(price)
        except (TypeError, ValueError):
            pass


def build_token_to_condition_map(compiled_plan: Any) -> dict[str, str]:
    """Build a token_id → condition_id mapping from a compiled plan."""
    mapping: dict[str, str] = {}
    if compiled_plan is None:
        return mapping
    for game in getattr(compiled_plan, "games", ()):
        for market in getattr(game, "markets", ()):
            cid = str(getattr(market, "condition_id", "") or "")
            if not cid:
                continue
            for target in getattr(market, "targets", ()):
                tok = str(getattr(target, "token_id", "") or "")
                if tok:
                    mapping[tok] = cid
    return mapping


def build_game_id_map(compiled_plan: Any) -> dict[str, str]:
    """Build an alternate_provider_game_id → canonical game_id mapping.

    The canonical game_id is ``provider_game_id`` from the compiled plan,
    which is the prefix used in strategy keys.  Alternate IDs (e.g. V2
    fixture IDs) are mapped to the canonical so that tick events using
    alternate IDs resolve to the correct game bucket.
    """
    mapping: dict[str, str] = {}
    if compiled_plan is None:
        return mapping
    for game in getattr(compiled_plan, "games", ()):
        gid = str(getattr(game, "provider_game_id", "") or "")
        if not gid:
            continue
        mapping[gid] = gid  # canonical maps to itself
        for _prov, alt_id in getattr(game, "alternate_provider_game_ids", ()):
            alt = str(alt_id or "").strip()
            if alt:
                mapping[alt] = gid
    return mapping
