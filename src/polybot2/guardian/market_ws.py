"""Polymarket market channel WebSocket client for orderbook monitoring.

Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market
and receives best_bid_ask, last_trade_price events for subscribed tokens.
No authentication required.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Awaitable

import websockets
import websockets.exceptions

logger = logging.getLogger("polybot2.guardian")

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
HEARTBEAT_INTERVAL_S = 10.0
RECONNECT_BASE_S = 2.0
RECONNECT_MAX_S = 30.0

OnBestBidAsk = Callable[[dict[str, Any]], Awaitable[None] | None]
OnLastTrade = Callable[[dict[str, Any]], Awaitable[None] | None]


class PolymarketMarketWS:
    """WebSocket client for the Polymarket market data channel.

    Monitors orderbook state (best bid/ask) and trade activity
    for tokens we hold. Used for overturn detection Signal 2.
    No authentication required.
    """

    def __init__(self) -> None:
        self._token_ids: set[str] = set()
        self._ws: Any = None
        self._connected = False
        self._stop = False

    def _build_subscription_msg(self, token_ids: list[str]) -> str:
        return json.dumps({
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        })

    async def subscribe(self, token_ids: list[str]) -> None:
        """Add token IDs to monitor."""
        new_ids = [tid for tid in token_ids if tid not in self._token_ids]
        if not new_ids:
            return
        self._token_ids.update(new_ids)
        if self._ws and self._connected:
            try:
                msg = self._build_subscription_msg(list(self._token_ids))
                await self._ws.send(msg)
                logger.debug("market WS: subscribed to %d total tokens", len(self._token_ids))
            except Exception as exc:
                logger.warning("market WS subscribe failed: %s", exc)

    async def run(
        self,
        on_best_bid_ask: OnBestBidAsk | None = None,
        on_last_trade: OnLastTrade | None = None,
    ) -> None:
        """Main loop: connect, subscribe, receive events. Reconnects on failure."""
        reconnect_delay = RECONNECT_BASE_S

        while not self._stop:
            try:
                await self._run_session(
                    on_best_bid_ask=on_best_bid_ask,
                    on_last_trade=on_last_trade,
                )
                reconnect_delay = RECONNECT_BASE_S
            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning("market WS connection closed: %s", exc)
            except Exception as exc:
                logger.warning("market WS error: %s: %s", type(exc).__name__, exc)

            self._connected = False
            self._ws = None

            if self._stop:
                break

            logger.info("market WS reconnecting in %.1fs...", reconnect_delay)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)

    async def _run_session(
        self,
        on_best_bid_ask: OnBestBidAsk | None = None,
        on_last_trade: OnLastTrade | None = None,
    ) -> None:
        if not self._token_ids:
            await asyncio.sleep(1.0)
            return

        async with websockets.connect(
            MARKET_WS_URL,
            ping_interval=None,
            ping_timeout=None,
            max_size=10 * 1024 * 1024,
        ) as ws:
            self._ws = ws
            self._connected = True

            sub_msg = self._build_subscription_msg(list(self._token_ids))
            await ws.send(sub_msg)
            logger.info("market WS connected, monitoring %d tokens", len(self._token_ids))

            heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))

            try:
                async for raw in ws:
                    if self._stop:
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(data, str) and data == "PONG":
                        continue

                    if not isinstance(data, dict):
                        continue

                    event_type = str(data.get("event_type", ""))
                    if event_type == "best_bid_ask" and on_best_bid_ask:
                        result = on_best_bid_ask(data)
                        if asyncio.iscoroutine(result):
                            await result
                    elif event_type == "last_trade_price" and on_last_trade:
                        result = on_last_trade(data)
                        if asyncio.iscoroutine(result):
                            await result
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

    async def _heartbeat_loop(self, ws: Any) -> None:
        try:
            while not self._stop:
                await asyncio.sleep(HEARTBEAT_INTERVAL_S)
                try:
                    await ws.send("PING")
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    def stop(self) -> None:
        self._stop = True
