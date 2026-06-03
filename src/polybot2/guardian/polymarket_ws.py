"""Polymarket user channel WebSocket client for real-time fill tracking.

Connects to wss://ws-subscriptions-clob.polymarket.com/ws/user
and receives trade/order events for subscribed condition IDs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Awaitable

import websockets
import websockets.exceptions

logger = logging.getLogger("polybot2.guardian")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
HEARTBEAT_INTERVAL_S = 10.0
RECONNECT_BASE_S = 2.0
RECONNECT_MAX_S = 30.0

# Callback types
OnTrade = Callable[[dict[str, Any]], Awaitable[None] | None]
OnOrder = Callable[[dict[str, Any]], Awaitable[None] | None]


class PolymarketUserWS:
    """WebSocket client for the Polymarket user channel.

    Receives real-time trade fills and order updates for our positions.
    Handles authentication, heartbeat, reconnection, and dynamic subscription.
    """

    def __init__(
        self,
        *,
        api_key: str,
        secret: str,
        passphrase: str,
    ):
        self._api_key = api_key
        self._secret = secret
        self._passphrase = passphrase
        self._condition_ids: set[str] = set()
        self._ws: Any = None
        self._connected = False
        self._stop = False

    def _build_subscription_msg(self, condition_ids: list[str]) -> str:
        """Build the subscription message with auth credentials."""
        return json.dumps({
            "auth": {
                "apiKey": self._api_key,
                "secret": self._secret,
                "passphrase": self._passphrase,
            },
            "markets": condition_ids,
            "type": "user",
        })

    async def connect_and_subscribe(self, condition_ids: list[str]) -> None:
        """Connect to the WS and subscribe to initial condition IDs."""
        self._condition_ids.update(condition_ids)

    async def subscribe(self, condition_ids: list[str]) -> None:
        """Add condition IDs to active subscription."""
        new_ids = [cid for cid in condition_ids if cid not in self._condition_ids]
        if not new_ids:
            return
        self._condition_ids.update(new_ids)
        if self._ws and self._connected:
            try:
                msg = json.dumps({
                    "auth": {
                        "apiKey": self._api_key,
                        "secret": self._secret,
                        "passphrase": self._passphrase,
                    },
                    "markets": list(self._condition_ids),
                    "type": "user",
                })
                await self._ws.send(msg)
                logger.debug("subscribed to %d additional condition IDs", len(new_ids))
            except Exception as exc:
                logger.warning("subscribe failed: %s", exc)

    async def run(
        self,
        on_trade: OnTrade | None = None,
        on_order: OnOrder | None = None,
    ) -> None:
        """Main loop: connect, subscribe, receive events, handle heartbeat.

        Reconnects on failure with exponential backoff.
        Blocks until stop() is called.
        """
        reconnect_delay = RECONNECT_BASE_S

        while not self._stop:
            try:
                await self._run_session(on_trade=on_trade, on_order=on_order)
                reconnect_delay = RECONNECT_BASE_S  # reset on clean disconnect
            except websockets.exceptions.ConnectionClosed as exc:
                logger.warning("user WS connection closed: %s", exc)
                if self._stop:
                    break
                logger.info("user WS reconnecting in %.1fs...", reconnect_delay)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)
            except Exception as exc:
                logger.warning("user WS error: %s: %s", type(exc).__name__, exc)
                if self._stop:
                    break
                logger.info("user WS reconnecting in %.1fs...", reconnect_delay)
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)

            self._connected = False
            self._ws = None

    async def _run_session(
        self,
        on_trade: OnTrade | None = None,
        on_order: OnOrder | None = None,
    ) -> None:
        """Single WS session: connect, subscribe, receive loop with heartbeat."""
        # Wait for condition IDs before connecting — Polymarket requires
        # at least one market subscription for the connection to be useful.
        while not self._condition_ids and not self._stop:
            await asyncio.sleep(2.0)
        if self._stop:
            return

        async with websockets.connect(
            WS_URL,
            ping_interval=None,  # we handle heartbeat manually
            ping_timeout=None,
            max_size=10 * 1024 * 1024,
        ) as ws:
            self._ws = ws
            self._connected = True

            # Send subscription with auth
            sub_msg = self._build_subscription_msg(list(self._condition_ids))
            await ws.send(sub_msg)
            logger.info("user WS connected, subscribed to %d condition IDs", len(self._condition_ids))

            # Start heartbeat task
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))

            try:
                async for raw in ws:
                    if self._stop:
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Handle PONG response (heartbeat ack)
                    if isinstance(data, str) and data == "PONG":
                        continue

                    if not isinstance(data, dict):
                        continue

                    event_type = data.get("event_type", "")
                    if event_type == "trade" and on_trade:
                        result = on_trade(data)
                        if asyncio.iscoroutine(result):
                            await result
                    elif event_type == "order" and on_order:
                        result = on_order(data)
                        if asyncio.iscoroutine(result):
                            await result
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

    async def _heartbeat_loop(self, ws: Any) -> None:
        """Send PING every HEARTBEAT_INTERVAL_S."""
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
        """Signal the WS client to stop."""
        self._stop = True

    @classmethod
    def from_env(cls) -> PolymarketUserWS:
        """Create from POLY_EXEC_* environment variables."""
        import os
        return cls(
            api_key=os.getenv("POLY_EXEC_API_KEY", ""),
            secret=os.getenv("POLY_EXEC_API_SECRET", ""),
            passphrase=os.getenv("POLY_EXEC_API_PASSPHRASE", ""),
        )
