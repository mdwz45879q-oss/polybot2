"""Authenticated Polymarket CLOB REST client for order queries.

Implements the same HMAC-SHA256 auth scheme as the Rust FastClobSubmitClient,
ported to Python with httpx.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger("polybot2.guardian")


class ClobClient:
    """Authenticated client for Polymarket CLOB REST API.

    Used for querying order fill state and (future) cancellation.
    """

    def __init__(
        self,
        *,
        clob_host: str = "https://clob.polymarket.com",
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        address: str,
        timeout: float = 10.0,
    ):
        self._host = clob_host.rstrip("/") + "/"
        self._api_key = api_key
        self._decoded_secret = base64.urlsafe_b64decode(api_secret)
        self._passphrase = api_passphrase
        self._address = address
        self._client = httpx.AsyncClient(timeout=timeout)

    def _sign(self, timestamp: int, method: str, path: str, body: str = "") -> str:
        """Compute HMAC-SHA256 signature (same scheme as Rust FastClobSubmitClient)."""
        message = f"{timestamp}{method}{path}{body}"
        sig = hmac.new(self._decoded_secret, message.encode(), hashlib.sha256).digest()
        return base64.urlsafe_b64encode(sig).decode()

    def _auth_headers(self, method: str, path: str, body: str = "") -> dict[str, str]:
        timestamp = int(time.time())
        signature = self._sign(timestamp, method, path, body)
        return {
            "POLY_ADDRESS": self._address,
            "POLY_API_KEY": self._api_key,
            "POLY_PASSPHRASE": self._passphrase,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": str(timestamp),
            "Content-Type": "application/json",
        }

    async def get_order(self, order_id: str) -> dict[str, Any] | None:
        """Query fill state for an order by its exchange ID.

        Returns the parsed JSON response or None on failure.
        """
        path = f"/order/{order_id}"
        url = f"{self._host}order/{order_id}"
        headers = self._auth_headers("GET", path)
        try:
            resp = await self._client.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                logger.debug("order not found: %s", order_id)
                return None
            logger.warning("get_order %s: status=%d body=%s", order_id, resp.status_code, resp.text[:200])
            return None
        except Exception as exc:
            logger.warning("get_order %s failed: %s", order_id, exc)
            return None

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a resting order by exchange ID. Returns True on success."""
        path = f"/order/{order_id}"
        url = f"{self._host}order/{order_id}"
        headers = self._auth_headers("DELETE", path)
        try:
            resp = await self._client.delete(url, headers=headers)
            if resp.status_code == 200:
                return True
            logger.warning("cancel_order %s: status=%d body=%s", order_id, resp.status_code, resp.text[:200])
            return False
        except Exception as exc:
            logger.warning("cancel_order %s failed: %s", order_id, exc)
            return False

    async def get_open_orders(self, *, market: str | None = None) -> list[dict[str, Any]]:
        """Get all open orders, optionally filtered by market/condition."""
        path = "/orders"
        url = f"{self._host}orders"
        params: dict[str, str] = {}
        if market:
            params["market"] = market
        headers = self._auth_headers("GET", path)
        try:
            resp = await self._client.get(url, headers=headers, params=params)
            if resp.status_code == 200:
                return resp.json()
            logger.warning("get_open_orders: status=%d", resp.status_code)
            return []
        except Exception as exc:
            logger.warning("get_open_orders failed: %s", exc)
            return []

    async def close(self) -> None:
        await self._client.aclose()

    @classmethod
    def from_env(cls) -> ClobClient:
        """Create a ClobClient from POLY_EXEC_* environment variables."""
        import os
        return cls(
            clob_host=os.getenv("POLY_EXEC_CLOB_HOST", "https://clob.polymarket.com"),
            api_key=os.getenv("POLY_EXEC_API_KEY", ""),
            api_secret=os.getenv("POLY_EXEC_API_SECRET", ""),
            api_passphrase=os.getenv("POLY_EXEC_API_PASSPHRASE", ""),
            address=os.getenv("POLY_EXEC_ADDRESS", ""),
        )
