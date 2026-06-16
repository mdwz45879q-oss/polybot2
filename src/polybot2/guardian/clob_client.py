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

    Used for querying order fill state, cancellation, and sell order submission.
    Combines HMAC-SHA256 auth for REST queries with py_clob_client_v2 SDK
    for EIP-712 signed order submission.
    """

    def __init__(
        self,
        *,
        clob_host: str = "https://clob.polymarket.com",
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        address: str,
        private_key: str = "",
        chain_id: int = 137,
        signature_type: int = 0,
        funder: str = "",
        timeout: float = 10.0,
    ):
        self._host = clob_host.rstrip("/") + "/"
        self._api_key = api_key
        self._decoded_secret = base64.urlsafe_b64decode(api_secret)
        self._passphrase = api_passphrase
        self._address = address
        self._private_key = private_key
        self._client = httpx.AsyncClient(timeout=timeout)

        # Cache: condition_id → (neg_risk, tick_size)
        self._market_info_cache: dict[str, tuple[bool, str]] = {}

        # SDK client for EIP-712 signed order submission (sell orders)
        self._sdk_client = None
        if private_key and api_key:
            try:
                from py_clob_client_v2 import ClobClient as SdkClobClient, ApiCreds, SignatureTypeV2
                creds = ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                )
                _sig_type = SignatureTypeV2.POLY_1271 if signature_type == 3 else signature_type
                self._sdk_client = SdkClobClient(
                    host=clob_host.rstrip("/"),
                    chain_id=chain_id,
                    key=private_key,
                    creds=creds,
                    signature_type=_sig_type,
                    funder=funder if funder else None,
                )
                logger.info("SDK client initialized for order signing")
            except Exception as exc:
                logger.warning("SDK client init failed (sell orders disabled): %s", exc)

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

    async def get_market_info(self, condition_id: str) -> tuple[bool, str]:
        """Fetch neg_risk and tick_size for a market from the CLOB.

        Caches results per condition_id. Returns (neg_risk, tick_size).
        Falls back to (True, "0.01") on failure.
        """
        if condition_id in self._market_info_cache:
            return self._market_info_cache[condition_id]
        url = f"{self._host}clob-markets/{condition_id}"
        try:
            resp = await self._client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                neg_risk = bool(data.get("neg_risk", True))
                tick_size = str(data.get("minimum_tick_size", "0.01") or "0.01")
                self._market_info_cache[condition_id] = (neg_risk, tick_size)
                logger.info("market info: condition=%s… neg_risk=%s tick_size=%s", condition_id[:16], neg_risk, tick_size)
                return (neg_risk, tick_size)
            logger.warning("get_market_info %s…: status=%d", condition_id[:16], resp.status_code)
        except Exception as exc:
            logger.warning("get_market_info %s… failed: %s", condition_id[:16], exc)
        fallback = (True, "0.01")
        self._market_info_cache[condition_id] = fallback
        return fallback

    async def submit_sell_order(
        self,
        token_id: str,
        size: float,
        price: float,
        condition_id: str = "",
    ) -> dict[str, Any] | None:
        """Submit a GTC sell order at the given price.

        Uses py_clob_client_v2 SDK for EIP-712 signing + submission.
        Uses create_and_post_order (single call with version-retry).
        Fetches neg_risk and tick_size from the CLOB per condition_id.
        Returns the response dict or None on failure.
        """
        if not self._sdk_client:
            logger.warning("sell order skipped — SDK client not initialized (no private key)")
            return None
        try:
            import asyncio
            from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side

            neg_risk, tick_size = await self.get_market_info(condition_id) if condition_id else (True, "0.01")

            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=Side.SELL,
            )
            options = PartialCreateOrderOptions(neg_risk=neg_risk, tick_size=tick_size)
            resp = await asyncio.to_thread(
                self._sdk_client.create_and_post_order,
                order_args, options, OrderType.GTC,
            )
            logger.info(
                "sell order submitted: token=%s… size=%.4f price=%.4f neg_risk=%s tick_size=%s resp=%s",
                token_id[:20], size, price, neg_risk, tick_size, str(resp)[:200],
            )
            return resp if isinstance(resp, dict) else {"raw": str(resp)}
        except Exception as exc:
            err_msg = str(exc)
            logger.warning("sell order failed: token=%s… size=%.4f price=%.4f error=%s", token_id[:20], size, price, err_msg)
            if "signature does not match" in err_msg.lower():
                raise
            return None

    async def cancel_order_by_id(self, order_id: str) -> bool:
        """Cancel a resting order using the SDK client (handles signing).

        Falls back to REST DELETE if SDK not available.
        """
        if self._sdk_client:
            try:
                import asyncio
                from py_clob_client_v2 import OrderPayload
                await asyncio.to_thread(
                    self._sdk_client.cancel_order, OrderPayload(orderID=order_id),
                )
                logger.info("order cancelled via SDK: %s", order_id)
                return True
            except Exception as exc:
                logger.warning("SDK cancel failed, falling back to REST: %s", exc)
        # REST fallback
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

    async def close(self) -> None:
        await self._client.aclose()

    @classmethod
    def from_env(cls) -> ClobClient:
        """Create a ClobClient from POLY_EXEC_* environment variables."""
        import os
        funder = os.getenv("POLY_EXEC_FUNDER", "")
        sig_type_str = os.getenv("POLY_EXEC_SIGNATURE_TYPE", "")
        sig_type = int(sig_type_str) if sig_type_str else (1 if funder else 0)
        return cls(
            clob_host=os.getenv("POLY_EXEC_CLOB_HOST", "https://clob.polymarket.com"),
            api_key=os.getenv("POLY_EXEC_API_KEY", ""),
            api_secret=os.getenv("POLY_EXEC_API_SECRET", ""),
            api_passphrase=os.getenv("POLY_EXEC_API_PASSPHRASE", ""),
            address=os.getenv("POLY_EXEC_FUNDER", ""),
            private_key=os.getenv("POLY_EXEC_PRESIGN_PRIVATE_KEY", ""),
            chain_id=int(os.getenv("POLY_EXEC_CHAIN_ID", "137")),
            signature_type=sig_type,
            funder=funder,
        )
