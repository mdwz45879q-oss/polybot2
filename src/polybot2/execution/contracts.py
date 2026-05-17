"""Contracts for latency-optimized execution service."""

from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Literal


OrderSide = Literal["buy_yes", "buy_no"]
TimeInForce = Literal["GTC", "GTD", "FOK", "FAK"]


def _normalize_side(side: str) -> str:
    raw = str(side or "").strip().lower()
    if raw in {"buy_yes", "yes"}:
        return "buy_yes"
    if raw in {"buy_no", "no"}:
        return "buy_no"
    raise ValueError("side must be one of {'buy_yes','buy_no'}")


def _normalize_tif(tif: str) -> str:
    raw = str(tif or "").strip().upper()
    if raw not in {"GTC", "GTD", "FOK", "FAK"}:
        raise ValueError("time_in_force must be one of {'GTC','GTD','FOK','FAK'}")
    return raw


@dataclass(frozen=True)
class FastExecutionConfig:
    clob_host: str = "https://clob.polymarket.com"
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    funder: str = ""
    signature_type: int = 0
    chain_id: int = 137
    timeout_seconds: float = 3.0
    presign_enabled: bool = False
    presign_private_key: str = ""
    presign_startup_warm_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not str(self.clob_host).strip():
            raise ValueError("clob_host must be non-empty")
        if int(self.chain_id) <= 0:
            raise ValueError("chain_id must be positive")
        if float(self.presign_startup_warm_timeout_seconds) <= 0.0:
            raise ValueError("presign_startup_warm_timeout_seconds must be > 0")

    @classmethod
    def from_env(cls, overrides: dict[str, object] | None = None) -> "FastExecutionConfig":
        def _get(name: str) -> str | None:
            return os.getenv(f"POLY_EXEC_{name}")

        vals: dict[str, object] = {}
        if (v := _get("CLOB_HOST")) is not None:
            vals["clob_host"] = v
        if (v := _get("API_KEY")) is not None:
            vals["api_key"] = v
        if (v := _get("API_SECRET")) is not None:
            vals["api_secret"] = v
        if (v := _get("API_PASSPHRASE")) is not None:
            vals["api_passphrase"] = v
        if (v := _get("FUNDER")) is not None:
            vals["funder"] = str(v)
        if (v := _get("SIGNATURE_TYPE")) is not None:
            vals["signature_type"] = int(v)
        if (v := _get("CHAIN_ID")) is not None:
            vals["chain_id"] = int(v)
        if (v := _get("TIMEOUT_SECONDS")) is not None:
            vals["timeout_seconds"] = float(v)
        if (v := _get("PRESIGN_ENABLED")) is not None:
            vals["presign_enabled"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("PRESIGN_PRIVATE_KEY")) is not None:
            vals["presign_private_key"] = v
        if (v := _get("PRESIGN_STARTUP_WARM_TIMEOUT_SECONDS")) is not None:
            vals["presign_startup_warm_timeout_seconds"] = float(v)
        if overrides:
            vals.update(overrides)
        return cls(**vals)


@dataclass(frozen=True)
class OrderRequest:
    token_id: str
    side: OrderSide | str
    amount_usdc: float
    limit_price: float
    time_in_force: TimeInForce | str
    client_order_id: str
    expire_ts: int | None = None
    condition_id: str = ""
    size_shares: float | None = None

    def __post_init__(self) -> None:
        if not str(self.token_id or "").strip():
            raise ValueError("token_id must be non-empty")
        if not str(self.client_order_id or "").strip():
            raise ValueError("client_order_id must be non-empty")
        _normalize_side(str(self.side))
        tif = _normalize_tif(str(self.time_in_force))
        if float(self.amount_usdc) <= 0.0:
            raise ValueError("amount_usdc must be > 0")
        if float(self.limit_price) <= 0.0 or float(self.limit_price) >= 1.0:
            raise ValueError("limit_price must be in (0, 1)")
        if tif == "GTD":
            if self.expire_ts is None or int(self.expire_ts) <= int(time.time()):
                raise ValueError("expire_ts must be in the future when time_in_force='GTD'")

    @property
    def normalized_side(self) -> str:
        return _normalize_side(str(self.side))

    @property
    def normalized_tif(self) -> str:
        return _normalize_tif(str(self.time_in_force))


    reason: str = ""


__all__ = [
    "ACTIVE_ORDER_STATUSES",
    "CancelRequest",
    "CancelResult",
    "FastExecutionConfig",
    "OrderRequest",
    "PreSignKey",
    "PreSignedOrder",
    "OrderSide",
    "OrderState",
    "OrderLifecycleStatus",
    "REASON_CANCEL_FAILED",
    "REASON_INVALID_REQUEST",
    "REASON_MISSING_IDS",
    "REASON_OK",
    "REASON_REPLACE_FAILED",
    "REASON_SUBMIT_FAILED",
    "REASON_TIMEOUT",
    "ReplaceRequest",
    "ReplaceResult",
    "SubmitResult",
    "TERMINAL_ORDER_STATUSES",
    "TimeInForce",
]
