"""Contracts for latency-optimized execution service."""

from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Any, Literal


OrderSide = Literal["buy_yes", "buy_no"]
TimeInForce = Literal["GTC", "GTD", "FOK", "FAK"]
OrderLifecycleStatus = Literal[
    "submitted",
    "open",
    "partially_filled",
    "filled",
    "canceled",
    "expired",
    "rejected",
    "failed",
    "replaced",
]

TERMINAL_ORDER_STATUSES: frozenset[str] = frozenset(
    {"filled", "canceled", "expired", "rejected", "failed", "replaced"}
)
ACTIVE_ORDER_STATUSES: frozenset[str] = frozenset(
    {"submitted", "open", "partially_filled"}
)

REASON_OK = "ok"
REASON_TIMEOUT = "timeout"
REASON_CANCEL_FAILED = "cancel_failed"
REASON_REPLACE_FAILED = "replace_failed"
REASON_SUBMIT_FAILED = "submit_failed"
REASON_INVALID_REQUEST = "invalid_request"
REASON_MISSING_IDS = "missing_ids"


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
    user_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""
    funder: str = ""
    signature_type: int = 0
    chain_id: int = 137
    timeout_seconds: float = 3.0
    ws_enabled: bool = True
    ws_wait_timeout_seconds: float = 0.75
    ws_poll_interval_seconds: float = 0.05
    rest_fallback_enabled: bool = True
    rest_poll_interval_seconds: float = 0.05
    rest_max_polls: int = 5
    lifecycle_monitor_enabled: bool = True
    lifecycle_monitor_poll_interval_seconds: float = 0.05
    max_retries: int = 1
    retry_base_delay_seconds: float = 0.05
    retry_max_delay_seconds: float = 0.25
    presign_enabled: bool = False
    presign_private_key: str = ""
    presign_ttl_seconds: float = 20.0
    presign_safety_margin_seconds: float = 1.0
    presign_pool_target_per_key: int = 8
    presign_refill_batch_size: int = 4
    presign_refill_interval_seconds: float = 0.02
    presign_startup_warm_timeout_seconds: float = 5.0
    presign_price_bps_bucket: float = 25.0
    presign_size_bucket_scheme: tuple[float, ...] = (5.0, 10.0, 20.0, 50.0, 100.0)
    presign_fallback_on_miss: bool = True
    presign_signer_type: str = "eip712_v2"
    signer_wire_version: str = "v2"
    active_order_refresh_interval_seconds: float = 0.25

    def __post_init__(self) -> None:
        if not str(self.clob_host).strip():
            raise ValueError("clob_host must be non-empty")
        if not str(self.user_ws_url).strip():
            raise ValueError("user_ws_url must be non-empty")
        if int(self.chain_id) <= 0:
            raise ValueError("chain_id must be positive")
        if int(self.signature_type) < 0:
            raise ValueError("signature_type must be >= 0")
        if float(self.timeout_seconds) <= 0.0:
            raise ValueError("timeout_seconds must be > 0")
        if float(self.ws_wait_timeout_seconds) <= 0.0:
            raise ValueError("ws_wait_timeout_seconds must be > 0")
        if float(self.ws_poll_interval_seconds) <= 0.0:
            raise ValueError("ws_poll_interval_seconds must be > 0")
        if float(self.rest_poll_interval_seconds) <= 0.0:
            raise ValueError("rest_poll_interval_seconds must be > 0")
        if int(self.rest_max_polls) < 0:
            raise ValueError("rest_max_polls must be >= 0")
        if float(self.lifecycle_monitor_poll_interval_seconds) <= 0.0:
            raise ValueError("lifecycle_monitor_poll_interval_seconds must be > 0")
        if int(self.max_retries) < 0:
            raise ValueError("max_retries must be >= 0")
        if float(self.retry_base_delay_seconds) <= 0.0:
            raise ValueError("retry_base_delay_seconds must be > 0")
        if float(self.retry_max_delay_seconds) <= 0.0:
            raise ValueError("retry_max_delay_seconds must be > 0")
        if float(self.presign_ttl_seconds) <= 0.0:
            raise ValueError("presign_ttl_seconds must be > 0")
        if float(self.presign_safety_margin_seconds) < 0.0:
            raise ValueError("presign_safety_margin_seconds must be >= 0")
        if float(self.presign_safety_margin_seconds) >= float(self.presign_ttl_seconds):
            raise ValueError("presign_safety_margin_seconds must be < presign_ttl_seconds")
        if int(self.presign_pool_target_per_key) < 0:
            raise ValueError("presign_pool_target_per_key must be >= 0")
        if int(self.presign_refill_batch_size) <= 0:
            raise ValueError("presign_refill_batch_size must be > 0")
        if float(self.presign_refill_interval_seconds) <= 0.0:
            raise ValueError("presign_refill_interval_seconds must be > 0")
        if float(self.presign_startup_warm_timeout_seconds) <= 0.0:
            raise ValueError("presign_startup_warm_timeout_seconds must be > 0")
        if float(self.presign_price_bps_bucket) <= 0.0:
            raise ValueError("presign_price_bps_bucket must be > 0")
        signer_wire_version = str(self.signer_wire_version or "").strip().lower()
        if signer_wire_version != "v2":
            raise ValueError("signer_wire_version must be 'v2'")
        object.__setattr__(self, "signer_wire_version", signer_wire_version)
        if str(self.presign_signer_type or "").strip().lower() != "eip712_v2":
            raise ValueError("presign_signer_type must be 'eip712_v2'")
        if float(self.active_order_refresh_interval_seconds) < 0.0:
            raise ValueError("active_order_refresh_interval_seconds must be >= 0")
        scheme = tuple(
            sorted(
                {
                    float(v)
                    for v in tuple(self.presign_size_bucket_scheme)
                    if float(v) > 0.0
                }
            )
        )
        if not scheme:
            raise ValueError("presign_size_bucket_scheme must include at least one positive bucket")
        object.__setattr__(self, "presign_size_bucket_scheme", scheme)

    @classmethod
    def from_env(cls, overrides: dict[str, object] | None = None) -> "FastExecutionConfig":
        def _get(name: str) -> str | None:
            return os.getenv(f"POLY_EXEC_{name}")

        vals: dict[str, object] = {}
        if (v := _get("CLOB_HOST")) is not None:
            vals["clob_host"] = v
        if (v := _get("USER_WS_URL")) is not None:
            vals["user_ws_url"] = v
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
        if (v := _get("WS_ENABLED")) is not None:
            vals["ws_enabled"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("REST_FALLBACK_ENABLED")) is not None:
            vals["rest_fallback_enabled"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("LIFECYCLE_MONITOR_ENABLED")) is not None:
            vals["lifecycle_monitor_enabled"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("LIFECYCLE_MONITOR_POLL_INTERVAL_SECONDS")) is not None:
            vals["lifecycle_monitor_poll_interval_seconds"] = float(v)
        if (v := _get("PRESIGN_ENABLED")) is not None:
            vals["presign_enabled"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("PRESIGN_PRIVATE_KEY")) is not None:
            vals["presign_private_key"] = v
        if (v := _get("PRESIGN_TTL_SECONDS")) is not None:
            vals["presign_ttl_seconds"] = float(v)
        if (v := _get("PRESIGN_SAFETY_MARGIN_SECONDS")) is not None:
            vals["presign_safety_margin_seconds"] = float(v)
        if (v := _get("PRESIGN_POOL_TARGET_PER_KEY")) is not None:
            vals["presign_pool_target_per_key"] = int(v)
        if (v := _get("PRESIGN_REFILL_BATCH_SIZE")) is not None:
            vals["presign_refill_batch_size"] = int(v)
        if (v := _get("PRESIGN_REFILL_INTERVAL_SECONDS")) is not None:
            vals["presign_refill_interval_seconds"] = float(v)
        if (v := _get("PRESIGN_STARTUP_WARM_TIMEOUT_SECONDS")) is not None:
            vals["presign_startup_warm_timeout_seconds"] = float(v)
        if (v := _get("PRESIGN_PRICE_BPS_BUCKET")) is not None:
            vals["presign_price_bps_bucket"] = float(v)
        if (v := _get("PRESIGN_SIZE_BUCKET_SCHEME")) is not None:
            parsed: list[float] = []
            for part in str(v).split(","):
                text = str(part or "").strip()
                if not text:
                    continue
                parsed.append(float(text))
            vals["presign_size_bucket_scheme"] = tuple(parsed)
        if (v := _get("PRESIGN_FALLBACK_ON_MISS")) is not None:
            vals["presign_fallback_on_miss"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if (v := _get("PRESIGN_SIGNER_TYPE")) is not None:
            vals["presign_signer_type"] = str(v)
        if (v := _get("SIGNER_WIRE_VERSION")) is not None:
            vals["signer_wire_version"] = str(v)
        if (v := _get("ACTIVE_ORDER_REFRESH_INTERVAL_SECONDS")) is not None:
            vals["active_order_refresh_interval_seconds"] = float(v)
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


@dataclass(frozen=True)
class CancelRequest:
    client_order_id: str = ""
    exchange_order_id: str = ""

    def __post_init__(self) -> None:
        if not str(self.client_order_id or "").strip() and not str(self.exchange_order_id or "").strip():
            raise ValueError("cancel request requires client_order_id or exchange_order_id")


@dataclass(frozen=True)
class ReplaceRequest:
    target_client_order_id: str = ""
    target_exchange_order_id: str = ""
    new_order: OrderRequest = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if not str(self.target_client_order_id or "").strip() and not str(self.target_exchange_order_id or "").strip():
            raise ValueError("replace request requires target_client_order_id or target_exchange_order_id")
        if self.new_order is None:
            raise ValueError("replace request requires new_order")


@dataclass(frozen=True)
class OrderState:
    client_order_id: str
    exchange_order_id: str
    token_id: str
    side: str
    requested_amount_usdc: float
    filled_amount_usdc: float
    limit_price: float
    time_in_force: str
    status: str
    reason: str = ""
    error_code: str = ""
    avg_fill_price: float | None = None
    submitted_ts: int = 0
    updated_ts: int = 0
    parent_client_order_id: str = ""
    replaced_by_client_order_id: str = ""

    def is_terminal(self) -> bool:
        return str(self.status).strip().lower() in TERMINAL_ORDER_STATUSES


@dataclass(frozen=True)
class PreSignKey:
    token_id: str
    side: str
    tif: str
    price_bucket: float
    size_bucket: float


@dataclass(frozen=True)
class PreSignedOrder:
    key: PreSignKey
    request: OrderRequest
    nonce: int
    expire_ts: int
    signed_ts: int
    payload: dict[str, Any]
    signature: str
    signer_type: str = "eip712_v2"

    def is_stale(self, *, now_ts: int, safety_margin_seconds: float) -> bool:
        return int(now_ts) >= int(self.expire_ts - max(0.0, float(safety_margin_seconds)))


@dataclass(frozen=True)
class SubmitResult:
    state: OrderState
    terminal: bool
    source: str
    timed_out: bool = False


@dataclass(frozen=True)
class CancelResult:
    state: OrderState | None
    canceled: bool
    source: str
    timed_out: bool = False
    reason: str = ""


@dataclass(frozen=True)
class ReplaceResult:
    old_state: OrderState | None
    new_state: OrderState | None
    replaced: bool
    source: str
    timed_out: bool = False
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
