from __future__ import annotations

import os
import time

import pytest

from polybot2.data.storage import DataRuntimeConfig
from polybot2.execution import FastExecutionConfig, FastExecutionService, OrderRequest, resolve_token_id
from polybot2.execution.contracts import SubmitResult


def _enabled(name: str) -> bool:
    return str(os.getenv(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def _required_env(name: str) -> str:
    value = str(os.getenv(name, "") or "").strip()
    if not value:
        raise AssertionError(f"Missing required environment variable: {name}")
    return value


def _live_cfg_from_env() -> FastExecutionConfig:
    return FastExecutionConfig.from_env(
        {
            # Keep the test deterministic and low-overhead for one-shot validation.
            "ws_enabled": _enabled("POLYBOT2_LIVE_EXEC_WS_ENABLED"),
            "lifecycle_monitor_enabled": _enabled("POLYBOT2_LIVE_EXEC_LIFECYCLE_MONITOR"),
            "presign_enabled": _enabled("POLYBOT2_LIVE_EXEC_PRESIGN_ENABLED"),
        }
    )


def _assert_submit_result(
    *,
    result: SubmitResult,
    expected_client_order_id: str,
    expected_token_id: str,
) -> None:
    assert str(result.source or "").strip() in {
        "submit_immediate",
        "fallback_submit_immediate",
        "presigned_submit_immediate",
        "submit_after_presign_immediate",
    }
    assert str(result.state.client_order_id) == str(expected_client_order_id)
    assert str(result.state.token_id) == str(expected_token_id)
    assert str(result.state.status or "").strip() != ""
    # If this trips, the request likely never made it through a valid broker submit path.
    assert str(result.state.error_code or "").strip().lower() != "submit_exception", (
        "submit_exception: " + str(result.state.reason or "")
    )


pytestmark = [
    pytest.mark.live,
    pytest.mark.skip(
        reason=(
            "Python live execution submit test is deprecated. "
            "Use native Rust live submit test (POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST=1)."
        )
    ),
]


def test_live_execution_submit_small_notional_fak() -> None:
    condition_id = _required_env("POLYBOT2_LIVE_EXEC_CONDITION_ID")
    outcome_index = int(_required_env("POLYBOT2_LIVE_EXEC_OUTCOME_INDEX"))
    token_id = _required_env("POLYBOT2_LIVE_EXEC_TOKEN_ID")

    limit_price = float(str(os.getenv("POLYBOT2_LIVE_EXEC_LIMIT_PRICE", "0.50") or "0.50"))
    amount_usdc = float(str(os.getenv("POLYBOT2_LIVE_EXEC_NOTIONAL_USDC", "0.1") or "0.1"))
    side = str(os.getenv("POLYBOT2_LIVE_EXEC_SIDE", "buy_yes") or "buy_yes").strip().lower()

    assert outcome_index in {0, 1}
    assert 0.0 < float(limit_price) < 1.0
    assert 0.0 < float(amount_usdc) <= 1.0

    runtime_db_path = str(os.getenv("POLYBOT2_LIVE_EXEC_DB_PATH", "") or "").strip()
    if runtime_db_path:
        resolved = resolve_token_id(
            runtime=DataRuntimeConfig(db_path=runtime_db_path),
            condition_id=condition_id,
            outcome_index=int(outcome_index),
        )
        assert str(resolved.token_id) == str(token_id), (
            "Provided token_id does not match local resolver mapping for "
            f"condition_id={condition_id!r}, outcome_index={outcome_index}."
        )

    request = OrderRequest(
        token_id=str(token_id),
        side=str(side),
        amount_usdc=float(amount_usdc),
        limit_price=float(limit_price),
        time_in_force="FAK",
        client_order_id=f"live_exec_smoke_{int(time.time())}_{outcome_index}",
        condition_id=str(condition_id),
    )

    service = FastExecutionService(config=_live_cfg_from_env())
    try:
        result = service.submit_immediate(request)
    finally:
        service.close()

    _assert_submit_result(
        result=result,
        expected_client_order_id=str(request.client_order_id),
        expected_token_id=str(request.token_id),
    )
