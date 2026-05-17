from __future__ import annotations

import polybot2.hotpath as hotpath_pkg

from polybot2.execution import FastExecutionConfig, OrderRequest
from polybot2.hotpath import HotPathConfig, NativeHotPathService


def test_execution_contracts_smoke() -> None:
    cfg = FastExecutionConfig()
    assert cfg.timeout_seconds > 0
    req = OrderRequest(
        token_id="tok",
        side="buy_yes",
        amount_usdc=10.0,
        limit_price=0.25,
        time_in_force="GTC",
        client_order_id="c1",
    )
    assert req.normalized_side == "buy_yes"


def test_hotpath_contracts_smoke() -> None:
    cfg = HotPathConfig(native_engine_required=True)
    assert cfg.native_engine_required is True


def test_hotpath_runtime_exports_are_native_only() -> None:
    assert hasattr(hotpath_pkg, "NativeHotPathService")
    assert NativeHotPathService is hotpath_pkg.NativeHotPathService
    assert not hasattr(hotpath_pkg, "HotPathService")
