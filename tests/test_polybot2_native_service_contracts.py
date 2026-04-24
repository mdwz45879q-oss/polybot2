from __future__ import annotations

from polybot2.hotpath.native_service import NativeHotPathService


class _FakeBridge:
    def health_snapshot(self):  # type: ignore[no-untyped-def]
        return {
            "running": True,
            "subscriptions": ["g1"],
            "reconnects": 2,
            "last_error": "",
        }

def test_native_service_health_omits_legacy_queue_fields() -> None:
    svc = NativeHotPathService(provider=object(), execution=object())
    svc._runtime_bridge = _FakeBridge()  # type: ignore[attr-defined]
    svc._running = True  # type: ignore[attr-defined]
    out = svc.health()
    assert "legacy_queue_metrics" not in out
    assert "ingest_queue_depth" not in out
    assert "route_queue_depth" not in out
    assert "sidecar_queue_depth" not in out
    assert bool(out.get("running")) is True


def test_native_service_paper_mode_uses_noop_dispatch() -> None:
    svc = NativeHotPathService(provider=object(), execution=object(), execution_mode="paper")
    svc.set_runtime_timing_policy(subscribe_lead_minutes=7, subscription_refresh_seconds=33)
    payload = svc._execution_config_payload()  # type: ignore[attr-defined]
    runtime_payload = svc._runtime_config_payload()  # type: ignore[attr-defined]
    assert str(payload.get("dispatch_mode") or "") == "noop"
    assert "amount_usdc" in runtime_payload
    assert "limit_price" in runtime_payload
    assert "buy_yes_limit_price" not in runtime_payload
    assert int(runtime_payload.get("subscribe_lead_minutes") or 0) == 7
    assert int(runtime_payload.get("subscription_refresh_seconds") or 0) == 33
    assert "telemetry_enabled" not in runtime_payload
    assert "telemetry_level" not in runtime_payload
    assert "telemetry_socket_path" not in runtime_payload
    assert "telemetry_queue_capacity" not in runtime_payload
    assert "presign_ttl_seconds" not in payload
    assert "presign_safety_margin_seconds" not in payload
    assert "presign_price_bps_bucket" not in payload
    assert "presign_size_bucket_scheme" not in payload
    assert "presign_fallback_on_miss" not in payload
    assert "max_retries" not in payload
    assert "retry_base_delay_seconds" not in payload
    assert "retry_max_delay_seconds" not in payload
    assert "replace_fallback_on_unsupported" not in payload
    assert "timeout_seconds" not in payload
    health = svc.health()
    assert str(health.get("execution_mode") or "") == "paper"
