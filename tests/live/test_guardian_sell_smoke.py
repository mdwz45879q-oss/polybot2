"""Live smoke test for guardian sell order POLY_1271 signing.

Proves that the Python SDK's EIP-712 POLY_1271 signature is accepted by the CLOB.
Expected result: a balance/position error (not "signature does not match order hash").

Requires:
  POLYBOT2_ENABLE_LIVE_SELL_SMOKE=1
  POLY_EXEC_API_KEY, POLY_EXEC_API_SECRET, POLY_EXEC_API_PASSPHRASE
  POLY_EXEC_PRESIGN_PRIVATE_KEY, POLY_EXEC_FUNDER
  POLY_EXEC_SIGNATURE_TYPE=3
"""
from __future__ import annotations

import asyncio
import os

import pytest

SKIP_REASON = "set POLYBOT2_ENABLE_LIVE_SELL_SMOKE=1 to run"

pytestmark = pytest.mark.skipif(
    os.getenv("POLYBOT2_ENABLE_LIVE_SELL_SMOKE", "").strip().lower() not in {"1", "true", "yes"},
    reason=SKIP_REASON,
)


SIGNATURE_ERROR = "signature does not match order hash"


def _load_env() -> None:
    """Load .env file if present (same as hotpath orchestrator)."""
    from pathlib import Path

    for candidate in [Path(".env"), Path(__file__).resolve().parents[2] / ".env"]:
        if candidate.exists():
            for line in candidate.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
            break


def _find_active_token() -> tuple[str, str]:
    """Find an active market token_id + condition_id from the CLOB."""
    import httpx

    host = os.getenv("POLY_EXEC_CLOB_HOST", "https://clob.polymarket.com")
    resp = httpx.get(f"{host}/markets?next_cursor=MA==", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
    for m in markets:
        tokens = m.get("tokens", [])
        cid = m.get("condition_id", "")
        if tokens and cid and m.get("active"):
            return tokens[0].get("token_id", ""), cid
    pytest.skip("no active market found on CLOB")


async def _run_sell_smoke() -> None:
    _load_env()

    from polybot2.guardian.clob_client import ClobClient

    client = ClobClient.from_env()
    assert client._sdk_client is not None, (
        "SDK client not initialized — check POLY_EXEC_PRESIGN_PRIVATE_KEY"
    )

    token_id, condition_id = _find_active_token()
    print(f"\nUsing token: {token_id[:20]}…")
    print(f"Condition: {condition_id[:20]}…")

    neg_risk, tick_size = await client.get_market_info(condition_id)
    print(f"neg_risk={neg_risk}, tick_size={tick_size}")

    error_text = ""
    try:
        resp = await client.submit_sell_order(
            token_id=token_id,
            size=0.5,
            price=0.01,
            condition_id=condition_id,
        )
        if resp is not None:
            print(f"Response (unexpected success): {resp}")
    except Exception as exc:
        error_text = str(exc)
        print(f"Error: {error_text[:500]}")

    assert SIGNATURE_ERROR not in error_text.lower(), (
        f"POLY_1271 signature rejected by CLOB!\nError: {error_text}"
    )
    if error_text:
        print(f"\nPASS — signature accepted, rejection reason: {error_text[:200]}")
    else:
        print("\nPASS — no signature error (order returned None or succeeded)")

    await client.close()


def test_guardian_sell_smoke():
    asyncio.run(_run_sell_smoke())
