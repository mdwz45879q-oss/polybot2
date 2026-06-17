"""Live smoke test for guardian sell order POLY_1271 signing.

Tests both BUY and SELL to distinguish signature failures from balance errors.
The CLOB may check balance before signature — a SELL with no position would
get "not enough balance" even with a broken signature (false positive).
A BUY order avoids that: with USDC balance, it reaches signature verification.

Requires:
  POLYBOT2_ENABLE_LIVE_SELL_SMOKE=1
  POLY_EXEC_API_KEY, POLY_EXEC_API_SECRET, POLY_EXEC_API_PASSPHRASE
  POLY_EXEC_PRESIGN_PRIVATE_KEY, POLY_EXEC_FUNDER
  POLY_EXEC_SIGNATURE_TYPE=3
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac as hmac_mod
import json
import os
import time

import pytest

SKIP_REASON = "set POLYBOT2_ENABLE_LIVE_SELL_SMOKE=1 to run"

pytestmark = pytest.mark.skipif(
    os.getenv("POLYBOT2_ENABLE_LIVE_SELL_SMOKE", "").strip().lower() not in {"1", "true", "yes"},
    reason=SKIP_REASON,
)


SIGNATURE_ERROR = "signature does not match order hash"


def _load_env() -> None:
    from pathlib import Path

    for candidate in [Path(".env"), Path(__file__).resolve().parents[2] / ".env"]:
        if candidate.exists():
            for line in candidate.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
            break


HARDCODED_TOKEN = "68110647781535190797837703927589470479832774174814667833732459898847586979826"
HARDCODED_CONDITION = "0xc5c075c77d2b8a98606097f5e42c3e907a66debf4a4058131dc0160235fa8fed"


def _print_versions() -> None:
    import importlib.metadata
    for pkg in ["py_clob_client_v2", "eth-account", "eth-abi", "eth-utils"]:
        try:
            v = importlib.metadata.version(pkg.replace("_", "-"))
        except Exception:
            v = "NOT INSTALLED"
        print(f"  {pkg}: {v}")


def _direct_post_order(sdk_client, signed, api_secret: str) -> tuple[int, str]:
    """Post a signed order directly, bypassing SDK's post_order/httpx.

    Returns (status_code, response_body).
    """
    import httpx
    from py_clob_client_v2.order_utils.model.order_data_v2 import order_to_json_v2

    owner = sdk_client.creds.api_key or ""
    order_payload = order_to_json_v2(signed, owner, "GTC")
    serialized = json.dumps(order_payload, separators=(",", ":"), ensure_ascii=False)

    print(f"  JSON body ({len(serialized)} chars): {serialized[:400]}…")

    eoa = sdk_client.signer.address()
    decoded_secret = base64.urlsafe_b64decode(api_secret)
    ts = int(time.time())
    hmac_message = f"{ts}POST/order{serialized}"
    hmac_sig = base64.urlsafe_b64encode(
        hmac_mod.new(decoded_secret, hmac_message.encode(), hashlib.sha256).digest()
    ).decode()

    headers = {
        "POLY_ADDRESS": eoa,
        "POLY_API_KEY": sdk_client.creds.api_key,
        "POLY_PASSPHRASE": sdk_client.creds.api_passphrase,
        "POLY_SIGNATURE": hmac_sig,
        "POLY_TIMESTAMP": str(ts),
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=10.0) as client:
        resp = client.post(
            f"{sdk_client.host}/order",
            content=serialized.encode("utf-8"),
            headers=headers,
        )
    return resp.status_code, resp.text


def _test_order(sdk_client, side_enum, side_name: str, token_id: str,
                neg_risk: bool, tick_size: str, api_secret: str) -> str:
    """Sign and post one order. Returns error text or empty string."""
    from py_clob_client_v2 import OrderArgs, PartialCreateOrderOptions

    order_args = OrderArgs(token_id=token_id, price=0.50, size=10.0, side=side_enum)
    options = PartialCreateOrderOptions(neg_risk=neg_risk, tick_size=tick_size)

    print(f"\n--- {side_name} order ---")
    print(f"  create_order (SDK signing)…")
    signed = sdk_client.create_order(order_args, options)
    print(f"  salt={signed.salt} side={int(signed.side)} sigType={int(signed.signatureType)}")
    print(f"  maker={signed.maker} signer={signed.signer}")
    print(f"  makerAmt={signed.makerAmount} takerAmt={signed.takerAmount}")
    print(f"  sig={str(signed.signature)[:60]}…")

    print(f"\n  Direct-posting (our own httpx, not SDK)…")
    status, body = _direct_post_order(sdk_client, signed, api_secret)
    print(f"  status={status} body={body[:300]}")

    return body if status != 200 else ""


async def _run_sell_smoke() -> None:
    _load_env()

    print("\n=== Library versions ===")
    _print_versions()

    print("\n=== Environment ===")
    funder = os.getenv("POLY_EXEC_FUNDER", "")
    sig_type = os.getenv("POLY_EXEC_SIGNATURE_TYPE", "")
    api_secret = os.getenv("POLY_EXEC_API_SECRET", "")
    print(f"  POLY_EXEC_FUNDER: {funder}")
    print(f"  POLY_EXEC_SIGNATURE_TYPE: {sig_type}")

    from polybot2.guardian.clob_client import ClobClient

    client = ClobClient.from_env()
    sdk = client._sdk_client
    assert sdk is not None, "SDK client not initialized"

    print(f"\n=== SDK client ===")
    print(f"  EOA: {sdk.signer.address()}")
    print(f"  funder: {sdk.builder.funder}")
    print(f"  sig_type: {sdk.builder.signature_type}")
    print(f"  v2_order_signer: {sdk.builder._v2_order_signer()}")

    token_id = HARDCODED_TOKEN
    condition_id = HARDCODED_CONDITION
    neg_risk, tick_size = await client.get_market_info(condition_id)
    print(f"\n=== Market ===")
    print(f"  token: {token_id[:30]}…")
    print(f"  neg_risk={neg_risk} tick_size={tick_size}")

    from py_clob_client_v2 import Side

    # Test BUY first — if account has USDC, the CLOB will reach signature verification.
    # If BUY gets "signature error", the signing is fundamentally broken.
    # If BUY gets "not enough balance" or succeeds, signing is fine for BUY.
    buy_error = _test_order(sdk, Side.BUY, "BUY", token_id, neg_risk, tick_size, api_secret)
    sell_error = _test_order(sdk, Side.SELL, "SELL", token_id, neg_risk, tick_size, api_secret)

    print("\n=== Results ===")
    buy_sig_fail = SIGNATURE_ERROR in buy_error.lower()
    sell_sig_fail = SIGNATURE_ERROR in sell_error.lower()
    print(f"  BUY signature rejected:  {buy_sig_fail}")
    print(f"  SELL signature rejected: {sell_sig_fail}")

    if buy_sig_fail or sell_sig_fail:
        if buy_sig_fail and sell_sig_fail:
            print("  DIAGNOSIS: POLY_1271 signing is broken for ALL orders")
        elif sell_sig_fail:
            print("  DIAGNOSIS: POLY_1271 signing broken for SELL only (or BUY hit balance check first)")
        else:
            print("  DIAGNOSIS: POLY_1271 signing broken for BUY only (unexpected)")
    else:
        print("  DIAGNOSIS: Signatures accepted (errors are balance/position, not signing)")

    if buy_error and SIGNATURE_ERROR not in buy_error.lower():
        print(f"  BUY non-sig error: {buy_error[:200]}")
    if sell_error and SIGNATURE_ERROR not in sell_error.lower():
        print(f"  SELL non-sig error: {sell_error[:200]}")

    await client.close()

    assert not buy_sig_fail, f"BUY POLY_1271 signature rejected!\n{buy_error}"
    assert not sell_sig_fail, f"SELL POLY_1271 signature rejected!\n{sell_error}"


def test_guardian_sell_smoke():
    asyncio.run(_run_sell_smoke())
