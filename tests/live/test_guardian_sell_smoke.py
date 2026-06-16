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
import json
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


HARDCODED_TOKEN = "68110647781535190797837703927589470479832774174814667833732459898847586979826"
HARDCODED_CONDITION = "0xc5c075c77d2b8a98606097f5e42c3e907a66debf4a4058131dc0160235fa8fed"


def _find_active_token() -> tuple[str, str]:
    """Return a known-good active token_id + condition_id."""
    return HARDCODED_TOKEN, HARDCODED_CONDITION


def _print_versions() -> None:
    """Print relevant library versions for debugging."""
    import importlib.metadata
    for pkg in ["py_clob_client_v2", "eth-account", "eth-abi", "eth-utils"]:
        try:
            v = importlib.metadata.version(pkg.replace("_", "-"))
        except Exception:
            v = "NOT INSTALLED"
        print(f"  {pkg}: {v}")


def _inspect_signed_order(signed: object) -> dict:
    """Extract and print all fields from a signed order."""
    fields = {}
    for attr in ["salt", "maker", "signer", "tokenId", "makerAmount", "takerAmount",
                  "side", "signatureType", "timestamp", "metadata", "builder",
                  "expiration", "signature"]:
        val = getattr(signed, attr, None)
        fields[attr] = val
        if attr == "signature":
            print(f"  {attr}: {str(val)[:60]}…" if val and len(str(val)) > 60 else f"  {attr}: {val}")
        else:
            print(f"  {attr}: {val}")
    return fields


async def _run_sell_smoke() -> None:
    _load_env()

    print("\n=== Library versions ===")
    _print_versions()

    print("\n=== Environment ===")
    funder = os.getenv("POLY_EXEC_FUNDER", "")
    sig_type = os.getenv("POLY_EXEC_SIGNATURE_TYPE", "")
    print(f"  POLY_EXEC_FUNDER: {funder}")
    print(f"  POLY_EXEC_SIGNATURE_TYPE: {sig_type}")

    from polybot2.guardian.clob_client import ClobClient

    client = ClobClient.from_env()
    sdk = client._sdk_client
    assert sdk is not None, "SDK client not initialized — check POLY_EXEC_PRESIGN_PRIVATE_KEY"

    print(f"\n=== SDK client ===")
    print(f"  signer.address (EOA): {sdk.signer.address()}")
    print(f"  builder.funder: {sdk.builder.funder}")
    print(f"  builder.signature_type: {sdk.builder.signature_type}")
    print(f"  builder._v2_order_signer(): {sdk.builder._v2_order_signer()}")

    token_id, condition_id = _find_active_token()
    print(f"\n=== Market ===")
    print(f"  token_id: {token_id}")
    print(f"  condition_id: {condition_id}")

    neg_risk, tick_size = await client.get_market_info(condition_id)
    print(f"  neg_risk: {neg_risk}")
    print(f"  tick_size: {tick_size}")

    from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
    from py_clob_client_v2.config import get_contract_config

    contract_config = get_contract_config(137)
    exchange = contract_config.neg_risk_exchange_v2 if neg_risk else contract_config.exchange_v2
    print(f"  exchange_address: {exchange}")

    # Use realistic sell parameters (large enough to pass CLOB minimum)
    test_size = 10.0
    test_price = 0.50

    order_args = OrderArgs(token_id=token_id, price=test_price, size=test_size, side=Side.SELL)
    options = PartialCreateOrderOptions(neg_risk=neg_risk, tick_size=tick_size)

    print(f"\n=== Step 1: create_order (sign) ===")
    print(f"  size={test_size}, price={test_price}, side=SELL")
    signed = sdk.create_order(order_args, options)
    fields = _inspect_signed_order(signed)

    # Verify signature locally
    print(f"\n=== Step 2: local signature verification ===")
    try:
        from eth_abi import encode as abi_encode
        from eth_utils import keccak as _keccak
        from eth_account import Account

        ORDER_TYPE_STRING = (
            "Order(uint256 salt,address maker,address signer,uint256 tokenId,"
            "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,"
            "uint256 timestamp,bytes32 metadata,bytes32 builder)"
        )
        SOLADY_TYPE_STRING = (
            "TypedDataSign(Order contents,string name,string version,uint256 chainId,"
            "address verifyingContract,bytes32 salt)"
            f"{ORDER_TYPE_STRING}"
        )
        DOMAIN_TYPE_STRING = (
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
        )

        ORDER_TYPE_HASH = _keccak(text=ORDER_TYPE_STRING)
        DOMAIN_TYPE_HASH = _keccak(text=DOMAIN_TYPE_STRING)
        SOLADY_TYPE_HASH = _keccak(text=SOLADY_TYPE_STRING)

        def _hex32(h: str) -> bytes:
            return bytes.fromhex(h.replace("0x", "").zfill(64))

        # Recompute contents_hash
        contents_hash = _keccak(primitive=abi_encode(
            ["bytes32", "uint256", "address", "address", "uint256", "uint256", "uint256",
             "uint8", "uint8", "uint256", "bytes32", "bytes32"],
            [ORDER_TYPE_HASH, int(fields["salt"]), fields["maker"], fields["signer"],
             int(fields["tokenId"]), int(fields["makerAmount"]), int(fields["takerAmount"]),
             int(fields["side"]), int(fields["signatureType"]), int(fields["timestamp"]),
             _hex32(fields["metadata"]), _hex32(fields["builder"])],
        ))
        print(f"  contents_hash: 0x{contents_hash.hex()}")

        # Recompute app_domain_separator
        CTF_EXCHANGE_NAME_HASH = _keccak(text="Polymarket CTF Exchange")
        CTF_EXCHANGE_VERSION_HASH = _keccak(text="2")
        app_domain_sep = _keccak(primitive=abi_encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [DOMAIN_TYPE_HASH, CTF_EXCHANGE_NAME_HASH, CTF_EXCHANGE_VERSION_HASH, 137, exchange],
        ))
        print(f"  app_domain_sep: 0x{app_domain_sep.hex()}")

        # Recompute TypedDataSign struct hash
        DW_NAME_HASH = _keccak(text="DepositWallet")
        DW_VERSION_HASH = _keccak(text="1")
        DW_SALT = bytes(32)
        tds_hash = _keccak(primitive=abi_encode(
            ["bytes32", "bytes32", "bytes32", "bytes32", "uint256", "address", "bytes32"],
            [SOLADY_TYPE_HASH, contents_hash, DW_NAME_HASH, DW_VERSION_HASH, 137,
             fields["signer"], DW_SALT],
        ))
        print(f"  typed_data_sign_hash: 0x{tds_hash.hex()}")

        # Recompute final digest
        digest = _keccak(primitive=b"\x19\x01" + app_domain_sep + tds_hash)
        print(f"  digest: 0x{digest.hex()}")

        # Extract inner signature (first 65 bytes of the POLY_1271 signature)
        sig_hex = fields["signature"]
        if sig_hex.startswith("0x"):
            sig_hex = sig_hex[2:]
        inner_sig_bytes = bytes.fromhex(sig_hex[:130])  # 65 bytes = 130 hex chars
        print(f"  inner_sig (65B): 0x{inner_sig_bytes.hex()[:40]}…")

        # Recover signer from inner signature
        recovered = Account._recover_hash(digest, signature=inner_sig_bytes)
        eoa = sdk.signer.address()
        print(f"  recovered: {recovered}")
        print(f"  expected EOA: {eoa}")
        print(f"  match: {recovered.lower() == eoa.lower()}")

        # Check embedded fields in POLY_1271 signature
        remaining = sig_hex[130:]
        embedded_domain_sep = remaining[:64]
        embedded_contents_hash = remaining[64:128]
        print(f"\n  Embedded domain_sep match: {embedded_domain_sep == app_domain_sep.hex()}")
        print(f"  Embedded contents_hash match: {embedded_contents_hash == contents_hash.hex()}")

    except Exception as e:
        print(f"  verification error: {e}")

    # Step 3: post the order
    print(f"\n=== Step 3: post_order to CLOB ===")
    error_text = ""
    try:
        resp = sdk.post_order(signed, OrderType.GTC)
        print(f"  Response: {str(resp)[:300]}")
    except Exception as exc:
        error_text = str(exc)
        print(f"  Error: {error_text[:500]}")

    assert SIGNATURE_ERROR not in error_text.lower(), (
        f"POLY_1271 signature rejected by CLOB!\nError: {error_text}"
    )
    if error_text:
        print(f"\nPASS — signature accepted, rejection reason: {error_text[:200]}")
    else:
        print(f"\nPASS — order accepted by CLOB")

    await client.close()


def test_guardian_sell_smoke():
    asyncio.run(_run_sell_smoke())
