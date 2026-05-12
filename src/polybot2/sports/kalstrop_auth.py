"""Shared Kalstrop HMAC authentication helper.

Used by both V1 and V2 providers/resolvers. The auth scheme is identical:
  1. SHA256(shared_secret_raw) → hashed_secret
  2. HMAC-SHA256(hashed_secret, "client_id:timestamp") → signature
  3. Headers: X-Client-ID, X-Timestamp, Authorization: Bearer {signature}
"""

from __future__ import annotations

import hashlib
import hmac
import time


def kalstrop_auth_headers(
    client_id: str,
    shared_secret_raw: str,
    timestamp: str | None = None,
) -> dict[str, str]:
    ts = str(timestamp or int(time.time()))
    hashed_secret = hashlib.sha256(shared_secret_raw.encode("utf-8")).hexdigest()
    payload = f"{client_id}:{ts}".encode("utf-8")
    signature = hmac.new(
        hashed_secret.encode("utf-8"), payload, hashlib.sha256
    ).hexdigest()
    return {
        "X-Client-ID": client_id,
        "X-Timestamp": ts,
        "Authorization": f"Bearer {signature}",
    }
