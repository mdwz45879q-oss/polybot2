"""Provider construction helpers for sports integrations."""

from __future__ import annotations

import logging
import os

from polybot2.sports.base import SportsDataProviderBase
from polybot2.sports.boltodds import BoltOddsProvider, BoltOddsProviderConfig
from polybot2.sports.kalstrop import KalstropProvider, KalstropProviderConfig
from polybot2.sports.recorder import RawFrameRecorder, UpdateRecorder


def resolve_kalstrop_credentials_from_env() -> tuple[str, str, str]:
    client_id = str(os.getenv("KALSTROP_CLIENT_ID") or "").strip()
    shared_secret_raw = str(os.getenv("KALSTROP_SHARED_SECRET_RAW") or "").strip()
    if client_id and shared_secret_raw:
        return (client_id, shared_secret_raw, "kalstrop_prefixed")

    legacy_client_id = str(os.getenv("CLIENT_ID") or "").strip()
    legacy_shared_secret_raw = str(os.getenv("SHARED_SECRET_RAW") or "").strip()
    if legacy_client_id and legacy_shared_secret_raw:
        return (legacy_client_id, legacy_shared_secret_raw, "legacy_generic")
    return ("", "", "")


def build_sports_provider(
    *,
    provider_name: str,
    recorder: UpdateRecorder | None = None,
    raw_frame_recorder: RawFrameRecorder | None = None,
    logger: logging.Logger | None = None,
) -> SportsDataProviderBase:
    p = str(provider_name or "").strip().lower()
    if p == "boltodds":
        api_key = str(os.getenv("BOLTODDS_API_KEY") or "").strip()
        if not api_key:
            raise ValueError("missing_BOLTODDS_API_KEY")
        return BoltOddsProvider(
            config=BoltOddsProviderConfig(api_key=api_key),
            recorder=recorder,
            raw_frame_recorder=raw_frame_recorder,
        )

    if p == "kalstrop":
        client_id, shared_secret_raw, source = resolve_kalstrop_credentials_from_env()
        if not client_id or not shared_secret_raw:
            raise ValueError("missing_kalstrop_credentials")
        if logger is not None:
            logger.info("Kalstrop credentials source=%s", source)
        http_base = str(os.getenv("KALSTROP_BASE_URL") or "https://sportsapi.kalstropservice.com/odds_v1/v1").strip()
        ws_url = str(os.getenv("KALSTROP_WS_URL") or "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws").strip()
        return KalstropProvider(
            config=KalstropProviderConfig(
                client_id=client_id,
                shared_secret_raw=shared_secret_raw,
                http_base=http_base,
                ws_url=ws_url,
            ),
            recorder=recorder,
            raw_frame_recorder=raw_frame_recorder,
        )

    raise ValueError(f"unsupported_provider:{p}")


def capture_stream_profile(provider_name: str) -> dict[str, bool]:
    p = str(provider_name or "").strip().lower()
    if p == "boltodds":
        return {"scores": True, "odds": False, "playbyplay": True}
    if p == "kalstrop":
        return {"scores": True, "odds": True, "playbyplay": False}
    return {"scores": False, "odds": False, "playbyplay": False}


__all__ = [
    "build_sports_provider",
    "capture_stream_profile",
    "resolve_kalstrop_credentials_from_env",
]
