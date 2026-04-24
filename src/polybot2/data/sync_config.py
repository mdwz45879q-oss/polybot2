"""Sync configuration contracts for polybot2."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketSyncConfig:
    gamma_api: str = "https://gamma-api.polymarket.com"
    batch_size: int = 500
    timeout: float = 30.0
    request_delay: float = 0.0
    concurrency: int = 20
    max_rps: int = 48
    fetch_max_retries: int = 3
    resolved_max_pages: int | None = None
    open_max_pages: int | None = None
    open_only: bool = False
    enable_reference_sync: bool = True
    enable_payload_artifacts: bool = False
    fast_mode: bool = False
    payload_artifact_dir: str = "artifacts/polybot2_market_payloads"


@dataclass(frozen=True)
class ProviderSyncConfig:
    boltodds_http_base: str = "https://spro.agency/api"
    request_timeout_seconds: float = 20.0


@dataclass(frozen=True)
class DataSyncConfigs:
    markets: MarketSyncConfig = MarketSyncConfig()
    providers: ProviderSyncConfig = ProviderSyncConfig()
