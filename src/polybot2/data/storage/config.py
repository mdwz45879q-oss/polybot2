"""Runtime config contracts for polybot2."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any


@dataclass(frozen=True)
class DataRuntimeConfig:
    db_path: str = "data/prediction_markets.db"
    gamma_api: str = "https://gamma-api.polymarket.com"
    http_timeout_s: float = 30.0
    db_batch_size: int = 500
    db_wal_mode: bool = True

    def __post_init__(self) -> None:
        if not str(self.db_path).strip():
            raise ValueError("db_path must be non-empty")
        if not str(self.gamma_api).strip():
            raise ValueError("gamma_api must be non-empty")
        if float(self.http_timeout_s) <= 0.0:
            raise ValueError("http_timeout_s must be > 0")
        if int(self.db_batch_size) <= 0:
            raise ValueError("db_batch_size must be > 0")

    @property
    def infra(self) -> "DataRuntimeConfig":
        return self

    @classmethod
    def from_env(cls, overrides: dict[str, Any] | None = None) -> "DataRuntimeConfig":
        vals: dict[str, Any] = {}
        if (v := os.getenv("POLYBOT2_DB_PATH")) is not None:
            vals["db_path"] = v
        if (v := os.getenv("POLYBOT2_GAMMA_API")) is not None:
            vals["gamma_api"] = v
        if (v := os.getenv("POLYBOT2_HTTP_TIMEOUT_S")) is not None:
            vals["http_timeout_s"] = float(v)
        if (v := os.getenv("POLYBOT2_DB_BATCH_SIZE")) is not None:
            vals["db_batch_size"] = int(v)
        if (v := os.getenv("POLYBOT2_DB_WAL_MODE")) is not None:
            vals["db_wal_mode"] = str(v).strip().lower() not in {"0", "false", "no", "off"}
        if overrides:
            vals.update(overrides)
        return cls(**vals)
