"""Base interface and shared behaviors for sports data providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
import threading
from typing import Any, Sequence

import httpx

from polybot2.sports.contracts import ProviderGameRecord, SportsProviderConfig


class SportsDataProviderBase(ABC):
    """Abstract provider contract for catalog and resolution."""

    def __init__(
        self,
        *,
        config: SportsProviderConfig,
        http_client: httpx.Client | None = None,
    ):
        self._config = config
        self._client = http_client or httpx.Client(timeout=float(config.request_timeout_seconds))
        self._lock = threading.RLock()

    @property
    def provider_name(self) -> str:
        return str(self._config.provider_name)

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    @abstractmethod
    def _get_provider_record(self, provider_game_id: str) -> ProviderGameRecord | None:
        raise NotImplementedError

    @abstractmethod
    def load_game_catalog(self) -> list[ProviderGameRecord]:
        raise NotImplementedError

    @abstractmethod
    def resolve_universal_ids(
        self,
        *,
        game_labels: Sequence[str] | None = None,
        universal_ids: Sequence[str] | None = None,
    ) -> list[str]:
        raise NotImplementedError


__all__ = ["SportsDataProviderBase"]
