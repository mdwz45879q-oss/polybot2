"""Base interface and shared behaviors for sports data providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
import hashlib
import json
import queue
import threading
from typing import Any, Callable, Sequence

import httpx

from polybot2.sports.contracts import ProviderGameRecord, SportsProviderConfig, StreamEnvelope
from polybot2.sports.recorder import NullRecorder, UpdateRecorder


Callback = Callable[[StreamEnvelope], None]


class SportsDataProviderBase(ABC):
    """Abstract provider contract with shared queue/record/dedup behavior."""

    def __init__(
        self,
        *,
        config: SportsProviderConfig,
        recorder: UpdateRecorder | None = None,
        http_client: httpx.Client | None = None,
    ):
        self._config = config
        self._recorder = recorder or NullRecorder()
        self._client = http_client or httpx.Client(timeout=float(config.request_timeout_seconds))
        self._event_queue: queue.Queue[StreamEnvelope] = queue.Queue(maxsize=int(config.queue_maxsize))
        self._callbacks: dict[str, list[Callback]] = {"odds": [], "scores": [], "playbyplay": [], "all": []}
        self._started = False
        self._lock = threading.RLock()

        self._dedup_recent: deque[str] = deque()
        self._dedup_index: set[str] = set()
        self._dedup_max = 200_000

    @property
    def provider_name(self) -> str:
        return str(self._config.provider_name)

    @property
    def is_started(self) -> bool:
        with self._lock:
            return bool(self._started)

    def register_callback(self, stream: str, callback: Callback) -> None:
        key = str(stream or "").strip().lower()
        if key not in {"odds", "scores", "playbyplay", "all"}:
            raise ValueError("stream must be one of {'odds','scores','playbyplay','all'}")
        self._callbacks[key].append(callback)

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._on_start()
            self._started = True

    def close(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._on_close()
            self._started = False
        try:
            self._client.close()
        except Exception:
            pass
        try:
            self._recorder.close()
        except Exception:
            pass

    def _on_start(self) -> None:
        return None

    def _on_close(self) -> None:
        return None

    def _enqueue(self, envelope: StreamEnvelope) -> None:
        if envelope.dedup_key in self._dedup_index:
            return
        self._dedup_index.add(envelope.dedup_key)
        self._dedup_recent.append(envelope.dedup_key)
        if len(self._dedup_recent) > self._dedup_max:
            old = self._dedup_recent.popleft()
            self._dedup_index.discard(old)

        try:
            self._recorder.record(envelope)
        except Exception:
            pass

        try:
            self._event_queue.put_nowait(envelope)
        except queue.Full:
            try:
                self._event_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._event_queue.put_nowait(envelope)
            except queue.Full:
                pass

        for fn in self._callbacks.get("all", []):
            fn(envelope)
        for fn in self._callbacks.get(envelope.stream, []):
            fn(envelope)

    def build_dedup_key(
        self,
        *,
        stream: str,
        universal_id: str,
        payload_kind: str,
        provider_timestamp: str,
        raw_payload: dict[str, Any],
    ) -> str:
        h = hashlib.sha1()
        h.update(str(self.provider_name).encode("utf-8"))
        h.update(b"|")
        h.update(str(stream).encode("utf-8"))
        h.update(b"|")
        h.update(str(universal_id).encode("utf-8"))
        h.update(b"|")
        h.update(str(payload_kind).encode("utf-8"))
        h.update(b"|")
        h.update(str(provider_timestamp).encode("utf-8"))
        h.update(b"|")
        try:
            raw = json.dumps(raw_payload, sort_keys=True, separators=(",", ":"), default=str)
        except Exception:
            raw = str(raw_payload)
        h.update(raw.encode("utf-8", errors="ignore"))
        return h.hexdigest()

    def drain_events(self, *, max_items: int = 1000, timeout_seconds: float = 0.0) -> list[StreamEnvelope]:
        out: list[StreamEnvelope] = []
        remaining = int(max(0, max_items))
        if remaining <= 0:
            return out

        first_timeout = max(0.0, float(timeout_seconds))
        try:
            first = self._event_queue.get(timeout=first_timeout)
            out.append(first)
            remaining -= 1
        except queue.Empty:
            return out

        while remaining > 0:
            try:
                nxt = self._event_queue.get_nowait()
            except queue.Empty:
                break
            out.append(nxt)
            remaining -= 1
        return out

    def get_provider_record(self, provider_game_id: str) -> ProviderGameRecord | None:
        uid = str(provider_game_id or "").strip()
        if not uid:
            return None
        return self._get_provider_record(uid)

    def pop_last_stream_timing(self, *, stream: str) -> dict[str, int]:
        """Optional hook for providers to expose last stream-stage timings."""
        del stream
        return {}

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

    @abstractmethod
    def subscribe_scores(self, universal_ids: Sequence[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    def subscribe_odds(self, universal_ids: Sequence[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    def subscribe_playbyplay(self, universal_ids: Sequence[str]) -> None:
        raise NotImplementedError

    @abstractmethod
    def stream_scores(self, *, read_timeout_seconds: float = 1.0) -> list[StreamEnvelope]:
        raise NotImplementedError

    @abstractmethod
    def stream_odds(self, *, read_timeout_seconds: float = 1.0) -> list[StreamEnvelope]:
        raise NotImplementedError

    @abstractmethod
    def stream_playbyplay(self, *, read_timeout_seconds: float = 1.0) -> list[StreamEnvelope]:
        raise NotImplementedError


__all__ = ["SportsDataProviderBase", "Callback"]
