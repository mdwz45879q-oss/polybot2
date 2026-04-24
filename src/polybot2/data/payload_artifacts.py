"""Compressed JSONL payload artifact writer for market metadata sync."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import time
from typing import Any


@dataclass(frozen=True, slots=True)
class PayloadLineage:
    payload_sha256: str
    payload_ref: str
    payload_size_bytes: int


class PayloadArtifactWriter:
    """Write large provider payloads to compressed JSONL artifacts.

    Each row writes an entry keyed by entity id and returns a compact lineage
    tuple for DB storage.
    """

    def __init__(self, *, root_dir: str = "artifacts/market_payloads", run_label: str | None = None):
        ts = str(run_label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        self._root = Path(root_dir).expanduser() / ts
        self._root.mkdir(parents=True, exist_ok=True)
        self._writers: dict[str, Any] = {}
        self._paths: dict[str, Path] = {}
        self._write_elapsed_s: float = 0.0
        self._close_elapsed_s: float = 0.0
        self._write_count: int = 0
        self._bytes_written: int = 0

    def close(self) -> None:
        started = time.perf_counter()
        for handle in self._writers.values():
            try:
                handle.close()
            except Exception:
                pass
        self._writers.clear()
        self._close_elapsed_s += max(0.0, time.perf_counter() - started)

    def _open_stream(self, stream_name: str):
        key = str(stream_name or "default").strip().lower() or "default"
        handle = self._writers.get(key)
        if handle is not None:
            return handle
        path = self._root / f"{key}.jsonl.gz"
        handle = gzip.open(path, mode="at", encoding="utf-8")
        self._writers[key] = handle
        self._paths[key] = path
        return handle

    @property
    def root_dir(self) -> str:
        return str(self._root)

    def write_payload(
        self,
        *,
        stream_name: str,
        entity_key: str,
        payload: dict[str, Any] | list[Any] | str | int | float | bool | None,
    ) -> PayloadLineage:
        started = time.perf_counter()
        payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
        payload_bytes = payload_json.encode("utf-8")
        digest = hashlib.sha256(payload_bytes).hexdigest()

        handle = self._open_stream(stream_name)
        row = {"entity_key": str(entity_key or ""), "payload_sha256": digest, "payload": payload}
        handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True, default=str))
        handle.write("\n")
        self._write_elapsed_s += max(0.0, time.perf_counter() - started)
        self._write_count += 1
        self._bytes_written += int(len(payload_bytes))

        ref_path = str(self._paths[str(stream_name or "default").strip().lower() or "default"])
        return PayloadLineage(
            payload_sha256=str(digest),
            payload_ref=ref_path,
            payload_size_bytes=int(len(payload_bytes)),
        )

    @property
    def write_elapsed_s(self) -> float:
        return float(self._write_elapsed_s)

    @property
    def close_elapsed_s(self) -> float:
        return float(self._close_elapsed_s)

    @property
    def write_count(self) -> int:
        return int(self._write_count)

    @property
    def bytes_written(self) -> int:
        return int(self._bytes_written)


__all__ = ["PayloadArtifactWriter", "PayloadLineage"]
