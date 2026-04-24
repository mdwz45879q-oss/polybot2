"""Optional stream update recorders."""

from __future__ import annotations

import base64
from abc import ABC, abstractmethod
from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from polybot2.sports.contracts import OddsUpdateEvent, PlayByPlayUpdateEvent, ScoreUpdateEvent, StreamEnvelope


def _sanitize_component(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "unknown"
    out = []
    for ch in text:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("_")
    sanitized = "".join(out).strip("_")
    return sanitized or "unknown"


class UpdateRecorder(ABC):
    @abstractmethod
    def record(self, envelope: StreamEnvelope) -> Path | None:
        raise NotImplementedError

    def close(self) -> None:
        return None


class RawFrameRecorder(ABC):
    @abstractmethod
    def record_raw(
        self,
        *,
        provider: str,
        stream: str,
        received_ts: int,
        universal_id: str = "",
        game_label: str = "",
        raw_frame: Any = None,
        parsed_frame: Any = None,
        parse_error: str = "",
    ) -> Path | None:
        raise NotImplementedError

    def close(self) -> None:
        return None


class NullRecorder(UpdateRecorder):
    def record(self, envelope: StreamEnvelope) -> Path | None:
        del envelope
        return None


class NullRawFrameRecorder(RawFrameRecorder):
    def record_raw(
        self,
        *,
        provider: str,
        stream: str,
        received_ts: int,
        universal_id: str = "",
        game_label: str = "",
        raw_frame: Any = None,
        parsed_frame: Any = None,
        parse_error: str = "",
    ) -> Path | None:
        del provider, stream, received_ts, universal_id, game_label, raw_frame, parsed_frame, parse_error
        return None


class JsonlUpdateRecorder(UpdateRecorder):
    """Append-only JSONL recorder partitioned by provider/stream/date/game."""

    def __init__(self, root: str | Path):
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._counts: dict[str, int] = {"total": 0}

    def _path_for(self, envelope: StreamEnvelope) -> Path:
        dt = datetime.fromtimestamp(int(envelope.received_ts), tz=timezone.utc)
        date_part = dt.strftime("%Y-%m-%d")
        return (
            self._root
            / f"provider={_sanitize_component(envelope.provider)}"
            / f"stream={_sanitize_component(envelope.stream)}"
            / f"date={date_part}"
            / f"game={_sanitize_component(envelope.universal_id)}.jsonl"
        )

    @staticmethod
    def _event_to_dict(event: OddsUpdateEvent | ScoreUpdateEvent | PlayByPlayUpdateEvent) -> dict[str, Any]:
        payload = asdict(event)
        return payload

    def record(self, envelope: StreamEnvelope) -> Path:
        path = self._path_for(envelope)
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "provider": envelope.provider,
            "stream": envelope.stream,
            "universal_id": envelope.universal_id,
            "payload_kind": envelope.payload_kind,
            "received_ts": int(envelope.received_ts),
            "dedup_key": envelope.dedup_key,
            "event": self._event_to_dict(envelope.event),
        }
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, separators=(",", ":"), sort_keys=False, default=str))
            f.write("\n")
        stream = str(envelope.stream or "unknown")
        self._counts["total"] = int(self._counts.get("total", 0)) + 1
        self._counts[stream] = int(self._counts.get(stream, 0)) + 1
        return path

    def stats(self) -> dict[str, int]:
        return dict(self._counts)


class JsonlRawFrameRecorder(RawFrameRecorder):
    """Append-only JSONL recorder for raw websocket frames."""

    def __init__(self, root: str | Path, *, default_universal_id: str = ""):
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._default_universal_id = str(default_universal_id or "").strip()
        self._counts: dict[str, int] = {"total": 0}

    def _path_for(self, *, provider: str, stream: str, universal_id: str) -> Path:
        uid = str(universal_id or "").strip() or self._default_universal_id or "unknown"
        return (
            self._root
            / f"provider={_sanitize_component(provider)}"
            / f"stream={_sanitize_component(stream)}"
            / f"game={_sanitize_component(uid)}.jsonl"
        )

    @staticmethod
    def _serialize_raw(raw_frame: Any) -> dict[str, Any]:
        if isinstance(raw_frame, (bytes, bytearray)):
            data = bytes(raw_frame)
            return {
                "encoding": "bytes",
                "utf8": data.decode("utf-8", errors="replace"),
                "base64": base64.b64encode(data).decode("ascii"),
            }
        if isinstance(raw_frame, str):
            return {"encoding": "text", "text": raw_frame}
        return {"encoding": "object", "json": raw_frame}

    def record_raw(
        self,
        *,
        provider: str,
        stream: str,
        received_ts: int,
        universal_id: str = "",
        game_label: str = "",
        raw_frame: Any = None,
        parsed_frame: Any = None,
        parse_error: str = "",
    ) -> Path:
        path = self._path_for(provider=str(provider), stream=str(stream), universal_id=str(universal_id))
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "provider": str(provider),
            "stream": str(stream),
            "received_ts": int(received_ts),
            "universal_id": str(universal_id or "").strip() or self._default_universal_id,
            "game_label": str(game_label or ""),
            "raw": self._serialize_raw(raw_frame),
            "parsed_frame": parsed_frame,
            "parse_error": str(parse_error or ""),
        }
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, separators=(",", ":"), sort_keys=False, default=str))
            f.write("\n")
        stream_key = str(stream or "unknown")
        self._counts["total"] = int(self._counts.get("total", 0)) + 1
        self._counts[stream_key] = int(self._counts.get(stream_key, 0)) + 1
        return path

    def stats(self) -> dict[str, int]:
        return dict(self._counts)


__all__ = [
    "UpdateRecorder",
    "RawFrameRecorder",
    "NullRecorder",
    "NullRawFrameRecorder",
    "JsonlUpdateRecorder",
    "JsonlRawFrameRecorder",
]
