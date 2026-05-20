"""JSONL log tail-follow for the hotpath log file.

Extracted from live_observer.py's tail pattern. Yields parsed
JSON events as dicts, blocking on new lines.
"""

from __future__ import annotations

import json
import time
from typing import Any, Iterator


def tail_log(log_path: str, *, poll_interval: float = 0.1) -> Iterator[dict[str, Any]]:
    """Tail a hotpath JSONL log file, yielding parsed events.

    Catches up on existing lines first, then blocks waiting for new lines.
    Uses readline() + sleep polling (same pattern as LiveObserver).
    """
    with open(log_path, "r") as f:
        while True:
            line = f.readline()
            if line:
                stripped = line.strip()
                if stripped:
                    try:
                        yield json.loads(stripped)
                    except json.JSONDecodeError:
                        continue
            else:
                time.sleep(poll_interval)


def read_log_snapshot(log_path: str) -> list[dict[str, Any]]:
    """Read all events from a hotpath JSONL log file (non-blocking).

    Returns a list of parsed events. Does not tail for new lines.
    """
    events: list[dict[str, Any]] = []
    with open(log_path, "r") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                try:
                    events.append(json.loads(stripped))
                except json.JSONDecodeError:
                    continue
    return events
