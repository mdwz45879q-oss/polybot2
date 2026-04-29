"""Read hotpath JSONL logs to extract runtime state for plan refresh."""

from __future__ import annotations

import json
from pathlib import Path


def read_fired_strategy_keys(log_path: str | Path) -> set[str]:
    """Read a hotpath JSONL log and return strategy keys for all
    successfully dispatched orders (ev=order, ok=true)."""
    fired: set[str] = set()
    path = Path(log_path)
    if not path.exists():
        return fired
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                continue
            if record.get("ev") != "order":
                continue
            if not record.get("ok"):
                continue
            sk = str(record.get("sk") or "").strip()
            if sk:
                fired.add(sk)
    return fired


def read_all_fired_strategy_keys(log_dir: str | Path, run_id: int) -> set[str]:
    """Read all hotpath logs for a run_id and collect fired strategy keys.

    Each hotpath start() creates a new log file. This function reads all of
    them to recover the full set of fired keys across restart cycles.
    """
    fired: set[str] = set()
    log_dir_path = Path(log_dir)
    if not log_dir_path.is_dir():
        return fired
    for log_file in log_dir_path.glob(f"hotpath_{run_id}_*.jsonl"):
        fired |= read_fired_strategy_keys(log_file)
    return fired


def find_latest_hotpath_log(log_dir: str | Path, run_id: int | None = None) -> Path | None:
    """Find the most recent hotpath JSONL log in the directory."""
    log_dir_path = Path(log_dir)
    if not log_dir_path.is_dir():
        return None
    pattern = f"hotpath_{run_id}_*.jsonl" if run_id is not None else "hotpath_*.jsonl"
    files = sorted(log_dir_path.glob(pattern), reverse=True)
    return files[0] if files else None
