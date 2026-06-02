"""Self-contained JSONL logger for the guardian system.

Every entry is interpretable with only the session_start header for context.
No cross-referencing with other log files needed for analysis.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger("polybot2.guardian")


class GuardianLogger:
    """Structured JSONL logger for guardian events.

    Two files in guardian_logs/:
    - guardian_{ts}.jsonl — events (low volume, human-scannable)
    - guardian_prices_{ts}.jsonl — price snapshots (high volume, 1/s)

    Both include the session_start header so each is self-contained.
    """

    def __init__(self, log_dir: str = ".") -> None:
        guardian_log_dir = os.path.join(log_dir, "guardian_logs")
        os.makedirs(guardian_log_dir, exist_ok=True)
        ts_str = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
        self._event_path = os.path.join(guardian_log_dir, f"guardian_{ts_str}.jsonl")
        self._price_path = os.path.join(guardian_log_dir, f"guardian_prices_{ts_str}.jsonl")
        self._event_file = open(self._event_path, "a")
        self._price_file = open(self._price_path, "a")
        logger.info("guardian event log: %s", self._event_path)
        logger.info("guardian price log: %s", self._price_path)

    @property
    def log_path(self) -> str:
        return self._event_path

    def _write_event(self, data: dict[str, Any]) -> None:
        data["ts"] = int(time.time() * 1000)
        try:
            self._event_file.write(json.dumps(data, default=str) + "\n")
            self._event_file.flush()
        except Exception:
            pass

    def _write_price(self, data: dict[str, Any]) -> None:
        data["ts"] = int(time.time() * 1000)
        try:
            self._price_file.write(json.dumps(data, default=str) + "\n")
            self._price_file.flush()
        except Exception:
            pass

    # ── Session lifecycle ────────────────────────────────────────────

    def log_session_start(self, header: dict[str, Any]) -> None:
        entry = {"ev": "session_start", **header}
        self._write_event(entry)
        self._write_price({"ev": "session_start", **header})

    # ── Score & match events ─────────────────────────────────────────

    def log_score_change(
        self,
        gid: str,
        home: int,
        away: int,
        prev_home: int,
        prev_away: int,
        half: str,
        var_type: str,
        var_sub: str,
        prices: dict[str, dict[str, float]],
    ) -> None:
        self._write_event({
            "ev": "score_change",
            "gid": gid,
            "score": [home, away],
            "prev_score": [prev_home, prev_away],
            "half": half,
            "var_type": var_type,
            "var_sub": var_sub,
            "prices": prices,
        })

    def log_var_action(self, gid: str, var_type: str, var_sub: str) -> None:
        self._write_event({
            "ev": "var_action",
            "gid": gid,
            "var_type": var_type,
            "var_sub": var_sub,
        })

    # ── Order lifecycle ──────────────────────────────────────────────

    def log_order_attempted(
        self,
        gid: str,
        sk: str,
        tok: str,
        eid: str,
        ok: bool,
        tif: str,
        trigger_score: tuple[int, int] | None,
        trigger_prev: tuple[int, int] | None,
    ) -> None:
        entry: dict[str, Any] = {
            "ev": "order_attempted",
            "gid": gid,
            "sk": sk,
            "tok": tok,
            "eid": eid,
            "ok": ok,
            "tif": tif,
        }
        if trigger_score:
            entry["trigger_score"] = list(trigger_score)
        if trigger_prev:
            entry["trigger_prev"] = list(trigger_prev)
        self._write_event(entry)

    def log_order_filled(
        self, tok: str, eid: str, fill_price: float, fill_size: float, status: str,
    ) -> None:
        self._write_event({
            "ev": "order_filled",
            "tok": tok,
            "eid": eid,
            "fill_price": fill_price,
            "fill_size": fill_size,
            "status": status,
        })

    # ── Price snapshots ──────────────────────────────────────────────

    def log_price_snapshot(self, prices: dict[str, dict[str, float]]) -> None:
        if not prices:
            return
        self._write_price({"ev": "price_snapshot", "prices": prices})

    # ── Overturn detection ───────────────────────────────────────────

    def log_overturn_armed(
        self,
        gid: str,
        original_score: tuple[int, int],
        reversed_score: tuple[int, int],
        affected_tokens: list[str],
        affected_orders: list[dict[str, Any]],
    ) -> None:
        self._write_event({
            "ev": "overturn_armed",
            "gid": gid,
            "original_score": list(original_score),
            "reversed_score": list(reversed_score),
            "affected_tokens": affected_tokens,
            "affected_orders": affected_orders,
        })

    def log_overturn_confirmed(
        self,
        gid: str,
        signal1: bool,
        signal2: bool,
        prices: dict[str, dict[str, float]],
    ) -> None:
        self._write_event({
            "ev": "overturn_confirmed",
            "gid": gid,
            "signal1": signal1,
            "signal2": signal2,
            "prices_at_trigger": prices,
        })

    # ── Guardian execution actions ───────────────────────────────────

    def log_order_cancelled(
        self, gid: str, sk: str, eid: str, ok: bool, dry_run: bool,
    ) -> None:
        self._write_event({
            "ev": "order_cancelled",
            "gid": gid,
            "sk": sk,
            "eid": eid,
            "ok": ok,
            "dry_run": dry_run,
        })

    def log_position_sold(
        self,
        gid: str,
        sk: str,
        tok: str,
        size: float,
        sell_price: float,
        buy_price: float,
        sell_eid: str,
        ok: bool,
        dry_run: bool,
    ) -> None:
        pnl = round((sell_price - buy_price) * size, 4) if buy_price > 0 else None
        self._write_event({
            "ev": "position_sold",
            "gid": gid,
            "sk": sk,
            "tok": tok,
            "size": size,
            "sell_price": sell_price,
            "buy_price": buy_price,
            "pnl": pnl,
            "sell_eid": sell_eid,
            "ok": ok,
            "dry_run": dry_run,
        })

    def log_sell_skipped(self, gid: str, sk: str, tok: str, reason: str) -> None:
        self._write_event({
            "ev": "sell_skipped",
            "gid": gid,
            "sk": sk,
            "tok": tok,
            "reason": reason,
        })

    def close(self) -> None:
        for f in (self._event_file, self._price_file):
            try:
                f.close()
            except Exception:
                pass
