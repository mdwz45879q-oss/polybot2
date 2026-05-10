"""Live terminal observer for the hotpath JSONL log file.

Reads the log with tail -f semantics and renders a fixed-position
terminal scoreboard that redraws in place. No scrolling.
"""

from __future__ import annotations

import glob
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from polybot2.hotpath.contracts import CompiledPlan


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass
class GameRow:
    gid: str
    home: int | None = None
    away: int | None = None
    inning: int | None = None
    half: str = ""
    game_state: str = ""
    last_ts: int = 0


@dataclass
class OrderRow:
    ts: int
    strategy_key: str
    token_id: str = ""
    ok: bool = False
    exchange_id: str = ""
    error: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_matchup_map(plan: CompiledPlan | None) -> tuple[dict[str, str], bool]:
    """Build gid -> matchup label using Polymarket codes.

    Returns (matchup_map, is_baseball). Baseball uses AWAY-HOME convention;
    soccer and other sports use HOME-AWAY.
    """
    if plan is None:
        return {}, False
    try:
        from polybot2.linking.mapping_loader import load_mapping
        mapping = load_mapping()
        team_map = mapping.team_map.get(plan.league, {})
    except Exception:
        return {}, False
    code_by_name: dict[str, str] = {}
    for canonical_name, info in team_map.items():
        code = info.get("polymarket_code", "")
        if code:
            code_by_name[canonical_name.lower()] = code.upper()

    # Determine sport family from league config
    try:
        league_cfg = mapping.leagues.get(plan.league, {})
        sport_family = str(league_cfg.get("sport_family", "")).strip().lower()
    except Exception:
        sport_family = ""
    is_baseball = sport_family == "baseball"

    matchups: dict[str, str] = {}
    for game in plan.games:
        home_code = code_by_name.get(
            game.canonical_home_team.lower(),
            game.canonical_home_team[:3].upper(),
        )
        away_code = code_by_name.get(
            game.canonical_away_team.lower(),
            game.canonical_away_team[:3].upper(),
        )
        if is_baseball:
            matchups[game.provider_game_id] = f"{away_code}-{home_code}"
        else:
            matchups[game.provider_game_id] = f"{home_code}-{away_code}"
    return matchups, is_baseball


def _bet_label(sk: str) -> str:
    """Parse strategy key into human-readable bet label.

    'gid:TOTAL:OVER:8.5'                    -> 'OVER 8.5'
    'gid:NRFI:YES'                           -> 'NRFI YES'
    'gid:MONEYLINE:HOME'                     -> 'ML HOME'
    'gid:SPREAD:HOME:-1.5'                   -> 'SPR HOME -1.5'
    'gid:SOCCER_HALFTIME_RESULT:HOME_YES'    -> 'HT HOME_YES'
    'gid:BTTS:YES'                           -> 'BTTS YES'
    'gid:TOTAL_CORNERS:OVER:8.5'             -> 'CRN OVER 8.5'
    'gid:EXACT_SCORE:1_0'                    -> 'EXACT 1-0'
    """
    parts = sk.split(":")
    if len(parts) < 3:
        return sk
    market = parts[1]
    side = parts[2]
    line = parts[3] if len(parts) > 3 else ""
    if market == "TOTAL":
        return f"{side} {line}".strip()
    if market == "NRFI":
        return f"NRFI {side}"
    if market == "MONEYLINE":
        return f"ML {side}"
    if market == "SPREAD":
        return f"SPR {side} {line}".strip()
    if market == "SOCCER_HALFTIME_RESULT":
        if side.startswith("0x"):
            return f"HT {side[:8]}.."
        return f"HT {side}"
    if market == "BTTS":
        return f"BTTS {side}"
    if market == "TOTAL_CORNERS":
        if side.startswith("0x"):
            return f"CRN {side[:8]}.."
        return f"CRN {side} {line}".strip()
    if market == "EXACT_SCORE":
        # Keys: EXACT_SCORE:1_3:YES, EXACT_SCORE:ANY_OTHER:NO
        yes_no = f" {line}" if line else ""
        if side == "ANY_OTHER":
            return f"EXACT OTHER{yes_no}"
        return f"EXACT {side.replace('_', '-')}{yes_no}"
    # Fallback: truncate hex condition_id if present
    if side.startswith("0x") and len(side) > 10:
        return f"{market} {side[:8]}.."
    return f"{market} {side}"


def _format_inning(inn: int | None, half: str) -> str:
    """Format period display. Baseball uses inning numbers; soccer uses half names."""
    if inn is not None:
        prefix = {"top": "T", "bottom": "B", "break": "Brk"}.get(half, "")
        return f"{prefix}{inn}"
    # Soccer: no inning number, use half directly
    h = half.strip()
    if not h:
        return "--"
    half_map = {
        "1st half": "1H",
        "2nd half": "2H",
        "Halftime": "HT",
        "Ended": "FT",
    }
    return half_map.get(h, h[:7])


def _now_ms() -> int:
    return int(time.time() * 1000)


def find_latest_log(log_dir: str, run_id: int | None = None) -> str | None:
    """Find the most recent hotpath_*.jsonl in the directory."""
    if run_id is not None:
        pattern = f"hotpath_{run_id}_*.jsonl"
    else:
        pattern = "hotpath_*.jsonl"
    files = sorted(glob.glob(os.path.join(log_dir, pattern)), reverse=True)
    return files[0] if files else None


# ---------------------------------------------------------------------------
# Observer
# ---------------------------------------------------------------------------

class LiveObserver:
    """Tail a hotpath JSONL log file and render an in-place terminal scoreboard."""

    def __init__(
        self,
        log_path: str,
        compiled_plan: CompiledPlan | None = None,
    ):
        self.log_path = log_path
        self.games: dict[str, GameRow] = {}
        self.orders: list[OrderRow] = []
        self.ws_status = "DOWN"
        self.reconnects = 0
        self.startup_ts: int | None = None
        self.run_id: int = 0
        self.mode: str = ""
        self.matchup_by_gid, self.is_baseball = _build_matchup_map(compiled_plan)

    def run(self) -> None:
        """Tail the log file and redraw on each new line. Blocks forever."""
        import signal
        stop = False
        def _on_signal(signum: int, frame: Any) -> None:
            nonlocal stop
            stop = True
        signal.signal(signal.SIGINT, _on_signal)
        signal.signal(signal.SIGTERM, _on_signal)

        with open(self.log_path, "r") as f:
            # Catch up on existing lines
            for line in f:
                stripped = line.strip()
                if stripped:
                    self._process_line(stripped)
            self._redraw()
            # Tail for new lines
            while not stop:
                line = f.readline()
                if line:
                    stripped = line.strip()
                    if stripped:
                        self._process_line(stripped)
                        self._redraw()
                else:
                    time.sleep(0.1)

    def _process_line(self, line: str) -> None:
        try:
            ev: dict[str, Any] = json.loads(line)
        except json.JSONDecodeError:
            return
        event_type = ev.get("ev", "")
        if event_type == "tick":
            self._on_tick(ev)
        elif event_type == "order":
            self._on_order(ev)
        elif event_type == "startup":
            self._on_startup(ev)
        elif event_type == "ws_connect":
            self.ws_status = "UP"
        elif event_type == "ws_disconnect":
            self.ws_status = "DOWN"
            self.reconnects = int(ev.get("reconnects", 0))

    def _on_tick(self, ev: dict[str, Any]) -> None:
        gid = str(ev.get("gid", ""))
        if not gid:
            return
        self.games[gid] = GameRow(
            gid=gid,
            home=ev.get("h"),
            away=ev.get("a"),
            inning=ev.get("inn"),
            half=str(ev.get("half", "")),
            game_state=str(ev.get("gs", "")),
            last_ts=int(ev.get("ts", 0)),
        )

    def _on_order(self, ev: dict[str, Any]) -> None:
        self.orders.append(OrderRow(
            ts=int(ev.get("ts", 0)),
            strategy_key=str(ev.get("sk", "")),
            token_id=str(ev.get("tok", "")),
            ok=bool(ev.get("ok", False)),
            exchange_id=str(ev.get("eid", "")),
            error=str(ev.get("err", "")),
        ))

    def _on_startup(self, ev: dict[str, Any]) -> None:
        self.startup_ts = int(ev.get("ts", 0))
        self.run_id = int(ev.get("run_id", 0))
        self.mode = str(ev.get("mode", ""))

    # -----------------------------------------------------------------------
    # Rendering
    # -----------------------------------------------------------------------

    def _format_score(self, g: GameRow) -> str:
        if g.home is None and g.away is None:
            return "--"
        if self.is_baseball:
            return f"{g.away or 0}-{g.home or 0}"   # baseball: away-home
        return f"{g.home or 0}-{g.away or 0}"       # soccer: home-away

    def _redraw(self) -> None:
        print("\x1b[2J\x1b[H", end="")
        self._print_header()
        self._print_games()
        self._print_orders()
        sys.stdout.flush()

    def _print_header(self) -> None:
        uptime = ""
        if self.startup_ts:
            elapsed_s = max(0, (_now_ms() - self.startup_ts) // 1000)
            h = elapsed_s // 3600
            m = (elapsed_s % 3600) // 60
            s = elapsed_s % 60
            uptime = f"{h:02d}:{m:02d}:{s:02d}"

        n_orders = len(self.orders)
        n_games = len(self.games)
        ws = f"ws:{self.ws_status}"
        mode_str = f" [{self.mode}]" if self.mode else ""
        print(
            f"polybot2 | run {self.run_id}{mode_str} | {ws} "
            f"| {n_games} games | {n_orders} orders | uptime {uptime}"
        )
        print()

    def _print_games(self) -> None:
        period_col = "INN" if self.is_baseball else "HALF"
        print(f" {'GAME':<15} {period_col:<7} {'SCORE':<8} BETS")
        # Sort: LIVE first (by most recent update), then FINAL, then NOT STARTED
        def sort_key(item: tuple[str, GameRow]) -> tuple[int, int]:
            g = item[1]
            if g.game_state == "LIVE":
                return (0, -g.last_ts)
            if g.game_state == "FINAL":
                return (1, -g.last_ts)
            return (2, -g.last_ts)

        for gid, g in sorted(self.games.items(), key=sort_key):
            matchup = self.matchup_by_gid.get(gid, gid[:12])
            inn = _format_inning(g.inning, g.half)
            if g.game_state == "FINAL":
                inn = "FINAL"
            elif g.game_state not in ("LIVE", ""):
                inn = "--"
            score = self._format_score(g) if g.game_state in ("LIVE", "FINAL", "") else "--"

            # Collect bets for this game
            game_bets = [
                o for o in self.orders
                if o.strategy_key.startswith(gid + ":")
            ]
            if game_bets:
                bet_str = "  ".join(
                    f"{_bet_label(o.strategy_key)} {'✓' if o.ok else '✗'}"
                    for o in game_bets
                )
            else:
                bet_str = "--"

            print(f" {matchup:<15} {inn:<7} {score:<8} {bet_str}")
        print()

    def _print_orders(self) -> None:
        if not self.orders:
            return
        print(f" ORDERS ({len(self.orders)})")
        for o in self.orders:
            ts_str = datetime.fromtimestamp(o.ts / 1000).strftime("%H:%M:%S")
            gid = o.strategy_key.split(":")[0] if ":" in o.strategy_key else ""
            matchup = self.matchup_by_gid.get(gid, gid[:12])
            label = _bet_label(o.strategy_key)
            if o.ok:
                status = f"ok     eid={o.exchange_id[:16]}"
            else:
                status = f"FAIL   {o.error[:50]}"
            print(f" {ts_str}  {matchup:<12} {label:<20} {status}")


__all__ = ["LiveObserver", "find_latest_log"]
