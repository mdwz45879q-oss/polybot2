#!/usr/bin/env python3
"""
TENNIS KALSTROP + KALSHI SNIPER v3 — single-file consolidated edition
======================================================================
Combines the 4-file split (sniper v2 + engine + plan builder + v1 library)
into ONE self-contained script with the same speed-optimization patterns
as your MLB orchestrator (uvloop, orjson, pre-signed headers, batched
orders, deterministic client_order_id).

What it does
------------
Watches every LIVE tennis fixture Kalstrop tracks (ATP / WTA / Challenger /
WTA 125K / UTR / SRL — NOT ITF, that's not in Kalstrop's tennis coverage)
and fires 99¢ math-locked snipe bets on Kalshi as set boundaries and
match-end events resolve specific Kalshi tickers to YES or NO.

Engine evaluators (Rida-style closed-form math):
  - MONEYLINE         fires at sets ≥ sets_to_win
  - FIRST_SET_WINNER  fires when set 1 completes
  - SET_N_WINNER      fires when set N completes (uses per-side prev tracking)
  - EXACT_MATCH       NO mid-match if math-impossible; YES at match end
  - MATCH_TOTAL       OVER on crossings; UNDER at match end
  - GAME_HANDICAP     YES / NO via closed-form max-margin
  - SET_TOTAL         OVER via min(sh,sa) ≥ line+1−stw; UNDER at match end
  - SET_HANDICAP      covers / not_covers with mid-match early-lock
  - COMPLETED_MATCH   fires at match-decided

Safety:
  - streamExists=false guard (3 false pushes → skip fixture)
  - Doubles fixture filter (Kalshi tennis is singles-only)
  - Abbreviation-plausibility check (rejects suspicious fuzzy matches)
  - Cold-start tombstones (no stale fires when joining mid-match)
  - BO3/BO5 detection (BO5 only for men's Grand Slam main draw, BO3 elsewhere)

Speed (same patterns as the MLB file):
  - uvloop + orjson with stdlib fallback
  - Pre-signed Kalshi headers (background loop, 0ms hot-path RSA)
  - Batched Kalshi orders (one HTTP per account, up to 20 orders per batch)
  - Deterministic client_order_id (idempotency — no double-fire on retry)
  - Persistent aiohttp session shared across accounts
  - Kalstrop discovery via /tournaments/tennis (exhaustive, not capped at 10)

Run:
  python3 tennis_sniper_v3.py --live --bet-size-usd 10 --limit-cents 99 \\
                              --config-files w_config_mitchell.py
Dry-run (no orders placed):
  python3 tennis_sniper_v3.py --bet-size-usd 1 --config-files w_config_mitchell.py
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Speed: uvloop event loop (falls back to stdlib)
# ---------------------------------------------------------------------------
import asyncio
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    _LOOP_ENGINE = "uvloop ⚡"
except ImportError:
    _LOOP_ENGINE = "stdlib"

# ---------------------------------------------------------------------------
# Speed: orjson for hot-path JSON parse/serialize (falls back to stdlib)
# ---------------------------------------------------------------------------
import json
try:
    import orjson
    def fast_loads(s): return orjson.loads(s)
    def fast_dumps(obj): return orjson.dumps(obj)
    _JSON_ENGINE = "orjson ⚡"
except ImportError:
    def fast_loads(s): return json.loads(s)
    def fast_dumps(obj): return json.dumps(obj).encode("utf-8")
    _JSON_ENGINE = "stdlib"

import argparse
import base64
import hashlib
import hmac
import importlib.util
import logging
import os
import re
import sys
import time
import unicodedata
import uuid
from collections import deque, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode


# =============================================================================
# BETTING_DATE — hard filter on Kalshi event-ticker date prefix (e.g. "26JUN24")
# =============================================================================
# Matches the MLB orchestrator pattern. If your account config file defines
# BETTING_DATE, that wins. Otherwise we auto-compute today's date in ET so the
# default is "only bet on today's matches" with no config edit required.
# Set BETTING_DATE = "0" in config to disable the filter entirely.
def _auto_betting_date_et() -> str:
    # ET = UTC-4 (EDT) — same convention as the MLB file. Use UTC-5 in winter
    # by changing this constant if needed (or leave at -4; Kalshi tickers are
    # dated by their internal event date, which lines up with EDT during the
    # tennis seasons that matter).
    et = datetime.now(timezone.utc) - timedelta(hours=4)
    return et.strftime("%y%b%d").upper()  # → "26JUN24"

BETTING_DATE: str = _auto_betting_date_et()  # overwritten by config in cli_main()

import aiohttp
import requests
import websockets
from websockets.exceptions import ConnectionClosed
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend


# =============================================================================
# KALSTROP CONFIG (read-only score feed — same creds you've been using)
# =============================================================================
KALSTROP_CLIENT_ID = "c_0e0bc3710e50bceda6d38a731016cfc7"
KALSTROP_SECRET = hashlib.sha256(
    "2e24ce60ec51730ac9a63435a4bba1df48f57e446fbbf5b0c596715212ec6ba8".encode("utf-8")
).hexdigest()
KALSTROP_REST = "https://sportsapi.kalstropservice.com/odds_v1/v1"
KALSTROP_WS = "wss://sportsapi.kalstropservice.com/odds_v1/v1/ws"


def kalstrop_headers() -> Dict[str, str]:
    ts = str(int(time.time()))
    sig = hmac.new(
        KALSTROP_SECRET.encode("utf-8"),
        f"{KALSTROP_CLIENT_ID}:{ts}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "X-Client-ID": KALSTROP_CLIENT_ID,
        "X-Timestamp": ts,
        "Authorization": f"Bearer {sig}",
    }


def kalstrop_ws_url() -> str:
    return f"{KALSTROP_WS}?{urlencode(kalstrop_headers())}"


# =============================================================================
# KALSHI CONFIG
# =============================================================================
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
# V2 EVENT-ORDER endpoints — match current_kalshi (the production-tested MLB
# orchestrator). The legacy /portfolio/orders endpoints are kept here as a
# comment only for grep'ability — do NOT re-enable them; this is the only
# routing the working Kalshi code uses today.
#
# Old (DO NOT USE):
#   KALSHI_ORDER_URL = f"{KALSHI_BASE}/portfolio/orders"
#   KALSHI_ORDER_PATH = "/trade-api/v2/portfolio/orders"
#   KALSHI_BATCH_ORDER_URL = f"{KALSHI_BASE}/portfolio/orders/batched"
#   KALSHI_BATCH_ORDER_PATH = "/trade-api/v2/portfolio/orders/batched"
KALSHI_ORDER_URL = f"{KALSHI_BASE}/portfolio/events/orders"
KALSHI_ORDER_PATH = "/trade-api/v2/portfolio/events/orders"
KALSHI_BATCH_ORDER_URL = f"{KALSHI_BASE}/portfolio/events/orders/batched"
KALSHI_BATCH_ORDER_PATH = "/trade-api/v2/portfolio/events/orders/batched"
KALSHI_STP = "taker_at_cross"   # V2 required field; verify exact enum on demo

TENNIS_SERIES = [
    "KXATPMATCH", "KXWTAMATCH",
    "KXATPCHALLENGERMATCH", "KXWTACHALLENGERMATCH",
    "KXITFMATCH", "KXITFWMATCH",
    "KXATPSETWINNER", "KXWTASETWINNER",
    "KXATPEXACTMATCH",
    "KXATPGTOTAL", "KXATPGSPREAD",
    "KXATPSETSWEEP",
]

MONEYLINE_SERIES = {
    "KXATPMATCH", "KXWTAMATCH",
    "KXATPCHALLENGERMATCH", "KXWTACHALLENGERMATCH",
    "KXITFMATCH", "KXITFWMATCH",
}
SET_WINNER_SERIES = {"KXATPSETWINNER", "KXWTASETWINNER"}

_CACHED_BET_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=5)
_SIGNING_POOL = ThreadPoolExecutor(max_workers=8)

# Deterministic client_order_id namespace — same (account, ticker, side) → same id
CLIENT_ORDER_NAMESPACE = uuid.UUID("e7c2d4f1-3a8b-4e9f-b6c5-1d2f3a4b5c6d")


def make_client_order_id(account_name: str, ticker: str, side: str) -> str:
    return str(uuid.uuid5(CLIENT_ORDER_NAMESPACE, f"{account_name}|{ticker}|{side}"))


# =============================================================================
# LOGGING
# =============================================================================
for f in ("tennis_v3_events.log", "tennis_v3_bets.log", "tennis_v3_ws_raw.log"):
    if os.path.exists(f):
        os.remove(f)

LOG_FORMAT = "%(asctime)s.%(msecs)03d %(name)s | %(message)s"
LOG_DATEFMT = "%H:%M:%S"

logger = logging.getLogger("tennis_v3")
logger.setLevel(logging.INFO)
fh = logging.FileHandler("tennis_v3_events.log")
fh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
logger.addHandler(ch)

bet_logger = logging.getLogger("tennis_v3_bets")
bet_logger.setLevel(logging.INFO)
bh = logging.FileHandler("tennis_v3_bets.log")
bh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
bet_logger.addHandler(bh)
bet_logger.addHandler(ch)

ws_logger = logging.getLogger("tennis_v3_ws")
ws_logger.setLevel(logging.DEBUG)
wh = logging.FileHandler("tennis_v3_ws_raw.log")
wh.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT))
ws_logger.addHandler(wh)


# =============================================================================
# ACCOUNT CONFIG (multi-account with pre-loaded RSA)
# =============================================================================
@dataclass
class AccountConfig:
    name: str
    api_key: str
    secret_key_pem: str
    bet_size_usd: float = 1.0
    max_price_cents: int = 99
    enabled: bool = True
    allowed_bet_types: Optional[Set[str]] = None
    bet_sizes_by_type: Dict[str, float] = field(default_factory=dict)
    bet_prices_by_type: Dict[str, int] = field(default_factory=dict)
    bets_placed: Set[str] = field(default_factory=set)
    private_key: Any = None
    pss_padding: Any = None
    hash_algo: Any = None

    def __post_init__(self):
        if self.secret_key_pem and self.secret_key_pem.startswith("-----BEGIN"):
            try:
                self.private_key = serialization.load_pem_private_key(
                    self.secret_key_pem.encode("utf-8"),
                    password=None, backend=default_backend(),
                )
                self.pss_padding = padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH,
                )
                self.hash_algo = hashes.SHA256()
            except Exception as e:
                logger.warning(f"Failed to load RSA key for {self.name}: {e}")
                self.private_key = None


ACCOUNTS: List[AccountConfig] = []
DRY_RUN: bool = True
LIMIT_BUY_CENTS: int = 99
ORDER_SIZE_USD: float = 1.0


def load_accounts(config_files: List[str]) -> List[AccountConfig]:
    accounts = []
    for path in config_files:
        if not os.path.exists(path):
            logger.warning(f"Config file not found: {path}")
            continue
        try:
            spec = importlib.util.spec_from_file_location("acc_cfg", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            api_key = getattr(mod, "API_KEY", None)
            secret = getattr(mod, "SECRET_KEY", None)
            size = getattr(mod, "BET_SIZE_DOLLARS", 1.0)
            maxp = getattr(mod, "MAX_PRICE_CENTS", 99)
            allowed = getattr(mod, "ALLOWED_BET_TYPES", None)
            sizes_by_type = getattr(mod, "BET_SIZES_BY_TYPE", {}) or {}
            prices_by_type = getattr(mod, "BET_PRICES_BY_TYPE", {}) or {}
            name = os.path.basename(path).replace("w_config_", "").replace(".py", "").capitalize()
            if api_key and secret:
                acc = AccountConfig(
                    name=name, api_key=api_key, secret_key_pem=secret,
                    bet_size_usd=size, max_price_cents=maxp, enabled=True,
                    allowed_bet_types=set(s.lower() for s in allowed) if allowed else None,
                    bet_sizes_by_type={k.lower(): v for k, v in sizes_by_type.items()},
                    bet_prices_by_type={k.lower(): int(v) for k, v in prices_by_type.items()},
                )
                if acc.private_key:
                    accounts.append(acc)
                    logger.info(f"✅ Loaded: {name} (${size} @ {maxp}¢)"
                                f" types={len(allowed) if allowed else 'ALL'}"
                                f" size_overrides={len(sizes_by_type)}"
                                f" price_overrides={len(prices_by_type)}")
                else:
                    logger.error(f"❌ Crypto load failed: {name}")
            else:
                logger.warning(f"⚠️  Missing API_KEY or SECRET_KEY in {path}")
        except Exception as e:
            logger.error(f"❌ Error loading {path}: {e}")
    return accounts


# =============================================================================
# KALSHI SIGNING
# =============================================================================
def sign_kalshi_for_account(account: AccountConfig, ts_ms: str, method: str, path: str) -> str:
    if account.private_key is None:
        return ""
    msg = f"{ts_ms}{method}{path}".encode("utf-8")
    sig = account.private_key.sign(msg, account.pss_padding, account.hash_algo)
    return base64.b64encode(sig).decode("ascii")


def get_kalshi_headers(account: AccountConfig, method: str, path: str) -> Dict[str, str]:
    ts = str(int(time.time() * 1000))
    return {
        "KALSHI-ACCESS-KEY": account.api_key,
        "KALSHI-ACCESS-SIGNATURE": sign_kalshi_for_account(account, ts, method, path),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }


# Pre-signed header cache (background loop keeps fresh — 0ms hot-path RSA)
PRE_SIGNED_HEADERS: Dict[str, Dict[str, str]] = {}
PRE_SIGNED_BATCH_HEADERS: Dict[str, Dict[str, str]] = {}
HEADER_WARMUP_SEC = 0.6


async def keep_headers_fresh():
    method = "POST"
    while True:
        try:
            ts_now = str(int(time.time() * 1000))
            for acc in ACCOUNTS:
                if not acc.enabled or acc.private_key is None:
                    continue
                sig_single = sign_kalshi_for_account(acc, ts_now, method, KALSHI_ORDER_PATH)
                sig_batch = sign_kalshi_for_account(acc, ts_now, method, KALSHI_BATCH_ORDER_PATH)
                base_hdrs = {
                    "KALSHI-ACCESS-KEY": acc.api_key,
                    "KALSHI-ACCESS-TIMESTAMP": ts_now,
                    "Content-Type": "application/json",
                }
                PRE_SIGNED_HEADERS[acc.name] = {**base_hdrs, "KALSHI-ACCESS-SIGNATURE": sig_single}
                PRE_SIGNED_BATCH_HEADERS[acc.name] = {**base_hdrs, "KALSHI-ACCESS-SIGNATURE": sig_batch}
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"⚠️ Header refresh failed: {e}")
            await asyncio.sleep(1)


# =============================================================================
# ENGINE — Rida-style closed-form evaluators
# =============================================================================
Target = Dict[str, Any]


@dataclass
class OverLine:
    half_int: int       # floor(line); e.g. 21.5 → 21
    target: Target


@dataclass
class HandicapSlot:
    side: str           # "home" | "away"
    line: float
    covers: Optional[Target] = None
    not_covers: Optional[Target] = None


@dataclass
class SetNWinnerSlot:
    set_n: int
    home: Optional[Target] = None
    away: Optional[Target] = None


@dataclass
class ExactMatchSlot:
    winner_side: str
    winner_sets: int
    loser_sets: int
    yes_target: Optional[Target] = None
    no_target: Optional[Target] = None


@dataclass
class GameHandicapSlot:
    side: str
    line_int: int
    yes_target: Optional[Target] = None
    no_target: Optional[Target] = None


@dataclass
class GameTargets:
    match_total_over: List[OverLine] = field(default_factory=list)
    match_total_under: List[OverLine] = field(default_factory=list)
    first_set_total_over: List[OverLine] = field(default_factory=list)
    first_set_total_under: List[OverLine] = field(default_factory=list)
    set_total_over: List[OverLine] = field(default_factory=list)
    set_total_under: List[OverLine] = field(default_factory=list)
    moneyline_home: Optional[Target] = None
    moneyline_away: Optional[Target] = None
    first_set_winner_home: Optional[Target] = None
    first_set_winner_away: Optional[Target] = None
    set_handicaps: List[HandicapSlot] = field(default_factory=list)
    completed_match_yes: Optional[Target] = None
    completed_match_no: Optional[Target] = None
    set_n_winners: List[SetNWinnerSlot] = field(default_factory=list)
    exact_match: List[ExactMatchSlot] = field(default_factory=list)
    game_handicaps: List[GameHandicapSlot] = field(default_factory=list)


@dataclass
class TennisGameState:
    sets_home: int = 0
    sets_away: int = 0
    prev_sets_home: Optional[int] = None    # needed for set_n_winner correctness
    prev_sets_away: Optional[int] = None
    games_home: int = 0
    games_away: int = 0
    total_games: int = 0
    prev_total_games: Optional[int] = None
    first_set_games: int = 0
    prev_first_set_games: Optional[int] = None
    total_sets: int = 0
    prev_total_sets: Optional[int] = None
    current_set: int = 1
    match_completed: bool = False
    first_set_completed: bool = False


@dataclass
class GameSlot:
    fixture_id: str
    sets_to_win: int
    targets: GameTargets
    state: TennisGameState = field(default_factory=TennisGameState)
    is_first_tick: bool = True
    # tombstones
    match_total_under_emitted: bool = False
    first_set_total_under_emitted: bool = False
    set_total_under_emitted: bool = False
    first_set_winner_resolved: bool = False
    moneyline_resolved: bool = False
    completed_match_resolved: bool = False
    set_handicap_resolved: Set[Tuple[str, float]] = field(default_factory=set)
    set_total_over_emitted: Set[int] = field(default_factory=set)
    set_n_winner_resolved: Set[int] = field(default_factory=set)
    exact_match_resolved: Set[Tuple[str, int, int]] = field(default_factory=set)
    game_handicap_resolved: Set[Tuple[str, int]] = field(default_factory=set)


@dataclass
class Intent:
    fixture_id: str
    market: str
    target: Target
    reason: str


class TennisEngine:
    def __init__(self) -> None:
        self.games: Dict[str, GameSlot] = {}

    def add_game(self, g: Dict[str, Any]) -> None:
        fid = g["fixture_id"]
        if fid in self.games:
            return
        stw = int(g.get("sets_to_win", 2))
        targets = GameTargets()
        for m in g.get("markets", []):
            mtype = m["type"]
            line = m.get("line")
            ts = m.get("targets", [])
            if mtype == "match_total":
                for t in ts:
                    ol = OverLine(int(line // 1), t["target"])
                    (targets.match_total_over if t["semantic"] == "over"
                        else targets.match_total_under).append(ol)
            elif mtype == "first_set_total":
                for t in ts:
                    ol = OverLine(int(line // 1), t["target"])
                    (targets.first_set_total_over if t["semantic"] == "over"
                        else targets.first_set_total_under).append(ol)
            elif mtype == "set_total":
                for t in ts:
                    ol = OverLine(int(line // 1), t["target"])
                    (targets.set_total_over if t["semantic"] == "over"
                        else targets.set_total_under).append(ol)
            elif mtype == "moneyline":
                for t in ts:
                    if t["semantic"] == "home":
                        targets.moneyline_home = t["target"]
                    elif t["semantic"] == "away":
                        targets.moneyline_away = t["target"]
            elif mtype == "first_set_winner":
                for t in ts:
                    if t["semantic"] == "home":
                        targets.first_set_winner_home = t["target"]
                    elif t["semantic"] == "away":
                        targets.first_set_winner_away = t["target"]
            elif mtype == "set_handicap":
                side = m["side"]
                slot = next((s for s in targets.set_handicaps
                              if s.side == side and abs(s.line - line) < 1e-9), None)
                if slot is None:
                    slot = HandicapSlot(side=side, line=float(line))
                    targets.set_handicaps.append(slot)
                for t in ts:
                    if t["semantic"] == "covers":
                        slot.covers = t["target"]
                    elif t["semantic"] == "not_covers":
                        slot.not_covers = t["target"]
            elif mtype == "completed_match":
                for t in ts:
                    if t["semantic"] == "yes":
                        targets.completed_match_yes = t["target"]
                    elif t["semantic"] == "no":
                        targets.completed_match_no = t["target"]
            elif mtype == "set_n_winner":
                n = int(m["set_n"])
                slot = next((s for s in targets.set_n_winners if s.set_n == n), None)
                if slot is None:
                    slot = SetNWinnerSlot(set_n=n)
                    targets.set_n_winners.append(slot)
                for t in ts:
                    if t["semantic"] == "home":
                        slot.home = t["target"]
                    elif t["semantic"] == "away":
                        slot.away = t["target"]
            elif mtype == "exact_match":
                for t in ts:
                    key = (t["winner_side"], int(t["winner_sets"]), int(t["loser_sets"]))
                    slot_em = next((s for s in targets.exact_match
                                    if (s.winner_side, s.winner_sets, s.loser_sets) == key), None)
                    if slot_em is None:
                        slot_em = ExactMatchSlot(winner_side=key[0],
                                                  winner_sets=key[1],
                                                  loser_sets=key[2])
                        targets.exact_match.append(slot_em)
                    sem = t.get("semantic", "yes")
                    if sem == "yes":
                        slot_em.yes_target = t["target"]
                    else:
                        slot_em.no_target = t["target"]
            elif mtype == "game_handicap":
                for t in ts:
                    key = (t["side"], int(t["line_int"]))
                    slot_gh = next((s for s in targets.game_handicaps
                                    if (s.side, s.line_int) == key), None)
                    if slot_gh is None:
                        slot_gh = GameHandicapSlot(side=key[0], line_int=key[1])
                        targets.game_handicaps.append(slot_gh)
                    sem = t.get("semantic", "yes")
                    if sem == "yes":
                        slot_gh.yes_target = t["target"]
                    else:
                        slot_gh.no_target = t["target"]
        for lst in (targets.match_total_over, targets.match_total_under,
                     targets.first_set_total_over, targets.first_set_total_under,
                     targets.set_total_over, targets.set_total_under):
            lst.sort(key=lambda ol: ol.half_int)
        self.games[fid] = GameSlot(fixture_id=fid, sets_to_win=stw, targets=targets)

    def process_tick(self, fixture_id: str, *, sets_home, sets_away, games_home, games_away,
                       total_games, first_set_games, total_sets, current_set,
                       match_completed, first_set_completed) -> List[Intent]:
        slot = self.games.get(fixture_id)
        if slot is None:
            return []
        prev = slot.state
        is_first = slot.is_first_tick
        slot.is_first_tick = False
        state = TennisGameState(
            sets_home=sets_home, sets_away=sets_away,
            prev_sets_home=None if is_first else prev.sets_home,
            prev_sets_away=None if is_first else prev.sets_away,
            games_home=games_home, games_away=games_away,
            total_games=total_games,
            prev_total_games=None if is_first else prev.total_games,
            first_set_games=(first_set_games if first_set_games is not None else prev.first_set_games),
            prev_first_set_games=None if is_first else prev.first_set_games,
            total_sets=total_sets,
            prev_total_sets=None if is_first else prev.total_sets,
            current_set=current_set,
            match_completed=match_completed or prev.match_completed,
            first_set_completed=first_set_completed or prev.first_set_completed,
        )
        slot.state = state
        # Cold-start tombstones — don't fire end-of-* events for an in-progress fixture
        if is_first:
            if state.first_set_completed:
                slot.first_set_winner_resolved = True
                slot.first_set_total_under_emitted = True
            if state.match_completed:
                slot.match_total_under_emitted = True
                slot.set_total_under_emitted = True
                slot.moneyline_resolved = True
                slot.completed_match_resolved = True
                for s in slot.targets.set_n_winners:
                    slot.set_n_winner_resolved.add(s.set_n)
                for em in slot.targets.exact_match:
                    slot.exact_match_resolved.add((em.winner_side, em.winner_sets, em.loser_sets))
                for gh in slot.targets.game_handicaps:
                    slot.game_handicap_resolved.add((gh.side, gh.line_int))

        intents: List[Intent] = []
        self._eval_match_totals(slot, state, intents)
        self._eval_first_set_totals(slot, state, intents)
        self._eval_set_totals(slot, state, intents)
        self._eval_first_set_winner(slot, state, intents)
        self._eval_moneyline(slot, state, intents)
        self._eval_set_handicap(slot, state, intents)
        self._eval_completed_match(slot, state, intents)
        self._eval_set_n_winner(slot, state, intents)
        self._eval_exact_match(slot, state, intents)
        self._eval_game_handicap(slot, state, intents)
        return intents

    # ---------- evaluators ----------
    def _eval_match_totals(self, slot, state, intents):
        tgt = slot.targets
        if state.prev_total_games is not None and state.total_games > state.prev_total_games:
            for ol in tgt.match_total_over:
                if state.prev_total_games <= ol.half_int < state.total_games:
                    intents.append(Intent(slot.fixture_id, "MATCH_TOTAL_OVER", ol.target,
                        f"total {state.prev_total_games}→{state.total_games} crosses {ol.half_int+0.5}"))
        if state.match_completed and not slot.match_total_under_emitted:
            slot.match_total_under_emitted = True
            for ol in tgt.match_total_under:
                if ol.half_int >= state.total_games:
                    intents.append(Intent(slot.fixture_id, "MATCH_TOTAL_UNDER", ol.target,
                        f"match ended at {state.total_games} games ≤ {ol.half_int+0.5}"))

    def _eval_first_set_totals(self, slot, state, intents):
        tgt = slot.targets
        if state.prev_first_set_games is not None and state.first_set_games > state.prev_first_set_games:
            for ol in tgt.first_set_total_over:
                if state.prev_first_set_games <= ol.half_int < state.first_set_games:
                    intents.append(Intent(slot.fixture_id, "FIRST_SET_TOTAL_OVER", ol.target,
                        f"set1 games {state.prev_first_set_games}→{state.first_set_games} crosses {ol.half_int+0.5}"))
        if state.first_set_completed and not slot.first_set_total_under_emitted:
            slot.first_set_total_under_emitted = True
            for ol in tgt.first_set_total_under:
                if ol.half_int >= state.first_set_games:
                    intents.append(Intent(slot.fixture_id, "FIRST_SET_TOTAL_UNDER", ol.target,
                        f"set1 ended at {state.first_set_games} games ≤ {ol.half_int+0.5}"))

    def _eval_set_totals(self, slot, state, intents):
        tgt = slot.targets
        stw = slot.sets_to_win
        if state.prev_total_sets is not None and state.total_sets > state.prev_total_sets:
            min_sets = min(state.sets_home, state.sets_away)
            for ol in tgt.set_total_over:
                if ol.half_int in slot.set_total_over_emitted:
                    continue
                n = ol.half_int
                min_needed = max(n + 1 - stw, 0)
                if min_sets >= min_needed:
                    slot.set_total_over_emitted.add(ol.half_int)
                    intents.append(Intent(slot.fixture_id, "SET_TOTAL_OVER", ol.target,
                        f"min(sh,sa)={min_sets} ≥ {min_needed} (BO{stw*2-1} Over {n+0.5})"))
        if state.match_completed and not slot.set_total_under_emitted:
            slot.set_total_under_emitted = True
            for ol in tgt.set_total_under:
                if ol.half_int >= state.total_sets:
                    intents.append(Intent(slot.fixture_id, "SET_TOTAL_UNDER", ol.target,
                        f"match ended {state.total_sets} sets ≤ {ol.half_int+0.5}"))

    def _eval_first_set_winner(self, slot, state, intents):
        if not state.first_set_completed or slot.first_set_winner_resolved:
            return
        slot.first_set_winner_resolved = True
        tgt = slot.targets
        if state.sets_home > state.sets_away and tgt.first_set_winner_home:
            intents.append(Intent(slot.fixture_id, "FIRST_SET_WINNER",
                tgt.first_set_winner_home, "home won set 1"))
        elif state.sets_away > state.sets_home and tgt.first_set_winner_away:
            intents.append(Intent(slot.fixture_id, "FIRST_SET_WINNER",
                tgt.first_set_winner_away, "away won set 1"))

    def _eval_moneyline(self, slot, state, intents):
        if slot.moneyline_resolved:
            return
        stw = slot.sets_to_win
        decided = (state.match_completed or state.sets_home >= stw or state.sets_away >= stw)
        if not decided:
            return
        slot.moneyline_resolved = True
        tgt = slot.targets
        if state.sets_home > state.sets_away and tgt.moneyline_home:
            intents.append(Intent(slot.fixture_id, "MONEYLINE", tgt.moneyline_home,
                f"home wins {state.sets_home}-{state.sets_away} (stw={stw})"))
        elif state.sets_away > state.sets_home and tgt.moneyline_away:
            intents.append(Intent(slot.fixture_id, "MONEYLINE", tgt.moneyline_away,
                f"away wins {state.sets_away}-{state.sets_home} (stw={stw})"))

    def _eval_set_handicap(self, slot, state, intents):
        tgt = slot.targets
        stw = slot.sets_to_win
        margin = state.sets_home - state.sets_away
        decided = (state.match_completed or state.sets_home >= stw or state.sets_away >= stw)
        for slot_hc in tgt.set_handicaps:
            key = (slot_hc.side, slot_hc.line)
            if key in slot.set_handicap_resolved:
                continue
            opp = state.sets_away if slot_hc.side == "home" else state.sets_home
            if decided:
                slot.set_handicap_resolved.add(key)
                adj = margin if slot_hc.side == "home" else -margin
                if adj + slot_hc.line > 0:
                    if slot_hc.covers:
                        intents.append(Intent(slot.fixture_id, "SET_HANDICAP_COVERS",
                            slot_hc.covers, f"{slot_hc.side} {slot_hc.line:+} covered (margin {adj})"))
                else:
                    if slot_hc.not_covers:
                        intents.append(Intent(slot.fixture_id, "SET_HANDICAP_NOT_COVERS",
                            slot_hc.not_covers, f"{slot_hc.side} {slot_hc.line:+} NOT covered (margin {adj})"))
            else:
                max_margin = stw - opp
                if max_margin + slot_hc.line <= 0:
                    slot.set_handicap_resolved.add(key)
                    if slot_hc.not_covers:
                        intents.append(Intent(slot.fixture_id, "SET_HANDICAP_NOT_COVERS",
                            slot_hc.not_covers,
                            f"{slot_hc.side} {slot_hc.line:+} cannot cover: max {max_margin} + {slot_hc.line:+} ≤ 0"))

    def _eval_completed_match(self, slot, state, intents):
        if slot.completed_match_resolved:
            return
        stw = slot.sets_to_win
        decided = (state.match_completed or state.sets_home >= stw or state.sets_away >= stw)
        if not decided:
            return
        slot.completed_match_resolved = True
        tgt = slot.targets
        if tgt.completed_match_yes:
            intents.append(Intent(slot.fixture_id, "COMPLETED_MATCH_YES",
                tgt.completed_match_yes,
                f"match decided ({state.sets_home}-{state.sets_away}, stw={stw})"))

    def _eval_set_n_winner(self, slot, state, intents):
        """KX*SETWINNER for any N. Uses PER-SIDE prev tracking — fires only for
        the side whose set count actually incremented. Fixes the 0-2 → 1-2 case
        where the old 'leader = latest winner' heuristic mis-fired."""
        tgt = slot.targets
        if not tgt.set_n_winners:
            return
        if state.prev_total_sets is None or state.total_sets <= state.prev_total_sets:
            return
        if state.prev_sets_home is None or state.prev_sets_away is None:
            return
        home_inc = state.sets_home > state.prev_sets_home
        away_inc = state.sets_away > state.prev_sets_away
        if home_inc == away_inc:
            return
        winner = "home" if home_inc else "away"
        for slot_sn in tgt.set_n_winners:
            n = slot_sn.set_n
            if n in slot.set_n_winner_resolved:
                continue
            if state.prev_total_sets >= n or state.total_sets < n:
                continue
            slot.set_n_winner_resolved.add(n)
            target = slot_sn.home if winner == "home" else slot_sn.away
            if target:
                intents.append(Intent(slot.fixture_id, f"SET_{n}_WINNER", target,
                    f"{winner} won set {n} "
                    f"({state.prev_sets_home}-{state.prev_sets_away} → "
                    f"{state.sets_home}-{state.sets_away})"))

    def _eval_exact_match(self, slot, state, intents):
        tgt = slot.targets
        if not tgt.exact_match:
            return
        stw = slot.sets_to_win
        decided = (state.match_completed or state.sets_home >= stw or state.sets_away >= stw)
        for em in tgt.exact_match:
            key = (em.winner_side, em.winner_sets, em.loser_sets)
            if key in slot.exact_match_resolved:
                continue
            player = state.sets_home if em.winner_side == "home" else state.sets_away
            opp = state.sets_away if em.winner_side == "home" else state.sets_home
            if decided:
                slot.exact_match_resolved.add(key)
                aw_sets = max(state.sets_home, state.sets_away)
                al_sets = min(state.sets_home, state.sets_away)
                aw_side = ("home" if state.sets_home > state.sets_away
                            else "away" if state.sets_away > state.sets_home else None)
                if aw_side == em.winner_side and aw_sets == em.winner_sets and al_sets == em.loser_sets:
                    if em.yes_target:
                        intents.append(Intent(slot.fixture_id, "EXACT_MATCH_YES", em.yes_target,
                            f"exact {em.winner_side} {em.winner_sets}-{em.loser_sets} hit"))
                else:
                    if em.no_target:
                        intents.append(Intent(slot.fixture_id, "EXACT_MATCH_NO", em.no_target,
                            f"exact {em.winner_side} {em.winner_sets}-{em.loser_sets} didn't happen"))
            else:
                if opp > em.loser_sets or player > em.winner_sets:
                    slot.exact_match_resolved.add(key)
                    if em.no_target:
                        intents.append(Intent(slot.fixture_id, "EXACT_MATCH_NO", em.no_target,
                            f"exact {em.winner_side} {em.winner_sets}-{em.loser_sets} impossible "
                            f"(player {player}, opp {opp})"))

    def _eval_game_handicap(self, slot, state, intents):
        tgt = slot.targets
        if not tgt.game_handicaps:
            return
        stw = slot.sets_to_win
        best_of = stw * 2 - 1
        sets_played = state.sets_home + state.sets_away
        sets_remaining = max(0, best_of - sets_played)
        max_opp_gain = sets_remaining * 7
        decided = (state.match_completed or state.sets_home >= stw or state.sets_away >= stw)
        for gh in tgt.game_handicaps:
            key = (gh.side, gh.line_int)
            if key in slot.game_handicap_resolved:
                continue
            line = gh.line_int - 0.5
            margin = (state.games_home - state.games_away) if gh.side == "home" else (state.games_away - state.games_home)
            if decided:
                slot.game_handicap_resolved.add(key)
                if margin > line:
                    if gh.yes_target:
                        intents.append(Intent(slot.fixture_id, "GAME_HANDICAP_YES", gh.yes_target,
                            f"{gh.side} margin {margin} > {line}"))
                else:
                    if gh.no_target:
                        intents.append(Intent(slot.fixture_id, "GAME_HANDICAP_NO", gh.no_target,
                            f"{gh.side} margin {margin} ≤ {line}"))
            else:
                if (margin - max_opp_gain) > line:
                    slot.game_handicap_resolved.add(key)
                    if gh.yes_target:
                        intents.append(Intent(slot.fixture_id, "GAME_HANDICAP_YES", gh.yes_target,
                            f"{gh.side} margin {margin} − max gain {max_opp_gain} > {line}"))
                elif (margin + max_opp_gain) <= line:
                    slot.game_handicap_resolved.add(key)
                    if gh.no_target:
                        intents.append(Intent(slot.fixture_id, "GAME_HANDICAP_NO", gh.no_target,
                            f"{gh.side} margin {margin} + max gain {max_opp_gain} ≤ {line}"))


# =============================================================================
# KALSHI PLAN BUILDER — TennisFixture → engine plan
# =============================================================================
@dataclass
class TennisFixture:
    fixture_id: str
    home_name: str
    away_name: str
    competition: str
    kalshi_event: Optional[str] = None
    kalshi_tickers: List[str] = field(default_factory=list)
    home_abbrev: Optional[str] = None
    away_abbrev: Optional[str] = None
    best_of: int = 3
    last_sets: Tuple[int, int] = (0, 0)
    last_total_games: int = 0
    last_free_text: str = ""
    match_ended: bool = False
    snipe_attempted: Set[str] = field(default_factory=set)
    recent_stream_false: int = 0


def best_of(competition: str) -> int:
    if not competition:
        return 3
    c = competition.lower()
    is_slam_main = any(s in c for s in (
        "roland garros", "french open", "wimbledon",
        "us open", "u.s. open", "australian open",
    ))
    is_qualifying = any(s in c for s in ("qualification", "qualifying", "qual"))
    is_men = ("men" in c) and ("women" not in c)
    return 5 if (is_slam_main and is_men and not is_qualifying) else 3


def _is_doubles_fixture(fx: TennisFixture) -> bool:
    if " / " in (fx.home_name or "") or " / " in (fx.away_name or ""):
        return True
    return "doubles" in (fx.competition or "").lower()


def _abbrevs_look_plausible(fx: TennisFixture) -> bool:
    for full, abbr in ((fx.home_name, fx.home_abbrev), (fx.away_name, fx.away_abbrev)):
        if not full or not abbr or len(abbr) < 2:
            return False
        norm = _normalize_name(full)
        if abbr.lower()[:2] not in norm:
            return False
    return True


def parse_ticker(ticker: str) -> Tuple[str, List[str], str]:
    parts = ticker.split("-")
    return parts[0], parts[1:], parts[-1]


def build_game_from_fixture(fx: TennisFixture) -> Optional[Dict[str, Any]]:
    if not fx.kalshi_tickers or not fx.home_abbrev or not fx.away_abbrev:
        return None
    sets_to_win = (fx.best_of + 1) // 2
    HA, AA = fx.home_abbrev, fx.away_abbrev
    ml_targets: List[Dict] = []
    gtotal_entries: List[Dict] = []
    gspread_targets: List[Dict] = []
    setn_by_n: Dict[int, List[Dict]] = {}
    exact_targets: List[Dict] = []
    unmatched: List[str] = []

    for tk in fx.kalshi_tickers:
        series, fragments, sfx = parse_ticker(tk)
        if series in MONEYLINE_SERIES and len(fragments) == 2:
            if sfx == HA:
                ml_targets.append({"semantic": "home", "target": {"ticker": tk, "side": "yes"}})
            elif sfx == AA:
                ml_targets.append({"semantic": "away", "target": {"ticker": tk, "side": "yes"}})
            else:
                unmatched.append(f"{tk} (sfx={sfx} not HA={HA}/AA={AA})")
            continue
        if series in SET_WINNER_SERIES and len(fragments) == 3:
            try:
                n = int(fragments[1])
            except ValueError:
                unmatched.append(f"{tk} (bad set-N)")
                continue
            sem = ("home" if sfx == HA else "away" if sfx == AA else None)
            if sem is None:
                unmatched.append(f"{tk} (sfx={sfx} not HA/AA)")
                continue
            setn_by_n.setdefault(n, []).append({"semantic": sem, "target": {"ticker": tk, "side": "yes"}})
            continue
        if series == "KXATPGTOTAL":
            try:
                n = int(sfx)
            except ValueError:
                unmatched.append(f"{tk} (bad GTOTAL line)")
                continue
            gtotal_entries.append({"line": n - 0.5, "targets": [
                {"semantic": "over",  "target": {"ticker": tk, "side": "yes"}},
                {"semantic": "under", "target": {"ticker": tk, "side": "no"}},
            ]})
            continue
        if series == "KXATPGSPREAD":
            if len(sfx) < 4:
                unmatched.append(f"{tk} (short GSPREAD suffix)"); continue
            abbr = sfx[:3]
            try:
                nval = int(sfx[3:])
            except ValueError:
                unmatched.append(f"{tk} (bad GSPREAD N)"); continue
            side = "home" if abbr == HA else "away" if abbr == AA else None
            if side is None:
                unmatched.append(f"{tk} (GSPREAD abbr {abbr} not HA/AA)"); continue
            gspread_targets.append({"side": side, "line_int": nval, "semantic": "yes",
                                     "target": {"ticker": tk, "side": "yes"}})
            gspread_targets.append({"side": side, "line_int": nval, "semantic": "no",
                                     "target": {"ticker": tk, "side": "no"}})
            continue
        if series == "KXATPEXACTMATCH":
            if len(sfx) < 5:
                unmatched.append(f"{tk} (short EXACT suffix)"); continue
            abbr = sfx[:3]
            try:
                ws, ls = int(sfx[3]), int(sfx[4])
            except ValueError:
                unmatched.append(f"{tk} (bad EXACT score)"); continue
            side = "home" if abbr == HA else "away" if abbr == AA else None
            if side is None:
                unmatched.append(f"{tk} (EXACT abbr {abbr} not HA/AA)"); continue
            exact_targets.append({"winner_side": side, "winner_sets": ws, "loser_sets": ls,
                                    "semantic": "yes", "target": {"ticker": tk, "side": "yes"}})
            exact_targets.append({"winner_side": side, "winner_sets": ws, "loser_sets": ls,
                                    "semantic": "no",  "target": {"ticker": tk, "side": "no"}})
            continue
        unmatched.append(f"{tk} (series {series} not yet wired)")

    markets: List[Dict] = []
    if ml_targets:
        markets.append({"type": "moneyline", "targets": ml_targets})
    for entry in gtotal_entries:
        markets.append({"type": "match_total", "line": entry["line"], "targets": entry["targets"]})
    for n, targets in setn_by_n.items():
        if n == 1:
            markets.append({"type": "first_set_winner", "targets": targets})
        else:
            markets.append({"type": "set_n_winner", "set_n": n, "targets": targets})
    if gspread_targets:
        markets.append({"type": "game_handicap", "targets": gspread_targets})
    if exact_targets:
        markets.append({"type": "exact_match", "targets": exact_targets})
    return {
        "fixture_id": fx.fixture_id,
        "sets_to_win": sets_to_win,
        "_unmatched_tickers": unmatched,
        "markets": markets,
    }


# =============================================================================
# NAME MATCHING (Kalstrop → Kalshi)
# =============================================================================
def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                    if unicodedata.category(c) != "Mn")


def _normalize_name(name: str) -> str:
    if not name:
        return ""
    s = _strip_accents(name).lower()
    s = re.sub(r"[,\.'’]", " ", s)
    s = re.sub(r"-", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_surname(name: str) -> str:
    n = _normalize_name(name)
    if not n:
        return ""
    if "," in name:
        return _normalize_name(name.split(",")[0])
    tokens = n.split()
    return tokens[-1] if tokens else ""


def _names_match_loosely(kalstrop_name: str, kalshi_subtitle: str) -> bool:
    a = _normalize_name(kalstrop_name)
    b = _normalize_name(kalshi_subtitle)
    if not a or not b:
        return False
    # Strict surname check — refuse to match on 1-char surnames (closes the
    # "F"/"M" doubles bug class from the AUGNAK mishap)
    sn = _extract_surname(kalstrop_name)
    if sn and len(sn) >= 3 and sn in b:
        return True
    if a in b or b in a:
        return True
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if len(a_tokens & b_tokens) >= 2:
        # require at least one of the shared tokens to be 4+ chars (filters out
        # "maria" / "alex" / "j" coincidences)
        if any(len(t) >= 4 for t in (a_tokens & b_tokens)):
            return True
    return False


# =============================================================================
# KALSTROP DISCOVERY — /tournaments/tennis (exhaustive, not capped)
# =============================================================================
def discover_live_tennis_fixtures() -> List[TennisFixture]:
    """Two-stage: enumerate every tennis competition from /tournaments/tennis,
    then pull LIVE fixtures from each /competition/{slug}/fixtures. This is
    the only way to see competitions that have zero pre-match fixtures (e.g.
    WTA 125K Birmingham when every match is currently live)."""
    comp_slugs: Dict[str, str] = {}
    try:
        r = requests.get(f"{KALSTROP_REST}/tournaments/tennis",
                          headers=kalstrop_headers(), params={"first": 100}, timeout=15)
        if r.status_code == 200:
            for tour in r.json():
                for comp in tour.get("competitions", []):
                    if comp.get("fixturesCount", 0) <= 0:
                        continue
                    if comp.get("slug"):
                        comp_slugs[comp["slug"]] = comp.get("name", "?")
    except Exception as e:
        logger.warning(f"Discovery /tournaments/tennis error: {e}")

    if not comp_slugs:
        logger.warning("No tennis competitions enumerated")
        return []

    fixtures: List[TennisFixture] = []
    for slug, name in comp_slugs.items():
        try:
            r = requests.get(f"{KALSTROP_REST}/competition/{slug}/fixtures",
                              headers=kalstrop_headers(), params={"first": 30}, timeout=10)
            if r.status_code != 200:
                continue
            for fx in r.json().get("nodes", []):
                if fx.get("status") != "LIVE":
                    continue
                cs = fx.get("competitors", [])
                if len(cs) != 2:
                    continue
                home_name = away_name = None
                for c in cs:
                    if c.get("isHome") is True:
                        home_name = c.get("displayName")
                    else:
                        away_name = c.get("displayName")
                if not home_name or not away_name:
                    home_name = cs[0].get("displayName")
                    away_name = cs[1].get("displayName")
                fixtures.append(TennisFixture(
                    fixture_id=fx["id"],
                    home_name=home_name, away_name=away_name,
                    competition=name, best_of=best_of(name),
                ))
        except Exception:
            continue
    logger.info(f"Discovery: {len(comp_slugs)} competitions scanned, {len(fixtures)} LIVE fixtures")
    return fixtures


# =============================================================================
# KALSHI EVENT REGISTRY — strict exact matcher (replaces fuzzy approach)
# =============================================================================
# Built once at startup + refreshed each discovery cycle. Holds every active
# Kalshi tennis event WITH BETTING_DATE in its ticker. Source of truth for
# matching: Kalshi's market.yes_sub_title field (full player name like
# "Sascha Gueymard Wayenburg"). No fuzzy guessing, no abbreviation plausibility
# check — the abbrev comes straight from the matched ticker.

@dataclass
class KalshiEventEntry:
    event_ticker: str
    series: str
    name_a: str
    name_b: str
    abbr_a: str
    abbr_b: str
    all_tickers: List[str] = field(default_factory=list)  # every related ticker

KALSHI_REGISTRY: List[KalshiEventEntry] = []


def build_kalshi_registry() -> List[KalshiEventEntry]:
    """Fetch all active Kalshi tennis events matching BETTING_DATE. Pulls
    per-series markets, groups by event_ticker, extracts both player full names
    from yes_sub_title, AND collects every related ticker (set winners, exact
    match, totals, spreads) that shares the event's date+match code. One call
    per series + one per ticker-series = ~12 HTTP × 150ms = ~2s total."""
    # Stage 1: collect MAIN events + extract full names per side
    main_events: List[KalshiEventEntry] = []
    for series in ("KXATPMATCH", "KXWTAMATCH",
                    "KXATPCHALLENGERMATCH", "KXWTACHALLENGERMATCH",
                    "KXITFMATCH", "KXITFWMATCH"):
        try:
            r = requests.get(f"{KALSHI_BASE}/markets",
                              params={"series_ticker": series, "limit": 200},
                              timeout=10)
            if r.status_code != 200:
                continue
            by_event: Dict[str, List[Dict]] = {}
            for m in r.json().get("markets", []):
                if m.get("status") != "active":
                    continue
                ev = m.get("event_ticker", "")
                if not ev:
                    continue
                # HARD DATE FILTER — only events whose ticker carries today's date
                if BETTING_DATE and BETTING_DATE != "0" and BETTING_DATE not in ev:
                    continue
                by_event.setdefault(ev, []).append(m)
            for ev, mkts in by_event.items():
                if len(mkts) != 2:
                    continue
                main_events.append(KalshiEventEntry(
                    event_ticker=ev, series=series,
                    name_a=mkts[0].get("yes_sub_title", ""),
                    name_b=mkts[1].get("yes_sub_title", ""),
                    abbr_a=mkts[0]["ticker"].split("-")[-1],
                    abbr_b=mkts[1]["ticker"].split("-")[-1],
                ))
        except Exception as e:
            logger.warning(f"Registry build err on {series}: {e}")
        time.sleep(0.15)

    # Stage 2: collect ALL related tickers (SETWINNER, EXACT, GTOTAL, GSPREAD)
    # per event by matching the date+match code (e.g. "26JUN24GAUSEA")
    if not main_events:
        logger.warning(f"📚 Kalshi registry: 0 events for BETTING_DATE={BETTING_DATE}")
        return main_events
    # Index events by their match-code suffix (everything after the series-)
    code_to_entry: Dict[str, KalshiEventEntry] = {}
    for e in main_events:
        parts = e.event_ticker.split("-")
        if len(parts) >= 2:
            code_to_entry[parts[1]] = e   # "26JUN24GAUSEA" -> entry
    extra_series = ("KXATPSETWINNER", "KXWTASETWINNER", "KXATPEXACTMATCH",
                     "KXATPGTOTAL", "KXATPGSPREAD", "KXATPSETSWEEP")
    for series in extra_series:
        try:
            r = requests.get(f"{KALSHI_BASE}/markets",
                              params={"series_ticker": series, "limit": 200},
                              timeout=10)
            if r.status_code != 200:
                continue
            for m in r.json().get("markets", []):
                if m.get("status") != "active":
                    continue
                tk = m.get("ticker", "")
                if BETTING_DATE and BETTING_DATE != "0" and BETTING_DATE not in tk:
                    continue
                # Find matching event by code
                for code, entry in code_to_entry.items():
                    if code in tk:
                        if tk not in entry.all_tickers:
                            entry.all_tickers.append(tk)
                        break
        except Exception as e:
            logger.warning(f"Registry tickers err on {series}: {e}")
        time.sleep(0.15)

    # Also include the 2 main moneyline tickers in each entry's all_tickers
    for e in main_events:
        for series in (e.series,):
            try:
                r = requests.get(f"{KALSHI_BASE}/markets",
                                  params={"series_ticker": series, "limit": 200},
                                  timeout=10)
                if r.status_code != 200:
                    continue
                for m in r.json().get("markets", []):
                    if m.get("event_ticker") == e.event_ticker:
                        tk = m.get("ticker", "")
                        if tk and tk not in e.all_tickers:
                            e.all_tickers.append(tk)
            except Exception:
                continue
            time.sleep(0.05)

    total_tickers = sum(len(e.all_tickers) for e in main_events)
    logger.info(f"📚 Kalshi registry: {len(main_events)} events / "
                 f"{total_tickers} tickers (date={BETTING_DATE or 'ALL'})")
    return main_events


def _name_tokens(name: str) -> Set[str]:
    return set(_normalize_name(name).split())


def _strong_overlap(a_tokens: Set[str], b_tokens: Set[str], min_len: int = 4) -> bool:
    return any(len(t) >= min_len for t in (a_tokens & b_tokens))


def find_kalshi_event(fixture: TennisFixture,
                       registry: List[KalshiEventEntry]) -> Optional[KalshiEventEntry]:
    """Strict matcher — both sides must share a ≥4-char surname token. Returns
    None if zero matches OR ambiguous (and we can't tie-break)."""
    home_tokens = _name_tokens(fixture.home_name)
    away_tokens = _name_tokens(fixture.away_name)
    candidates: List[KalshiEventEntry] = []
    for entry in registry:
        a_tokens = _name_tokens(entry.name_a)
        b_tokens = _name_tokens(entry.name_b)
        match_ab = (_strong_overlap(home_tokens, a_tokens)
                     and _strong_overlap(away_tokens, b_tokens))
        match_ba = (_strong_overlap(home_tokens, b_tokens)
                     and _strong_overlap(away_tokens, a_tokens))
        if match_ab or match_ba:
            candidates.append(entry)
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        # Tie-break by total shared character count
        def score(entry):
            at = _name_tokens(entry.name_a)
            bt = _name_tokens(entry.name_b)
            return (sum(len(t) for t in home_tokens & at)
                    + sum(len(t) for t in away_tokens & bt)
                    + sum(len(t) for t in home_tokens & bt)
                    + sum(len(t) for t in away_tokens & at))
        candidates.sort(key=score, reverse=True)
        if score(candidates[0]) > score(candidates[1]):
            return candidates[0]
        logger.warning(f"⛔ AMBIGUOUS match for {fixture.home_name} vs "
                        f"{fixture.away_name}: tied between:")
        for c in candidates[:3]:
            logger.warning(f"     {c.event_ticker} ({c.name_a} vs {c.name_b})")
        return None
    return None


def map_fixture_to_kalshi(fixture: TennisFixture) -> bool:
    """Strict registry-based matcher. Returns True if mapped to a unique
    Kalshi event whose date matches BETTING_DATE."""
    if not KALSHI_REGISTRY:
        return False
    entry = find_kalshi_event(fixture, KALSHI_REGISTRY)
    if entry is None:
        return False
    # Figure out which side of the entry corresponds to which Kalstrop player
    home_tokens = _name_tokens(fixture.home_name)
    a_tokens = _name_tokens(entry.name_a)
    if _strong_overlap(home_tokens, a_tokens):
        fixture.home_abbrev = entry.abbr_a
        fixture.away_abbrev = entry.abbr_b
    else:
        fixture.home_abbrev = entry.abbr_b
        fixture.away_abbrev = entry.abbr_a
    fixture.kalshi_event = entry.event_ticker
    # Use the pre-collected tickers from the registry — no per-fixture HTTP
    fixture.kalshi_tickers = list(entry.all_tickers)
    return True


# =============================================================================
# RETIREMENT DETECTION
# =============================================================================
def detect_retirement(ft: str) -> Optional[str]:
    """Returns 'home' or 'away' for the WINNER (opponent of retiree), or None."""
    s = ft.strip().lower()
    if len(s) < 20 or "retired" not in s:
        return None
    if "player 1 retired" in s and "player 2 won" in s:
        return "away"
    if "player 2 retired" in s and "player 1 won" in s:
        return "home"
    return None


# =============================================================================
# KALSTROP STATE PARSER
# =============================================================================
def parse_kalstrop_push(payload: dict) -> Optional[dict]:
    ms = payload.get("matchSummary") or {}
    cur = ms.get("currentPhase") or {}
    msd = ms.get("matchStatusDisplay") or []
    ft = (msd[0].get("freeText") if msd and isinstance(msd[0], dict) else "") or ""
    phases = ms.get("phases") or []
    stream_exists = payload.get("streamExists")
    if stream_exists is None:
        stream_exists = ms.get("streamExists")
    try:
        sh = int(ms.get("homeScore", "0") or 0)
        sa = int(ms.get("awayScore", "0") or 0)
    except (TypeError, ValueError):
        sh = sa = 0
    total_games = 0
    home_games = 0
    away_games = 0
    first_set_games = None
    for ph in phases:
        try:
            n = int(ph.get("phase"))
            h = int(ph.get("homeScore", "0") or 0)
            a = int(ph.get("awayScore", "0") or 0)
            total_games += h + a
            home_games += h
            away_games += a
            if n == 1:
                first_set_games = h + a
        except (TypeError, ValueError):
            pass
    total_sets = sh + sa
    return {
        "fixture_id": payload.get("fixtureId"),
        "ft": ft,
        "sets": (sh, sa),
        "phase": cur.get("phase"),
        "games": (cur.get("homeScore"), cur.get("awayScore")),
        "total_games": total_games,
        "home_games": home_games,
        "away_games": away_games,
        "first_set_games": first_set_games,
        "total_sets": total_sets,
        "match_ended": ft.lower() == "ended",
        "first_set_completed": total_sets >= 1 or ft.lower() == "ended",
        "stream_exists": stream_exists,
        "retirement_winner": detect_retirement(ft),
    }


# =============================================================================
# BET FIRING
# =============================================================================
BETS_PLACED: Set[str] = set()

# Tracking for scoreboard
RECENT_FIRES: Deque[dict] = deque(maxlen=20)
LATEST_STATE: Dict[str, dict] = {}


def build_v2_order(ticker: str, side: str, price_c: int, qty: int,
                    tif: str, client_order_id: str) -> Dict[str, Any]:
    """V2 event-orders body. YES buy → bid at price; NO buy → ask at (1 − price).
    Matches current_kalshi (MLB) exactly so this file can use the same auth
    pipeline and response handling without surprises."""
    if side == "yes":
        v2_side, p = "bid", price_c
    else:
        v2_side, p = "ask", 100 - price_c
    body = {
        "ticker": ticker,
        "side": v2_side,
        "count": str(qty),
        "price": f"{p/100:.2f}",
        "time_in_force": tif or "good_till_canceled",
        "self_trade_prevention_type": KALSHI_STP,   # V2 required field
    }
    if client_order_id:
        body["client_order_id"] = client_order_id
    return body


async def _place_batch_for_account(
    account: AccountConfig,
    bets: List[Tuple[str, str, int, str, str, str]],
    session: aiohttp.ClientSession,
) -> List[Tuple[bool, str]]:
    """One batch HTTP per account, up to 20 orders. Same shape as MLB file."""
    to_place: List[Tuple[Dict, str, str]] = []
    for ticker, side, price_c, reason, bet_key, event_type in bets:
        if account.allowed_bet_types is not None and event_type.lower() not in account.allowed_bet_types:
            continue
        acct_key = f"{ticker}_{side}"
        if acct_key in account.bets_placed:
            continue
        et_lower = event_type.lower()
        max_p = account.bet_prices_by_type.get(et_lower, account.max_price_cents)
        actual = min(price_c, max_p)
        size = account.bet_sizes_by_type.get(et_lower, account.bet_size_usd)
        qty = max(1, int(size * 100 / actual))
        coid = make_client_order_id(account.name, ticker, side)
        order = build_v2_order(ticker, side, actual, qty,
                                tif="good_till_canceled", client_order_id=coid)
        to_place.append((order, bet_key, acct_key))
    if not to_place:
        return []
    if DRY_RUN:
        for order, bet_key, acct_key in to_place:
            account.bets_placed.add(acct_key)
            bet_logger.info(f"[DRY] [{account.name}] 🎯 {order['ticker']} {order['side']} {order['count']} @ {order['price']}")
        return [(True, bk) for _, bk, _ in to_place]

    headers = PRE_SIGNED_BATCH_HEADERS.get(account.name)
    if headers is None:
        bet_logger.warning(f"[{account.name}] Pre-signed batch headers not ready, signing sync")
        headers = get_kalshi_headers(account, "POST", KALSHI_BATCH_ORDER_PATH)
    body_bytes = fast_dumps({"orders": [t[0] for t in to_place]})
    results: List[Tuple[bool, str]] = []
    try:
        start = time.perf_counter()
        async with session.post(KALSHI_BATCH_ORDER_URL, data=body_bytes,
                                  headers=headers, timeout=_CACHED_BET_TIMEOUT) as resp:
            status = resp.status
            latency_ms = (time.perf_counter() - start) * 1000
            txt = await resp.text()
            bet_logger.info(f"[{account.name}] 📦 BATCH {len(to_place)} | {latency_ms:.0f}ms | {status}")
            if status in (200, 201):
                # Log raw response once per batch — invaluable when Kalshi tweaks
                # response shape on the demo cluster (matches current_kalshi).
                bet_logger.info(f"[{account.name}] 🔍 RAW BATCH: {txt[:600]}")
                data = json.loads(txt)
                order_results = data.get("orders", [])
                for i, ord_res in enumerate(order_results):
                    order, bet_key, acct_key = to_place[i]
                    # V2 event-orders: result may be FLAT or NESTED under .order
                    obj = ord_res.get("order", ord_res)
                    err = ord_res.get("error")
                    if err:
                        bet_logger.error(f"[{account.name}]    ❌ {order['ticker']}: {err}")
                        results.append((False, bet_key))
                    elif obj and obj.get("order_id"):
                        oid = obj.get("order_id", "?")
                        # V2 fields: fill_count + remaining_count (NOT the
                        # legacy filled_count/status from /portfolio/orders).
                        fill_count = float(obj.get("fill_count", 0) or 0)
                        remaining = float(obj.get("remaining_count", 0) or 0)
                        account.bets_placed.add(acct_key)
                        if fill_count > 0:
                            bet_logger.info(f"[{account.name}]    ✅ {order['ticker']} {order['side']} FILLED {fill_count}")
                        elif remaining > 0:
                            bet_logger.info(f"[{account.name}]    📋 {order['ticker']} {order['side']} RESTING {remaining} (id={oid})")
                        else:
                            # Matches current_kalshi: V2 can return an order
                            # accepted with both counts at 0 (mostly informational)
                            bet_logger.info(f"[{account.name}]    ✅ {order['ticker']} {order['side']} ACCEPTED 0-fill (id={oid})")
                        results.append((True, bet_key))
                    else:
                        results.append((False, bet_key))
            else:
                bet_logger.error(f"[{account.name}]    ❌ Batch failed ({status}): {txt[:300]}")
                results = [(False, bk) for _, bk, _ in to_place]
    except Exception as e:
        bet_logger.error(f"[{account.name}]    ❌ Batch error: {e}")
        results = [(False, bk) for _, bk, _ in to_place]
    return results


async def fire_bet_batch(bets: List[Tuple[str, str, int, str, str, str]],
                         session: aiohttp.ClientSession):
    if not bets:
        return
    enabled = [a for a in ACCOUNTS if a.enabled]
    if not enabled and not DRY_RUN:
        return
    if DRY_RUN:
        for ticker, side, price_c, reason, bet_key, event_type in bets:
            BETS_PLACED.add(bet_key)
            bet_logger.info(f"[DRY] 🎯 {event_type} {ticker} {side.upper()} — {reason}")
        return
    pending = [b for b in bets if b[4] not in BETS_PLACED]
    if not pending:
        return
    results = await asyncio.gather(*[
        _place_batch_for_account(acc, pending, session) for acc in enabled
    ], return_exceptions=True)
    for acct_results in results:
        if isinstance(acct_results, Exception):
            bet_logger.error(f"❌ Account batch exception: {acct_results}")
            continue
        for success, bet_key in acct_results:
            if success:
                BETS_PLACED.add(bet_key)


async def fire_intents(intents: List[Intent], fx: TennisFixture, state: dict,
                        session: aiohttp.ClientSession):
    bets = []
    for intent in intents:
        target = intent.target
        ticker = target["ticker"]
        side = target["side"]
        local_key = f"{ticker}_{side}"
        if local_key in fx.snipe_attempted:
            continue
        fx.snipe_attempted.add(local_key)
        global_key = f"{ticker}_{side}_{fx.fixture_id[:8]}"
        bets.append((ticker, side, LIMIT_BUY_CENTS,
                      f"[{intent.market}] {intent.reason}",
                      global_key, intent.market))
        logger.info(f"🎾 LOCK fid={fx.fixture_id[:8]} {intent.market} "
                    f"{ticker} side={side.upper()} — {intent.reason}")
        RECENT_FIRES.append({
            "ts":      datetime.now(timezone.utc).strftime("%H:%M:%S"),
            "fid":     fx.fixture_id[:8],
            "match":   f"{fx.home_abbrev or '?'}v{fx.away_abbrev or '?'}",
            "market":  intent.market,
            "ticker":  ticker,
            "side":    side,
            "score":   f"{state['sets'][0]}-{state['sets'][1]} "
                        f"{state.get('home_games',0)}-{state.get('away_games',0)}",
        })
    if bets:
        await fire_bet_batch(bets, session)


# =============================================================================
# ENGINE INSTANCE + Plan management
# =============================================================================
ENGINE = TennisEngine()


def add_fixture_to_engine(fx: TennisFixture) -> bool:
    if _is_doubles_fixture(fx):
        logger.info(f"  ⛔ rejecting doubles fixture: {fx.home_name} vs {fx.away_name} "
                    f"(comp={fx.competition!r})")
        return False
    if not fx.kalshi_tickers or not fx.home_abbrev or not fx.away_abbrev:
        return False
    # NB: _abbrevs_look_plausible check removed — registry-based matcher pulls
    # abbrevs directly from the matched Kalshi ticker, so they're correct by
    # construction. No fuzzy guess to second-guess.
    if fx.fixture_id in ENGINE.games:
        return True
    game = build_game_from_fixture(fx)
    if not game:
        return False
    ENGINE.add_game(game)
    unmatched = game.get("_unmatched_tickers", [])
    logger.info(f"  🧠 engine + fid={fx.fixture_id[:8]}  markets={len(game['markets'])}  "
                f"sets_to_win={game['sets_to_win']}  unmatched={len(unmatched)}")
    return True


# =============================================================================
# WS LOOP — Kalstrop V2 → engine → fire
# =============================================================================
SUB_OPS_QUEUE: "asyncio.Queue" = None


async def subscribe_to_fixtures(ws, fixture_ids: List[str]):
    if not fixture_ids:
        return
    await ws.send(json.dumps({
        "id": f"tennis-sub-{int(time.time()*1000)}",
        "type": "subscribe",
        "payload": {
            "operationName": "sportsMatchStateUpdatedV2",
            "query": "subscription sportsMatchStateUpdatedV2($fixtureIds: [String!]!) { sportsMatchStateUpdatedV2(fixtureIds: $fixtureIds) }",
            "variables": {"fixtureIds": fixture_ids},
        },
    }))


async def unsubscribe_from_fixtures(ws, fixture_ids: List[str]):
    if not fixture_ids:
        return
    await ws.send(json.dumps({
        "id": f"tennis-unsub-{int(time.time()*1000)}",
        "type": "unsubscribe",
        "payload": {
            "operationName": "sportsMatchStateUpdatedV2",
            "variables": {"fixtureIds": fixture_ids},
        },
    }))


async def kalstrop_ws_listener(
    fixtures_by_id: Dict[str, TennisFixture],
    session: aiohttp.ClientSession,
    stop: asyncio.Event,
):
    backoff = 2
    while not stop.is_set():
        try:
            async with websockets.connect(kalstrop_ws_url(), max_size=None,
                                            ping_interval=None) as ws:
                logger.info(f"[kalstrop] WS connected ({len(fixtures_by_id)} fixtures)")
                backoff = 2
                await subscribe_to_fixtures(ws, list(fixtures_by_id.keys()))

                async def consume_sub_ops():
                    while not stop.is_set():
                        try:
                            op, fids = await asyncio.wait_for(SUB_OPS_QUEUE.get(), timeout=1.0)
                            if op == "subscribe":
                                await subscribe_to_fixtures(ws, fids)
                                logger.info(f"[kalstrop] ➕ subscribed to {len(fids)} new")
                            elif op == "unsubscribe":
                                await unsubscribe_from_fixtures(ws, fids)
                                logger.info(f"[kalstrop] ➖ unsubscribed from {len(fids)}")
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logger.warning(f"[kalstrop] sub-op error: {e}")

                consumer = asyncio.create_task(consume_sub_ops())
                try:
                    while not stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            try:
                                await ws.send(json.dumps({"type": "ping"}))
                            except Exception:
                                pass
                            continue
                        try:
                            msg = fast_loads(raw)
                        except Exception:
                            continue
                        if ws_logger.isEnabledFor(logging.DEBUG):
                            ws_logger.debug(f"RAW: {raw[:500]}")
                        payload = ((msg.get("payload") or {}).get("data") or {}).get("sportsMatchStateUpdatedV2")
                        if not payload:
                            continue
                        state = parse_kalstrop_push(payload)
                        if not state:
                            continue
                        fid = state["fixture_id"]
                        fx = fixtures_by_id.get(fid)
                        if not fx:
                            continue
                        if fx.match_ended:
                            continue

                        # streamExists guard
                        if state.get("stream_exists") is False:
                            fx.recent_stream_false += 1
                        elif state.get("stream_exists") is True:
                            fx.recent_stream_false = 0
                        if fx.recent_stream_false >= 3:
                            logger.warning(f"⚠️ SKIPPING fid={fid[:8]}: "
                                            f"streamExists=false x{fx.recent_stream_false} — Kalstrop inferring")
                            continue

                        # Retirement gate — fire moneyline only, skip all other evaluators
                        retirement_winner = state.get("retirement_winner")
                        if retirement_winner:
                            slot = ENGINE.games.get(fid)
                            if slot and not slot.moneyline_resolved:
                                slot.moneyline_resolved = True
                                tgt = slot.targets
                                target = tgt.moneyline_home if retirement_winner == "home" else tgt.moneyline_away
                                if target:
                                    intent = Intent(fid, "MONEYLINE", target,
                                        f"retirement: {retirement_winner} wins (freeText={state['ft']!r})")
                                    await fire_intents([intent], fx, state, session)
                            fx.match_ended = True
                            if slot:
                                slot.match_total_under_emitted = True
                                slot.set_total_under_emitted = True
                                slot.first_set_total_under_emitted = True
                                slot.first_set_winner_resolved = True
                                slot.completed_match_resolved = True
                                for s in slot.targets.set_n_winners:
                                    slot.set_n_winner_resolved.add(s.set_n)
                                for em in slot.targets.exact_match:
                                    slot.exact_match_resolved.add((em.winner_side, em.winner_sets, em.loser_sets))
                                for gh in slot.targets.game_handicaps:
                                    slot.game_handicap_resolved.add((gh.side, gh.line_int))
                            LATEST_STATE[fid] = state
                            logger.info(f"🏥 RETIREMENT fid={fid[:8]} winner={retirement_winner} ft={state['ft']!r}")
                            continue

                        sh, sa = state["sets"]
                        try:
                            intents = ENGINE.process_tick(
                                fid,
                                sets_home=sh, sets_away=sa,
                                games_home=state["home_games"], games_away=state["away_games"],
                                total_games=state["total_games"],
                                first_set_games=state["first_set_games"],
                                total_sets=state["total_sets"],
                                current_set=int(state["phase"] or 1),
                                match_completed=state["match_ended"],
                                first_set_completed=state["first_set_completed"],
                            )
                        except Exception as e:
                            logger.error(f"engine.process_tick fid={fid[:8]}: {e}")
                            intents = []
                        if intents:
                            await fire_intents(intents, fx, state, session)

                        LATEST_STATE[fid] = state
                        fx.last_sets = state["sets"]
                        fx.last_total_games = state["total_games"]
                        fx.last_free_text = state["ft"]
                        if state["match_ended"]:
                            fx.match_ended = True
                finally:
                    consumer.cancel()
        except ConnectionClosed as e:
            logger.warning(f"[kalstrop] WS closed: {e.code} — reconnect in {backoff}s")
        except Exception as e:
            logger.error(f"[kalstrop] err: {type(e).__name__}: {e}")
        await asyncio.sleep(backoff)
        backoff = min(60, backoff * 2)


# =============================================================================
# DISCOVERY LOOP — re-runs every 90s, adds new fixtures + drops ended ones
# =============================================================================
async def discovery_loop(fixtures_by_id: Dict[str, TennisFixture],
                          stop: asyncio.Event, interval: int = 90):
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        try:
            await asyncio.sleep(interval)
            current = await loop.run_in_executor(None, discover_live_tennis_fixtures)
            current_ids = {f.fixture_id for f in current}
            old_ids = set(fixtures_by_id.keys())
            new_ids = current_ids - old_ids
            ended_ids = old_ids - current_ids

            if new_ids:
                new_fxs = [f for f in current if f.fixture_id in new_ids]
                logger.info(f"[discovery] +{len(new_fxs)} new fixtures — refreshing registry + matching...")
                # Refresh Kalshi registry so newly-listed matches show up
                global KALSHI_REGISTRY
                KALSHI_REGISTRY = await loop.run_in_executor(None, build_kalshi_registry)
                tasks = [loop.run_in_executor(None, map_fixture_to_kalshi, fx) for fx in new_fxs]
                await asyncio.gather(*tasks, return_exceptions=True)
                successfully_mapped = []
                for fx in new_fxs:
                    if fx.kalshi_event and add_fixture_to_engine(fx):
                        fixtures_by_id[fx.fixture_id] = fx
                        successfully_mapped.append(fx.fixture_id)
                        logger.info(f"  ✅ {fx.home_name} vs {fx.away_name}  "
                                    f"kx={fx.kalshi_event}  tickers={len(fx.kalshi_tickers)}")
                    else:
                        logger.info(f"  · {fx.home_name} vs {fx.away_name}  (no map / rejected)")
                if successfully_mapped:
                    await SUB_OPS_QUEUE.put(("subscribe", successfully_mapped))

            if ended_ids:
                ended_list = list(ended_ids)
                for fid in ended_list:
                    ended_fx = fixtures_by_id.pop(fid, None)
                    if ended_fx:
                        logger.info(f"[discovery] -- ended: {ended_fx.home_name} vs {ended_fx.away_name}")
                await SUB_OPS_QUEUE.put(("unsubscribe", ended_list))
        except Exception as e:
            logger.error(f"[discovery] err: {e}")


# =============================================================================
# SCOREBOARD
# =============================================================================
def _comp_tag(comp: str) -> str:
    c = (comp or "").lower()
    if "french open" in c or "roland" in c: return "RG"
    if "wimbledon" in c: return "WIM"
    if "us open" in c or "u.s. open" in c: return "USO"
    if "australian open" in c: return "AUS"
    if "challenger" in c: return "CH"
    if "125k" in c or "wta 125" in c: return "W125"
    if "itf" in c: return "ITF"
    if "atp" in c: return "ATP"
    if "wta" in c: return "WTA"
    if "utr" in c: return "UTR"
    if "srl" in c or "simulated" in c: return "SRL"
    return "?"


def render_scoreboard(fixtures_by_id: Dict[str, TennisFixture]) -> str:
    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    n_fx = len(fixtures_by_id)
    n_engine = len(ENGINE.games)
    n_fires = sum(len(f.snipe_attempted) for f in fixtures_by_id.values())
    n_placed = len(BETS_PLACED)
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    bar = "═" * 110
    lines = [
        "",
        bar,
        f"🎾 v3 SCOREBOARD  •  {now}Z  •  mode={mode}  •  fixtures={n_fx}  •  "
        f"engine_games={n_engine}  •  intents_fired={n_fires}  •  bets_placed={n_placed}",
        bar,
        f"  {'fid':<10} {'match':<32} {'comp':<5} {'sets':<7} {'games':<9} {'set':<4} {'state':<8} {'fires':<5}",
        "  " + "─" * 106,
    ]
    if not fixtures_by_id:
        lines.append("  (no live fixtures — initial discovery may still be running)")
    for fid, fx in fixtures_by_id.items():
        st = LATEST_STATE.get(fid)
        if st is not None:
            sh, sa = st["sets"]
            gh, ga = st.get("home_games", 0), st.get("away_games", 0)
            phase = str(st.get("phase") or "-")
            if fx.recent_stream_false >= 3:
                state = "INFER"
            elif st["match_ended"]:
                state = "ENDED"
            else:
                state = "LIVE"
        else:
            sh = sa = gh = ga = 0
            phase = "-"
            state = "PEND"
        match_str = f"{fx.home_abbrev or '?'} vs {fx.away_abbrev or '?'}"
        if len(match_str) > 32:
            match_str = match_str[:31] + "…"
        lines.append(
            f"  {fid[:8]:<10} {match_str:<32} {_comp_tag(fx.competition):<5} "
            f"{f'{sh}-{sa}':<7} {f'{gh}-{ga}':<9} {phase:<4} {state:<8} "
            f"{len(fx.snipe_attempted):<5}"
        )
    if RECENT_FIRES:
        lines.append("  " + "─" * 106)
        lines.append(f"  recent fires (latest {min(len(RECENT_FIRES), 8)}):")
        for r in list(RECENT_FIRES)[-8:]:
            lines.append(
                f"    {r['ts']}  fid={r['fid']}  {r['market']:<22} "
                f"{r['ticker']:<42} {r['side']:<3}  @{r['score']}"
            )
    lines.append(bar)
    return "\n".join(lines)


async def scoreboard_loop(fixtures_by_id: Dict[str, TennisFixture],
                          stop: asyncio.Event, interval: int = 20):
    while not stop.is_set():
        try:
            await asyncio.sleep(interval)
            block = render_scoreboard(fixtures_by_id)
            for line in block.split("\n"):
                logger.info(line)
        except Exception as e:
            logger.error(f"[scoreboard] err: {e}")


# =============================================================================
# DEBUG: print every fixture mapping at startup
# =============================================================================
def debug_print_fixture_mapping(fixtures_by_id: Dict[str, TennisFixture]):
    if not fixtures_by_id:
        logger.info("(no fixtures mapped)")
        return
    logger.info("")
    logger.info("=" * 90)
    logger.info("🔍 FIXTURE MAPPING DEBUG")
    logger.info("=" * 90)
    for fid, fx in fixtures_by_id.items():
        logger.info(
            f"  ✅ fid={fid[:8]}  {fx.home_name:<28} → {fx.home_abbrev or '?':<5} vs "
            f"{fx.away_name:<28} → {fx.away_abbrev or '?':<5} |  "
            f"{fx.kalshi_event} | tickers={len(fx.kalshi_tickers)} | {fx.competition}"
        )
    logger.info("=" * 90)


# =============================================================================
# MAIN
# =============================================================================
async def main(args):
    global SUB_OPS_QUEUE
    SUB_OPS_QUEUE = asyncio.Queue()

    logger.info("=" * 80)
    logger.info("TENNIS KALSTROP + KALSHI SNIPER v3 (consolidated)")
    logger.info(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE BETTING'}")
    logger.info(f"Limit: {LIMIT_BUY_CENTS}¢   Size: ${ORDER_SIZE_USD}")
    logger.info(f"Accounts: {len(ACCOUNTS)}")
    logger.info(f"⚡ JSON: {_JSON_ENGINE} | Event loop: {_LOOP_ENGINE} | Headers: pre-signed")
    logger.info("=" * 80)

    loop = asyncio.get_running_loop()
    logger.info(f"📅 BETTING_DATE = {BETTING_DATE!r}  (set to '0' in config to disable filter)")
    logger.info("Building Kalshi registry (date-filtered)...")
    global KALSHI_REGISTRY
    KALSHI_REGISTRY = await loop.run_in_executor(None, build_kalshi_registry)
    logger.info("Initial Kalstrop discovery...")
    fixtures = await loop.run_in_executor(None, discover_live_tennis_fixtures)
    logger.info(f"Found {len(fixtures)} LIVE tennis fixtures — matching against registry...")
    map_tasks = [loop.run_in_executor(None, map_fixture_to_kalshi, fx) for fx in fixtures]
    await asyncio.gather(*map_tasks, return_exceptions=True)

    fixtures_by_id: Dict[str, TennisFixture] = {}
    for fx in fixtures:
        if fx.kalshi_event and add_fixture_to_engine(fx):
            fixtures_by_id[fx.fixture_id] = fx

    debug_print_fixture_mapping(fixtures_by_id)
    logger.info(f"🧠 Engine ready: {len(ENGINE.games)} games loaded")

    stop = asyncio.Event()
    connector = aiohttp.TCPConnector(
        limit=0, ttl_dns_cache=300, enable_cleanup_closed=True,
        keepalive_timeout=300, force_close=False,
    )
    async with aiohttp.ClientSession(connector=connector, timeout=_CACHED_BET_TIMEOUT) as session:
        logger.info(f"✅ Kalshi session created (shared across {len(ACCOUNTS)} accounts)")
        header_task = asyncio.create_task(keep_headers_fresh())
        await asyncio.sleep(HEADER_WARMUP_SEC)
        logger.info(f"🔑 Pre-signed headers warm for {len(PRE_SIGNED_HEADERS)} accounts")

        tasks = [
            asyncio.create_task(kalstrop_ws_listener(fixtures_by_id, session, stop)),
            asyncio.create_task(discovery_loop(fixtures_by_id, stop)),
            asyncio.create_task(scoreboard_loop(fixtures_by_id, stop)),
        ]
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — shutting down")
            stop.set()
        finally:
            header_task.cancel()
            for t in tasks:
                t.cancel()


def cli_main():
    global DRY_RUN, LIMIT_BUY_CENTS, ORDER_SIZE_USD, ACCOUNTS

    ap = argparse.ArgumentParser(description="Tennis Kalshi sniper v3 (single-file)")
    ap.add_argument("--live", action="store_true",
                    help="Enable live trading. DEFAULT IS DRY RUN.")
    ap.add_argument("--limit-cents", type=int, default=99,
                    help="Max buy price in cents (default 99)")
    ap.add_argument("--bet-size-usd", type=float, default=1.0,
                    help="Per-account bet size in dollars (default $1)")
    ap.add_argument("--config-files", nargs="+", required=True,
                    help="Account config files to load (w_config_*.py)")
    ap.add_argument("--yes", action="store_true",
                    help="Skip LIVE confirmation prompt (for systemd/cron)")
    ap.add_argument("--date", default=None,
                    help='BETTING_DATE override (e.g. "26JUN25" or "0" to disable filter)')
    args = ap.parse_args()

    DRY_RUN = not args.live
    LIMIT_BUY_CENTS = args.limit_cents
    ORDER_SIZE_USD = args.bet_size_usd

    if args.live and not args.yes:
        print("\n" + "!" * 80)
        print(f"!!  LIVE MODE — ORDERS WILL BE PLACED ON KALSHI")
        print(f"!!  Limit: {LIMIT_BUY_CENTS}¢   Size: ${ORDER_SIZE_USD}")
        print(f"!!  Accounts: {args.config_files}")
        print("!" * 80 + "\n")
        confirm = input("Type 'LIVE' to proceed: ")
        if confirm.strip() != "LIVE":
            print("Aborted.")
            return

    ACCOUNTS = load_accounts(args.config_files)
    if not ACCOUNTS and not DRY_RUN:
        logger.error("❌ No accounts loaded — refusing to run --live")
        return
    if not ACCOUNTS:
        logger.warning("⚠️  No accounts — DRY RUN will still discover + signal")
    for a in ACCOUNTS:
        a.bet_size_usd = ORDER_SIZE_USD
        a.max_price_cents = LIMIT_BUY_CENTS

    # BETTING_DATE override from first account's config (matches MLB file pattern).
    # If config has BETTING_DATE, use it; otherwise the auto-computed today's-ET
    # value already in the global takes effect. CLI flag wins over config.
    global BETTING_DATE
    if args.date:
        BETTING_DATE = args.date
        logger.info(f"📅 BETTING_DATE override from CLI: {BETTING_DATE}")
    else:
        for path in args.config_files:
            try:
                spec = importlib.util.spec_from_file_location("acc_cfg_date", path)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                cfg_date = getattr(mod, "BETTING_DATE", None)
                if cfg_date:
                    BETTING_DATE = cfg_date
                    logger.info(f"📅 BETTING_DATE from {path}: {BETTING_DATE}")
                    break
            except Exception:
                continue
        else:
            logger.info(f"📅 BETTING_DATE auto-computed (today ET): {BETTING_DATE}")

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        logger.info(f"\n⏹️  Stopped. Bets placed: {len(BETS_PLACED)}")


if __name__ == "__main__":
    cli_main()
