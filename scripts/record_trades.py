#!/usr/bin/env python3
"""
Record Polymarket trades for linked games via market + user channel WebSockets.

Usage:
    python scripts/record_trades.py \
        --league rgm rgw \
        --market-type moneyline \
        --out ./captures/trades/tennis_moneyline \
        --duration 14400

    python scripts/record_trades.py \
        --league fifwc \
        --out ./captures/trades/fifwc_all \
        --link-run-id 5

Output per game directory:
    trades.jsonl       — all market trades (from market channel)
    best_bid_ask.jsonl — best bid/ask changes (from market channel)
    my_fills.jsonl     — our fills only (from user channel, requires POLY_EXEC_* env vars)
"""

import argparse
import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path

try:
    import websockets
    import websockets.exceptions
except ImportError:
    print("pip install websockets"); sys.exit(2)

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
HEARTBEAT_S = 10.0
RECONNECT_BASE_S = 2.0
RECONNECT_MAX_S = 30.0


def _safe_dir_name(name: str) -> str:
    return name.replace(" ", "_").replace("/", "_").replace(".", "")


# Allow imports from the config/ directory
_root = Path(__file__).resolve().parent.parent
_config = _root / "config"
if str(_config) not in sys.path:
    sys.path.insert(0, str(_config))


def _primary_provider(league_cfg: dict) -> str:
    raw = league_cfg.get("provider", "")
    if isinstance(raw, list):
        return str(raw[0]).strip().lower() if raw else ""
    return str(raw).strip().lower()


def _resolve_run_id(db_path: str, league: str, explicit_run_id: int | None) -> int:
    if explicit_run_id is not None:
        return explicit_run_id
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT run_id FROM link_run_provider_games
        WHERE canonical_league = ?
        ORDER BY run_id DESC LIMIT 1
        """,
        (league,),
    ).fetchone()
    conn.close()
    if row is None:
        print(f"[!] No link run found for league={league}. Run 'polybot2 link build' first.")
        sys.exit(1)
    return int(row["run_id"])


def _extract_tokens(
    db_path: str, leagues: list[str], run_id: int, market_types: list[str] | None,
) -> tuple[dict[str, dict], set[str], int]:
    """Compile plan per league and extract token_id → metadata with strategy keys."""
    from mappings import LEAGUES
    from polybot2.data.storage.database import open_database, DataRuntimeConfig
    from polybot2.hotpath.compiler import compile_hotpath_plan, HotPathPlanError
    from polybot2.linking.mapping_loader import load_live_trading_policy

    policy = load_live_trading_policy()
    token_lookup: dict[str, dict] = {}
    condition_ids: set[str] = set()
    total_games = 0

    cfg = DataRuntimeConfig(db_path=db_path)
    with open_database(cfg) as db:
        for lk in leagues:
            lcfg = LEAGUES.get(lk, {})
            provider = _primary_provider(lcfg)
            sport = lcfg.get("sport_family", "soccer")
            stw = int(lcfg.get("sets_to_win", 2))
            if not provider:
                print(f"[!] No provider for league={lk}, skipping")
                continue
            try:
                plan = compile_hotpath_plan(
                    db=db, provider=provider, league=lk, run_id=run_id,
                    sport=sport, sets_to_win=stw, live_policy=policy,
                    now_ts_utc=int(time.time()), plan_horizon_hours=8760,
                    include_inactive=True,
                )
            except HotPathPlanError as exc:
                print(f"[!] compile failed for league={lk}: {exc} — skipping")
                continue

            for game in plan.games:
                total_games += 1
                game_label = f"{game.canonical_home_team} vs {game.canonical_away_team}"
                for market in game.markets:
                    mt = market.sports_market_type
                    if market_types and mt not in market_types:
                        continue
                    cid = str(market.condition_id or "").strip()
                    if cid:
                        condition_ids.add(cid)
                    for target in market.targets:
                        tok = str(target.token_id or "").strip()
                        if not tok:
                            continue
                        token_lookup[tok] = {
                            "sk": target.strategy_key,
                            "game": game_label,
                            "league": game.canonical_league,
                            "market_type": mt,
                            "label": target.outcome_label,
                        }

    return token_lookup, condition_ids, total_games


class GameFiles:
    """Manages per-game, per-market-type output files (created lazily)."""

    def __init__(self, out_path: Path, game_names: list[str], has_user_channel: bool):
        self._out_path = out_path
        self._has_user_channel = has_user_channel
        self._game_dirs: dict[str, Path] = {}
        for gn in game_names:
            gd = out_path / _safe_dir_name(gn)
            gd.mkdir(parents=True, exist_ok=True)
            self._game_dirs[gn] = gd
        self._open_files: dict[str, any] = {}
        self.trade_count = 0
        self.bba_count = 0
        self.fill_count = 0

    def _get_file(self, game: str, key: str) -> any:
        """Get or lazily open a file handle keyed by 'game::key'."""
        fk = f"{game}::{key}"
        f = self._open_files.get(fk)
        if f is None:
            gd = self._game_dirs.get(game)
            if gd is None:
                return None
            f = (gd / f"{key}.jsonl").open("a")
            self._open_files[fk] = f
        return f

    def write_trade(self, game: str, market_type: str, entry: dict) -> None:
        f = self._get_file(game, f"trades_{market_type}")
        if not f:
            return
        f.write(json.dumps(entry) + "\n")
        self.trade_count += 1
        if self.trade_count % 50 == 0:
            f.flush()
            print(f"  [{self.trade_count} trades, {self.bba_count} bba, {self.fill_count} fills]")

    def write_bba(self, game: str, market_type: str, entry: dict) -> None:
        f = self._get_file(game, f"bba_{market_type}")
        if not f:
            return
        f.write(json.dumps(entry) + "\n")
        self.bba_count += 1
        if self.bba_count % 200 == 0:
            f.flush()

    def write_fill(self, game: str, market_type: str, entry: dict) -> None:
        if not self._has_user_channel:
            return
        f = self._get_file(game, f"fills_{market_type}")
        if not f:
            return
        f.write(json.dumps(entry) + "\n")
        f.flush()
        self.fill_count += 1
        print(f"  [FILL] {entry.get('game', '')} {entry.get('sk', '')} "
              f"price={entry.get('price', '')} size={entry.get('size', '')} "
              f"eid={entry.get('taker_order_id', '')[:16]}...")

    def close(self) -> None:
        for f in self._open_files.values():
            f.flush()
            f.close()


async def _run_market_ws(
    token_lookup: dict[str, dict],
    gf: GameFiles,
    stop: asyncio.Event,
) -> None:
    token_ids = list(token_lookup.keys())
    sub_msg = json.dumps({
        "assets_ids": token_ids,
        "type": "market",
        "custom_feature_enabled": True,
    })
    reconnect_delay = RECONNECT_BASE_S

    while not stop.is_set():
        try:
            async with websockets.connect(
                MARKET_WS_URL,
                ping_interval=None, ping_timeout=None,
                max_size=10 * 1024 * 1024,
            ) as ws:
                await ws.send(sub_msg)
                print(f"[market] connected, monitoring {len(token_ids)} tokens")
                reconnect_delay = RECONNECT_BASE_S
                hb = asyncio.create_task(_heartbeat(ws, stop))
                try:
                    async for raw in ws:
                        if stop.is_set():
                            break
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(data, dict):
                            continue
                        et = data.get("event_type", "")
                        if et not in ("last_trade_price", "best_bid_ask"):
                            continue
                        asset_id = str(data.get("asset_id", ""))
                        meta = token_lookup.get(asset_id)
                        if meta is None:
                            continue
                        now_ms = int(time.time() * 1000)
                        game = meta.get("game", "")
                        mt = meta.get("market_type", "unknown")

                        if et == "last_trade_price":
                            gf.write_trade(game, mt, {
                                "ts": now_ms, "token_id": asset_id, **meta,
                                "price": data.get("price", ""),
                                "side": data.get("side", ""),
                                "size": data.get("size", ""),
                            })
                        elif et == "best_bid_ask":
                            gf.write_bba(game, mt, {
                                "ts": now_ms, "token_id": asset_id, **meta,
                                "best_bid": data.get("best_bid", ""),
                                "best_ask": data.get("best_ask", ""),
                                "spread": data.get("spread", ""),
                            })
                finally:
                    hb.cancel()
                    try:
                        await hb
                    except asyncio.CancelledError:
                        pass
                # Clean exit from async for (server closed gracefully)
                if not stop.is_set():
                    print(f"[market] connection closed, reconnecting...")
        except (websockets.exceptions.ConnectionClosed, Exception) as exc:
            if stop.is_set():
                break
            print(f"[market] {type(exc).__name__}: {exc}, reconnecting in {reconnect_delay:.0f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)


async def _run_user_ws(
    token_lookup: dict[str, dict],
    condition_ids: set[str],
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    gf: GameFiles,
    stop: asyncio.Event,
) -> None:
    sub_msg = json.dumps({
        "auth": {
            "apiKey": api_key,
            "secret": api_secret,
            "passphrase": api_passphrase,
        },
        "markets": sorted(condition_ids),
        "type": "user",
    })
    reconnect_delay = RECONNECT_BASE_S

    while not stop.is_set():
        try:
            async with websockets.connect(
                USER_WS_URL,
                ping_interval=None, ping_timeout=None,
                max_size=10 * 1024 * 1024,
            ) as ws:
                await ws.send(sub_msg)
                print(f"[user] connected, monitoring {len(condition_ids)} condition IDs")
                reconnect_delay = RECONNECT_BASE_S
                hb = asyncio.create_task(_heartbeat(ws, stop))
                try:
                    async for raw in ws:
                        if stop.is_set():
                            break
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(data, dict):
                            continue
                        if data.get("event_type") != "trade":
                            continue
                        asset_id = str(data.get("asset_id", ""))
                        meta = token_lookup.get(asset_id)
                        if meta is None:
                            continue
                        now_ms = int(time.time() * 1000)
                        game = meta.get("game", "")
                        mt = meta.get("market_type", "unknown")
                        gf.write_fill(game, mt, {
                            "ts": now_ms,
                            "token_id": asset_id,
                            "taker_order_id": str(data.get("taker_order_id", "")),
                            **meta,
                            "price": data.get("price", ""),
                            "side": data.get("side", ""),
                            "size": data.get("size", ""),
                            "status": data.get("status", ""),
                        })
                finally:
                    hb.cancel()
                    try:
                        await hb
                    except asyncio.CancelledError:
                        pass
                if not stop.is_set():
                    print(f"[user] connection closed, reconnecting...")
        except (websockets.exceptions.ConnectionClosed, Exception) as exc:
            if stop.is_set():
                break
            print(f"[user] {type(exc).__name__}: {exc}, reconnecting in {reconnect_delay:.0f}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)


async def _heartbeat(ws, stop: asyncio.Event):
    try:
        while not stop.is_set():
            await asyncio.sleep(HEARTBEAT_S)
            try:
                await ws.send("PING")
            except Exception:
                break
    except asyncio.CancelledError:
        pass


async def _run(
    token_lookup: dict[str, dict],
    condition_ids: set[str],
    out_path: Path,
    duration: int,
) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    api_key = os.environ.get("POLY_EXEC_API_KEY", "")
    api_secret = os.environ.get("POLY_EXEC_API_SECRET", "")
    api_passphrase = os.environ.get("POLY_EXEC_API_PASSPHRASE", "")
    has_user = bool(api_key and api_secret and api_passphrase)

    if not has_user:
        print("[!] POLY_EXEC_* credentials not set — user channel (my_fills) disabled")

    out_path.mkdir(parents=True, exist_ok=True)
    game_names = sorted({m["game"] for m in token_lookup.values()})
    gf = GameFiles(out_path, game_names, has_user)

    async def _timeout():
        await asyncio.sleep(duration)
        print(f"\n[+] duration ({duration}s) elapsed")
        stop.set()

    timeout_task = asyncio.create_task(_timeout())

    tasks = [_run_market_ws(token_lookup, gf, stop)]
    if has_user:
        tasks.append(_run_user_ws(
            token_lookup, condition_ids,
            api_key, api_secret, api_passphrase,
            gf, stop,
        ))

    try:
        await asyncio.gather(*tasks)
    finally:
        timeout_task.cancel()
        gf.close()
        print(f"\n[+] done. {gf.trade_count} trades, {gf.bba_count} bba, {gf.fill_count} fills across {len(game_names)} games")
        for gn in game_names:
            print(f"    {out_path / _safe_dir_name(gn)}/")


def main():
    ap = argparse.ArgumentParser(description="Record Polymarket trades for linked games")
    ap.add_argument("--league", nargs="+", required=True, help="Canonical league key(s)")
    ap.add_argument("--market-type", nargs="+", default=None,
                     help="Filter by market type (e.g., moneyline totals btts). Default: all")
    ap.add_argument("--link-run-id", type=int, default=None, help="Explicit link run ID")
    ap.add_argument("--duration", type=int, default=14400, help="Max seconds (default 14400)")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--db", default="data/prediction_markets.db", help="SQLite DB path")
    args = ap.parse_args()

    run_id = _resolve_run_id(args.db, args.league[0], args.link_run_id)
    print(f"[+] using run_id={run_id}")

    token_lookup, condition_ids, game_count = _extract_tokens(
        args.db, args.league, run_id, args.market_type,
    )
    mt_label = ", ".join(args.market_type) if args.market_type else "all"
    print(f"[+] {game_count} games, {len(token_lookup)} tokens, {len(condition_ids)} conditions (market_types: {mt_label})")

    if not token_lookup:
        print("[!] no tokens matched — check league and market-type filters")
        return 1

    asyncio.run(_run(token_lookup, condition_ids, Path(args.out), args.duration))
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
