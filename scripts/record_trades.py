#!/usr/bin/env python3
"""
Record Polymarket trades for linked games via the market channel WebSocket.

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

Output:
    {out}/trades.jsonl  — one JSON line per trade
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
HEARTBEAT_S = 10.0
RECONNECT_BASE_S = 2.0
RECONNECT_MAX_S = 30.0


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
        SELECT lr.run_id FROM link_runs lr
        INNER JOIN link_run_provider_games pg ON pg.run_id = lr.run_id
        WHERE pg.canonical_league = ?
        ORDER BY lr.run_id DESC LIMIT 1
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
) -> tuple[dict[str, dict], int]:
    """Extract token_id → metadata from DB for linked games, filtered by market type."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    token_lookup: dict[str, dict] = {}
    game_set: set[str] = set()

    for lk in leagues:
        rows = conn.execute(
            """
            SELECT
                t.token_id, t.outcome_label, t.outcome_index,
                m.condition_id, m.sports_market_type, m.question, m.line,
                pe.title AS event_title, pe.event_id,
                pg.provider_game_id, pg.canonical_league
            FROM link_run_provider_games pg
            INNER JOIN link_event_bindings eb
                ON eb.run_id = pg.run_id AND eb.provider_game_id = pg.provider_game_id
            INNER JOIN pm_events pe ON pe.event_id = eb.event_id
            INNER JOIN pm_markets m ON m.event_id = pe.event_id
            INNER JOIN pm_market_tokens t ON t.condition_id = m.condition_id
            WHERE pg.run_id = ? AND pg.canonical_league = ?
            """,
            (run_id, lk),
        ).fetchall()

        for row in rows:
            mt = str(row["sports_market_type"] or "").strip().lower()
            if market_types and mt not in market_types:
                continue
            tok = str(row["token_id"] or "").strip()
            if not tok:
                continue
            game_set.add(str(row["event_id"]))
            token_lookup[tok] = {
                "sk": "",
                "game": str(row["event_title"] or ""),
                "league": str(row["canonical_league"] or lk),
                "market_type": mt,
                "label": str(row["outcome_label"] or ""),
            }

    conn.close()
    return token_lookup, len(game_set)


async def _run(token_lookup: dict[str, dict], out_path: Path, duration: int):
    token_ids = list(token_lookup.keys())
    sub_msg = json.dumps({
        "assets_ids": token_ids,
        "type": "market",
        "custom_feature_enabled": True,
    })

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    out_path.mkdir(parents=True, exist_ok=True)

    # Build per-game file handles: game_name → (trades_file, bba_file)
    game_names = sorted({m["game"] for m in token_lookup.values()})
    game_files: dict[str, tuple] = {}
    for gn in game_names:
        safe_name = gn.replace(" ", "_").replace("/", "_").replace(".", "")
        game_dir = out_path / safe_name
        game_dir.mkdir(parents=True, exist_ok=True)
        game_files[gn] = (
            (game_dir / "trades.jsonl").open("a"),
            (game_dir / "best_bid_ask.jsonl").open("a"),
        )

    trade_count = 0
    bba_count = 0
    reconnect_delay = RECONNECT_BASE_S

    async def _timeout():
        await asyncio.sleep(duration)
        print(f"\n[+] duration ({duration}s) elapsed")
        stop.set()

    timeout_task = asyncio.create_task(_timeout())

    try:
        while not stop.is_set():
            try:
                async with websockets.connect(
                    MARKET_WS_URL,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    await ws.send(sub_msg)
                    print(f"[+] connected, monitoring {len(token_ids)} tokens")
                    reconnect_delay = RECONNECT_BASE_S

                    heartbeat_task = asyncio.create_task(_heartbeat(ws, stop))
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
                            event_type = data.get("event_type", "")
                            if event_type not in ("last_trade_price", "best_bid_ask"):
                                continue

                            asset_id = str(data.get("asset_id", ""))
                            meta = token_lookup.get(asset_id)
                            if meta is None:
                                continue

                            now_ms = int(time.time() * 1000)

                            gn = meta.get("game", "")
                            gf = game_files.get(gn)
                            if gf is None:
                                continue
                            tf, bf = gf

                            if event_type == "last_trade_price":
                                entry = {
                                    "ts": now_ms,
                                    "token_id": asset_id,
                                    **meta,
                                    "price": data.get("price", ""),
                                    "side": data.get("side", ""),
                                    "size": data.get("size", ""),
                                }
                                tf.write(json.dumps(entry) + "\n")
                                trade_count += 1
                                if trade_count % 50 == 0:
                                    tf.flush()
                                    print(f"  [{trade_count} trades, {bba_count} bba]")

                            elif event_type == "best_bid_ask":
                                entry = {
                                    "ts": now_ms,
                                    "token_id": asset_id,
                                    **meta,
                                    "best_bid": data.get("best_bid", ""),
                                    "best_ask": data.get("best_ask", ""),
                                    "spread": data.get("spread", ""),
                                }
                                bf.write(json.dumps(entry) + "\n")
                                bba_count += 1
                                if bba_count % 200 == 0:
                                    bf.flush()
                    finally:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass

            except websockets.exceptions.ConnectionClosed as exc:
                if stop.is_set():
                    break
                print(f"[!] connection closed: {exc}, reconnecting in {reconnect_delay:.0f}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)
            except Exception as exc:
                if stop.is_set():
                    break
                print(f"[!] {type(exc).__name__}: {exc}, reconnecting in {reconnect_delay:.0f}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_S)
    finally:
        timeout_task.cancel()
        for gn, (tf, bf) in game_files.items():
            tf.flush(); tf.close()
            bf.flush(); bf.close()
        print(f"\n[+] done. {trade_count} trades, {bba_count} best_bid_ask events across {len(game_files)} games")
        for gn in game_names:
            safe_name = gn.replace(" ", "_").replace("/", "_").replace(".", "")
            print(f"    {out_path / safe_name}/")


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

    token_lookup, game_count = _extract_tokens(
        args.db, args.league, run_id, args.market_type,
    )
    mt_label = ", ".join(args.market_type) if args.market_type else "all"
    print(f"[+] {game_count} games, {len(token_lookup)} tokens (market_types: {mt_label})")

    if not token_lookup:
        print("[!] no tokens matched — check league and market-type filters")
        return 1

    asyncio.run(_run(token_lookup, Path(args.out), args.duration))
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
