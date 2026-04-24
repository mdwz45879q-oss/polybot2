# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

polybot2 is a sports-trading bot for Polymarket. Hybrid architecture: Python control plane (~16k LOC) for CLI, data sync, linking, and orchestration; Rust native hotpath (~5.5k LOC) via PyO3/maturin for low-latency score ingest → decision → order dispatch. Current scope: MLB + Kalstrop provider. Deployment: Linux EC2 (eu-west-1).

## Build & Test

**Rust native module (required for hotpath):**
```bash
# macOS (conda + system Python conflict):
env -u CONDA_PREFIX maturin build --release --manifest-path native/polybot2_native/Cargo.toml --interpreter python3
pip install --force-reinstall native/polybot2_native/target/wheels/polybot2_native-*.whl

# Linux / clean virtualenv:
maturin develop --manifest-path native/polybot2_native/Cargo.toml
```

**Python package (editable):**
```bash
pip install -e ".[dev]"
```

**Tests:**
```bash
cargo test --manifest-path native/polybot2_native/Cargo.toml   # 15 Rust tests
pytest tests/ --ignore=tests/live -q                            # 146 Python tests
pytest tests/test_polybot2_hotpath_observe.py -k "heartbeat"    # single test
```

Tests in `tests/live/` require real API credentials and `POLYBOT2_ENABLE_LIVE_*` env vars.

## Architecture

### Data Flow

```
polybot2 data sync          → SQLite (markets, tokens, events)
polybot2 provider sync      → SQLite (provider games)
polybot2 link build         → SQLite (link runs, bindings)
polybot2 link review session → SQLite (decisions)
polybot2 hotpath run        → Compiled plan → Rust runtime → WS → engine → dispatch → Polymarket CLOB
                              ↓ telemetry (Unix DGRAM socket)
polybot2 hotpath observe    → scoreboard + execution log
```

### Rust Native Hotpath (`native/polybot2_native/src/`)

| Module | Role |
|--------|------|
| `engine.rs` | Core: PyO3 interface, `process_tick` orchestration, game state management, plan loading |
| `eval.rs` | MLB evaluation strategies: totals (over/under), NRFI, moneyline, spread |
| `parse.rs` | Tick parsing from Kalstrop WS frames (PyO3 + serde_json variants), extraction helpers |
| `dispatch/flow.rs` | Intent → order lifecycle: noop (paper) and http (live) paths, cancel-replace |
| `dispatch/presign_pool.rs` | Presign order pool: parallel warmup at startup, incremental refill during runtime |
| `dispatch/sdk_exec.rs` | Polymarket SDK integration: sign, submit, cancel, get orders |
| `dispatch/events.rs` | Telemetry event emission helpers, lifecycle status mapping |
| `ws.rs` | Live worker: Kalstrop WS connection, subscription management, heartbeat emission |
| `telemetry.rs` | Non-blocking Unix DGRAM telemetry: bounded channel → worker thread → socket |
| `runtime.rs` | PyO3 `NativeHotPathRuntime`: start/stop, health snapshot, FFI boundary |

### Python Control Plane (`src/polybot2/`)

| Module | Role |
|--------|------|
| `_cli/` | argparse CLI: commands for data, provider, link, hotpath |
| `data/` | Market sync from Polymarket CLOB API, SQLite storage |
| `linking/` | Deterministic provider↔Polymarket matching, review workflows |
| `execution/` | Config container for Rust dispatch (order methods removed — Rust handles all dispatch) |
| `hotpath/` | Plan compiler, native service adapter, replay, observe monitor |
| `hotpath/mlb/` | `MlbOrderPolicy` dataclass (evaluation logic moved to Rust `eval.rs`) |
| `sports/` | Provider abstractions: Kalstrop (WS), BoltOdds (REST) |
| `config/` | `live_trading.py` (execution policy), `mappings.py` (team aliases, league rules) |

### FFI Boundary (Python → Rust)

Python `NativeHotPathService` calls Rust `NativeHotPathRuntime` via PyO3:
- `start(config_json, compiled_plan_json, exec_config_json)` — all configs are `deny_unknown_fields` JSON
- `stop()`, `set_subscriptions(ids)`, `prewarm_presign(templates_json)`, `health_snapshot()`
- Compiled plan serialized via `serialize_compiled_plan()` in `native_engine.py`

### Telemetry Pipeline

Rust emits events via `TelemetryEmitter.emit()` → bounded `sync_channel(4096)` → worker thread → `UnixDatagram::send_to("/tmp/polybot2_hotpath_telemetry.sock")`. Python observe monitor binds a DGRAM socket at the same path. **The monitor must bind BEFORE the Rust worker starts** (socket race condition — monitor.start() before hotpath.start()).

macOS Unix DGRAM limit is 2048 bytes. Heartbeat payloads use compact field names (`h`, `a`, `s`, `inn`, `half`) to stay under this limit.

## Critical Invariants

1. **Hotpath ordering:** Match update → decision work → order submission → telemetry. Telemetry must never block or delay the decision/submit path.

2. **Fail-closed:** Presign pool miss → error (no fallback to unsigned submit). TIF must be FAK. Startup warmup failure → process won't trade.

3. **Spread evaluation formula:** `margin + line > 0` where the compiler negates the line for the complement ("No" label) side of spread markets.

4. **Totals over crossing:** `bisect_right` returns the insertion point — check `hi_raw > 0` before using `hi_raw - 1` as the target index. If 0, no line was crossed.

5. **One-shot intents:** Each strategy key can emit at most one intent per session (tracked via `attempted_strategy_keys` HashSet).

## First Steps for a New Session

Begin with a comprehensive audit of the Rust hot path, with special attention to:

1. **Telemetry pipeline** — verify events flow end-to-end from Rust emission through the Unix DGRAM socket to the Python observe store. Check `health_snapshot()` for `telemetry_emitted` vs `telemetry_dropped` counts.
2. **MLB engine and triggering logic** (`eval.rs`, `engine.rs`) — trace `process_tick` → `evaluate_totals` / `evaluate_nrfi` / `evaluate_final` paths. Verify cooldown/debounce/one-shot filtering behaves correctly.
3. **Subscription lifecycle** — `ws.rs` manages active subscriptions via `refresh_active_subscriptions`. Games enter/leave the active set based on `subscribe_lead_minutes` and kickoff time.

### Known Gaps Requiring Attention

- **Subscription events not granular enough.** The `subscriptions_changed` event emits the total active count but does NOT emit per-game subscribe/unsubscribe events (e.g., "game X subscribed", "game Y unsubscribed"). This makes it impossible for operators to see exactly which games the hotpath is tracking at any moment.

- **Missing unit tests for triggering edge cases.** The evaluation logic in `eval.rs` has no unit tests for:
  - **Mid-game subscription:** subscribing to a game already in progress (e.g., score is 3-2 in the 5th inning). The engine must correctly handle the initial state without falsely triggering on the first tick (e.g., the totals `bisect_right` logic must not treat the first observed total as a "crossing" from 0).
  - **Post-game subscription:** subscribing to a game that has already finished. The engine must not place any bets — `is_game_completed` should gate all evaluations, and `match_completed` must be correctly parsed from the initial tick.
  - **NRFI after first inning:** subscribing after the first inning is complete — the engine must not emit NRFI signals for innings that have already passed.

## CLI Commands

```bash
polybot2 data sync --db path.sqlite
polybot2 provider sync --provider kalstrop --league mlb --db path.sqlite
polybot2 link build --provider kalstrop --league mlb --db path.sqlite
polybot2 link review session --provider kalstrop --league mlb --link-run-id N --db path.sqlite
polybot2 hotpath run --provider kalstrop --league mlb --link-run-id N --execution-mode paper --with-observe
polybot2 hotpath replay --provider kalstrop --league mlb --link-run-id N --capture-manifest path.json
polybot2 hotpath observe
polybot2 mapping validate
```

## Environment Variables

Required for live execution: `POLY_EXEC_API_KEY`, `POLY_EXEC_API_SECRET`, `POLY_EXEC_API_PASSPHRASE`, `POLY_EXEC_PRESIGN_PRIVATE_KEY`, `POLY_EXEC_FUNDER`.

Provider credentials: `KALSTROP_CLIENT_ID`, `KALSTROP_SHARED_SECRET_RAW` (or legacy `CLIENT_ID`, `SHARED_SECRET_RAW`).

Database: `POLYBOT2_DB_PATH` (default: `../../data/prediction_markets.db` relative to working dir).

## Constraints

- Deployment target is Linux EC2 (eu-west-1). Dev is macOS (ARM). macOS has a 2048-byte Unix DGRAM limit that Linux does not.
- Python ≥ 3.11, Rust edition 2021, PyO3 0.22 with ABI3.
- `polymarket-client-sdk` 0.4.4 pins `alloy` at 1.6.3 — do not add a different alloy version or traits will mismatch.
- Prefer deletion over compatibility shims. No backwards-compat wrappers for removed features.
