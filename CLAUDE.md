# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

polybot2 is a sports-trading bot for Polymarket. Hybrid architecture: Python control plane (~16k LOC) for CLI, data sync, linking, and orchestration; Rust native hotpath via PyO3/maturin for low-latency score ingest → decision → order dispatch. Current scope: MLB + Kalstrop provider. Deployment: Linux EC2 (eu-west-1).

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
cargo test --manifest-path native/polybot2_native/Cargo.toml   # Rust tests
pytest tests/ --ignore=tests/live -q                            # Python tests
pytest tests/test_polybot2_hotpath_observe.py -k "heartbeat"    # single test
```

Tests in `tests/live/` require real API credentials and `POLYBOT2_ENABLE_LIVE_*` env vars. The live Rust execution tests (`live_rust_submit_*`) are gated behind `POLYBOT2_ENABLE_LIVE_RUST_EXECUTION_TEST=1` and validate `OrderSubmitter::submit_order_async` (build + sign + post) against the real Polymarket CLOB using sub-minimum orders that get rejected without risking money.

`cargo build --release` outside maturin will fail to link Python symbols on macOS. Use `cargo check --release` for release-mode validation; use `maturin build` for a real wheel.

## Architecture

### Data Flow

```
polybot2 data sync           → SQLite (markets, tokens, events)
polybot2 provider sync       → SQLite (provider games)
polybot2 link build          → SQLite (link runs, bindings)
polybot2 link review session → SQLite (decisions)
polybot2 hotpath run         → Compiled plan → Rust runtime → WS → engine → DispatchHandle → channel → submitter → CLOB
polybot2 hotpath live        → Orchestrator: periodic data sync → provider sync → link build → recompile plan → restart hotpath
```

### Rust Native Hotpath (`native/polybot2_native/src/`)

The hot path is split across two threads. The **WS thread** parses frames, evaluates the plan, pops presigned orders for all intents in the frame, and hands them to the submitter as one `SubmitWork::Batch` — no HTTP, no string allocation on the success path. The **submitter thread** owns the SDK client, spawns each batch as a concurrent task (up to 3 in-flight via Semaphore), posts to the CLOB, and logs outcomes.

| Module | Role |
|--------|------|
| `kalstrop_types.rs` | Zero-copy serde structs for Kalstrop WS frames (`KalstropFrame<'a>`, etc.) |
| `parse.rs` | Tick parsing: zero-copy live path (`parse_tick_from_kalstrop_update`) + PyO3 paths for replay |
| `engine.rs` | Core: PyO3 interface, `process_tick`, `process_kalstrop_frame`, integer-indexed plan loading, per-game state. `clone_registry()` exposes `Arc<TargetRegistry>` for cross-thread sharing. |
| `eval.rs` | MLB evaluation strategies. All evaluators take `GameIdx` and return `Vec<RawIntent>`. Zero string formatting on the hot path. |
| `dispatch/flow.rs` | `DispatchHandle::pop_for_target(TargetIdx)` (sync, returns signed order or err) and `send_batch(SubmitBatch, &log)` (sync, sends one Batch on the channel). |
| `dispatch/presign_pool.rs` | Presign pool indexed by `TokenIdx` (`Vec<Option<PreSignedOrderData>>`). Depth is always 1 (one-shot intents). `warm_presign_startup_into` is a free function that signs one order per token at startup, borrowing the SDK client + signer from the submitter and writing into the dispatch handle's pool. |
| `dispatch/sdk_exec.rs` | `OrderSubmitter`: holds `Option<SdkRuntime>`, signs and posts orders. Methods: `ensure_sdk_runtime_async`, `post_signed_order_async`, `submit_signed_chunked_async`, `build_signed_order_async`, `submit_order_async`. Also `map_post_response` helper that treats empty `order_id` with `success: true` as failure. |
| `dispatch/submitter.rs` | `run_submitter_async`: dispatcher loop that receives `SubmitWork::Batch` and `tokio::spawn`s each batch as a concurrent submit task, bounded by `Semaphore(3)`. Each task calls SDK `post_order`/`post_orders` and logs outcomes. No cross-frame coalescing — each frame gets its own HTTP call immediately. Writes `SubmitterHealth` on init/success/error/exit. |
| `dispatch/types.rs` | `DispatchHandle`, `OrderSubmitter`, `SubmitWork`, `SubmitBatch`, `PreSignedOrderData`, `OrderRequestData`. |
| `ws.rs` | Live worker: Kalstrop WS connect, subscription management, frame drain loop. Uses `worker_clock_origin: Instant` for monotonic dedup/cooldown. Calls `process_decoded_frame_sync` (no `await` on dispatch). |
| `runtime.rs` | PyO3 `NativeHotPathRuntime`: builds both halves at startup with shared `Arc<TargetRegistry>`, runs presign warmup, spawns submitter and WS threads, lifecycle (`start`/`stop`). `health_snapshot()` exposes WS + submitter health. |
| `replay.rs` | `process_decoded_frame_sync`: parse → engine → pop presigned orders for all frame intents → send one `SubmitWork::Batch` → log ticks. Used by both live worker and replay. |
| `log_writer.rs` | Structured JSONL log. Wrapped in `Arc<Mutex<LogWriter>>` and shared by WS thread (logs ticks) and submitter thread (logs order outcomes). |

### Hot Path Pipeline

```
WS thread (per frame):
  serde_json::from_str<KalstropFrame<'a>>            (zero-copy)
  → parse_tick_from_kalstrop_update                  (1 heap alloc: universal_id)
  → engine.process_tick                              (game_id_to_idx → GameIdx, integer eval, ~100ns)
                                                      emits Intent { target_idx } — Copy, no strings
  → process_decoded_frame_sync builds one SubmitBatch per frame:
        for each Intent:
            DispatchHandle::pop_for_target(target_idx)   (Vec index + Option::take, ~100ns)
            batch.push((target_idx, signed_order))
        DispatchHandle::send_batch(batch, &log)          (one tx.send for the whole frame)
  → return to ws.next()                              (<1µs total per material frame)

Submitter dispatcher (independent):
  rx.recv().await                                    (blocks for next batch)
  semaphore.acquire_owned().await                    (max 3 concurrent submits)
  tokio::spawn(submit_batch_task(batch, client, registry, log, health))
    → if 1 item: post_order(signed)
    → else:      post_orders chunked (15/chunk)
    → record_outcome(health)
    → log_order_ok / log_order_err   (resolve sk/tok strings via Arc<TargetRegistry>)
    → drop(permit)                                   (release semaphore slot)
```

### TargetRegistry

`Arc<TargetRegistry>` is the read-only mapping `TargetIdx → TargetSlot { token_idx, strategy_key }` and `TokenIdx → TokenSlot { token_id }`. Built once in `engine.load_plan` and cloned into both `DispatchHandle` (WS thread) and `OrderSubmitter` (submitter thread).

The registry exists so the channel payload can be `(TargetIdx, SdkSignedOrder)` — strings (`strategy_key`, `token_id`) are reconstructed from the registry only at log time, on the submitter, after the order has been handed off. The WS thread allocates no strings on the success path.

The `tokens` vector is deduplicated by `token_id` at load time. A `TargetIdx` always references a `TokenIdx`; multiple targets pointing to the same token (rare in practice) share the single pool entry. This preserves the "one signed order per unique token" invariant — there is exactly one `Option` slot per `TokenIdx`, regardless of how many targets reference that token.

### Integer-Indexed Evaluator

At plan load, `engine.load_plan` interns each `provider_game_id` → `GameIdx(u16)` and each target → `TargetIdx(u16)`. Per-game compact arrays drive evaluation:

- `over_lines: Vec<OverLine { half_int: u16, target_idx }>` — sorted by `half_int`. On a score change from `prev` to `now`, iterate and match `half_int in [prev, now)`. A line of 5.5 is stored as `half_int = 5`.
- `under_lines: Vec<OverLine>` — same shape, fired at game completion when `half_int >= total`.
- `nrfi_yes`, `nrfi_no`: `Option<TargetIdx>` — direct slot access.
- `moneyline_home`, `moneyline_away`: `Option<TargetIdx>` — direct slot access.
- `spreads: Vec<(SpreadSide, f64, TargetIdx)>` — small, iterated at game end.

Per-game state (rows, GameState, resolution flags) is stored in `Vec<...>` indexed by `GameIdx`. Cooldown/debounce tracking uses `last_emit_ns: Vec<i64>` and `last_signature: Vec<Option<DecisionSig>>` indexed by `TargetIdx`. `DecisionSig { token_idx: TokenIdx }` is `Copy` — comparison is integer equality, no string allocation. One-shot gating is enforced by the presign pool (depth=1, `Option::take()`), not by the engine — there is no `attempted` bitset.

The only string hash on the live path is `game_id_to_idx.get(fixture_id)` — one `HashMap::get` per tick. After filtering, the engine emits `Intent { target_idx: TargetIdx }` (`Copy`, no strings). String resolution happens only at the FFI boundary (`process_score_event` looks up via the engine's own `tokens`/`target_slots`) and on the submitter (via the shared `Arc<TargetRegistry>` at log time). `line_key()` is dead code on the hot path; it remains in `eval.rs` only as a test helper (gated `#[cfg(test)]`).

The Python compiler (`compiler.py`) still produces strategy keys (`"gid:TOTAL:OVER:5.5"`, etc.) and embeds them in the compiled plan JSON. Rust stores them in `TargetSlot.strategy_key` for log output but never parses or hashes them.

### Decoupled Submitter

Two structs back the dispatch path:

- **`DispatchHandle`** (WS thread): `cfg`, `registry: Arc<TargetRegistry>`, `presign_template_catalog: HashMap<String, OrderRequestData>` (set-once from Python's `prewarm_presign`), `presign_templates: Vec<Option<OrderRequestData>>` indexed by `TokenIdx`, `presign_pool: Vec<Option<PreSignedOrderData>>` indexed by `TokenIdx` (depth=1, `Option::take()` for pop), `submit_tx: Option<UnboundedSender<SubmitWork>>`. All methods are synchronous.
- **`OrderSubmitter`** (submitter thread): `cfg`, `registry: Arc<TargetRegistry>`, `sdk_runtime: Option<...>`, `cached_signer: Option<...>`, `submit_rx`, `log: Arc<Mutex<LogWriter>>`, `health: Arc<Mutex<SubmitterHealth>>`. Async; owns the SDK client.

Channel: `tokio::sync::mpsc::unbounded_channel<SubmitWork>` where `SubmitWork::Batch(SmallVec<[(TargetIdx, SdkSignedOrder); 4]>)`. `UnboundedSender::send` is sync and never blocks. The WS thread sends exactly one Batch per material frame in `process_decoded_frame_sync`. The submitter dispatcher `tokio::spawn`s each batch immediately as a concurrent task, bounded by `Semaphore(3)`. No cross-frame coalescing — frame 2's HTTP call starts without waiting for frame 1's round-trip. Each spawned task handles its own `post_order`/`post_orders` call and releases the semaphore permit when done.

`SubmitWork::Stop` is the shutdown signal. `run_submitter_async` drains the in-flight batch before exiting on Stop. Channel-closed (sender dropped) also exits cleanly.

In **noop mode** no submitter thread is spawned; `DispatchHandle::submit_tx` stays `None`; `process_decoded_frame_sync` short-circuits to log `"noop"` per intent inline, never touching the pool or channel.

`LogWriter` is wrapped in `Arc<Mutex<>>` and shared between the WS thread (logs ticks via `log.log_tick`, `log_ws_connect`, `log_ws_disconnect`) and the submitter thread (logs order outcomes via `log_order_ok`/`log_order_err`). Lock contention is negligible — writes are buffered, hold time is sub-µs. **The success path on the WS thread holds zero log locks before the channel send** — tick logging happens after dispatch.

### Presign Pool

Pre-signs one order per unique token at startup so the WS thread can pop in ~100ns instead of ECDSA-signing in ~10–50ms. Pool depth is always 1 per token (one-shot intents fire each token at most once; `presign_pool_target_per_key` config removed). No runtime refill — startup warmup is the only presign path. Pool ownership lives on `DispatchHandle` (WS thread) as `Vec<Option<PreSignedOrderData>>` indexed by `TokenIdx` (`Option::take()` for pop). The SDK client used to sign warmup orders lives on `OrderSubmitter` (submitter thread). At startup:

1. `OrderSubmitter::ensure_sdk_runtime_async` initializes the SDK client.
2. `warm_presign_startup_into(&cfg, &client, &signer, &templates_slice, &mut pool_slice)` signs one order per token in parallel (`tokio::spawn` per token) and writes results into the handle's pool by `TokenIdx`.
3. `DispatchHandle::install_submit_tx(submit_tx)` wires the channel.
4. Submitter thread is spawned with `OrderSubmitter` (and channel rx); WS thread is spawned with `DispatchHandle` (and channel tx).

Presign pool miss → fail-closed error logged on the WS thread; no fallback to inline sign-and-submit. Startup warmup failure → process won't trade.

### WS Event Loop

```
'event_loop: loop {
    drain commands (non-blocking)
    maybe refresh subscriptions + resubscribe

    // Frame drain loop — process ALL pending frames first
    loop {
        read frame (100ms timeout on first, 0ms on subsequent)
        if timeout → break to housekeeping
        process_decoded_frame_sync(...)          // no .await on dispatch
    }

    log.lock().flush()
    // Housekeeping: only when socket is idle
}
```

The worker uses `worker_clock_origin: Instant` set at startup, and `source_recv_ns = worker_clock_origin.elapsed().as_nanos() as i64` per tick. This is the engine's monotonic clock for dedup/cooldown deltas — wall-clock (`now_unix_ns`) is reserved for log timestamps and L2 auth headers.

### Python Control Plane (`src/polybot2/`)

| Module | Role |
|--------|------|
| `_cli/` | argparse CLI: data, provider, link, hotpath subcommands |
| `data/` | Market sync from Polymarket CLOB API, SQLite storage |
| `linking/` | Deterministic provider↔Polymarket matching, review workflows |
| `execution/` | Config container for Rust dispatch (no order methods — Rust handles all dispatch) |
| `hotpath/` | Plan compiler, native service adapter, replay |
| `hotpath/mlb/` | `MlbOrderPolicy` dataclass (evaluation logic moved to Rust `eval.rs`) |
| `sports/` | Provider abstractions: Kalstrop (WS), BoltOdds (REST) |
| `config/` | `live_trading.py` (execution policy), `mappings.py` (team aliases, league rules) |

### FFI Boundary (Python → Rust)

Python `NativeHotPathService` calls Rust `NativeHotPathRuntime` via PyO3:
- `start(config_json, compiled_plan_json, exec_config_json)` — all configs use `deny_unknown_fields`
- `stop()`, `set_subscriptions(ids)`, `prewarm_presign(templates_json)`, `health_snapshot()`
- Compiled plan serialized via `serialize_compiled_plan()` in `native_engine.py`. The JSON shape is unchanged from the pre-integer-ID era; Rust builds its indexed structures (and the registry) from the same JSON.
- `health_snapshot()` returns `{running, subscriptions, reconnects, last_error, submitter: {present, running, last_error, posted_ok, posted_err}}`. The nested `submitter` object is populated in HTTP mode and absent (`present: false`) in paper mode.

### Order Types

The dispatch layer supports three order types, configured via `time_in_force` in `HOTPATH_EXECUTION_POLICY` (parsed into `OrderTimeInForce` at startup):
- **FAK/FOK** (market orders) → `client.market_order().amount(SdkAmount::usdc(...))` — uses `amount_usdc`
- **GTC** (limit orders) → `client.limit_order().size(...).price(...)` — uses `size_shares`

GTD is not supported (presigned GTD orders cannot carry runtime-computed expiration). Limit prices must be `.normalize()`d to strip trailing zeros, or the SDK rejects them for exceeding the token's tick-size precision.

## Critical Invariants

1. **Hotpath ordering:** Match update → decision → channel send → tick log. The WS thread does no HTTP work and acquires no log locks before the channel send on the success path.

2. **Fail-closed:** Presign pool miss → error logged on the WS thread (no fallback to unsigned submit). Startup warmup failure → process won't trade. Empty `order_id` with `success: true` from the CLOB → treated as `Err` (not a phantom fill).

3. **Spread evaluation:** `(margin as f64) + spread_line > 0` where the compiler negates the line for the complement ("No"/"Away") side of spread markets. `margin = home - away` for HOME, `-margin` for AWAY.

4. **Totals over crossing:** For a score change from `prev` to `now`, iterate `over_lines` and fire any with `half_int in [prev, now)`. Direct array indexing — no string keys, no HashMap.

5. **One-shot intents:** Each token can fire at most once per session, enforced by the presign pool (depth=1, `Option::take()`). Once `pop_for_target` takes the signed order, the pool slot is `None` and any repeat intent fails closed at dispatch time. There is no engine-side `attempted` bitset — the pool is the sole gate.

6. **Independent evaluation:** `evaluate_totals`, `evaluate_nrfi`, `evaluate_final` all run on every tick (not an if/else chain). Each takes `&mut self` only to update its own resolution flags (`Vec<bool>` writes); no string allocations.

7. **NRFI first-inning gate:** `nrfi_first_inning_observed: Vec<bool>` indexed by `GameIdx`. Late subscriptions (inning > 1) are permanently skipped. Ticks without inning data defer evaluation. Extra innings: if `freeText` contains "extra", `inning_number` is set to `None`. Kalstrop break naming: `"Break top 1 bottom 1"` = mid-inning break (bottom of 1st not yet played, first inning NOT over); `"Break top 2 bottom 1"` = first inning fully done (NRFI NO can fire here if total=0).

8. **Zero-copy parsing:** The live WS path deserializes directly into borrowed `KalstropFrame<'a>` structs. Only `universal_id` and `period` are heap-allocated per tick. All other string fields are `&'static str` (fixed enum values).

9. **Submitter thread isolation:** The WS thread never owns or references the SDK client. Submitter ownership is established at startup; the channel is the only communication path. The `Arc<TargetRegistry>` is the only shared read-only state.

10. **Frame-preserving batch:** `process_decoded_frame_sync` builds exactly one `SubmitWork::Batch` per material WS frame. All intents from the same frame ride together in one channel send. The submitter dispatcher spawns each batch as a concurrent task (up to 3 in-flight) — no cross-frame coalescing, no head-of-line blocking.

11. **Monotonic clock:** Engine dedup/cooldown deltas use a `worker_clock_origin: Instant` set at WS-worker startup, sourced via `Instant::elapsed().as_nanos()`. Wall-clock (`now_unix_ns`) is used only for log timestamps and L2 auth headers — never for engine math.

12. **Pool sharing semantics:** The presign pool is indexed by `TokenIdx`, not `TargetIdx`. Two targets pointing to the same token share one queue entry. This is enforced structurally — `tokens` is deduplicated at plan load.

## CLI Commands

```bash
polybot2 data sync --db path.sqlite
polybot2 provider sync --provider kalstrop --league mlb --db path.sqlite
polybot2 link build --provider kalstrop --league mlb --db path.sqlite
polybot2 link review session --provider kalstrop --league mlb --link-run-id N --db path.sqlite
polybot2 hotpath run --provider kalstrop --league mlb --link-run-id N --execution-mode paper
polybot2 hotpath run --provider kalstrop --league mlb --link-run-id N
polybot2 hotpath live --league mlb --link-run-id N --execution-mode live --refresh-interval 300
polybot2 hotpath observe --log-file path/to/hotpath_42_*.jsonl   # live terminal scoreboard
polybot2 hotpath observe --run-id 42 --link-run-id N --db path.sqlite  # auto-discover log, resolve team names
polybot2 hotpath replay --provider kalstrop --league mlb --link-run-id N --capture-manifest path.json
polybot2 provider capture --league mlb --today --out ./captures   # raw score frame recording
polybot2 mapping validate
```

The prerequisite pipeline must run in order before the hotpath: data sync → provider sync → link build → link review. The `hotpath live` command automates this cycle with periodic refresh.

### Hotpath Live Orchestrator

`polybot2 hotpath live` runs the hotpath with periodic stop/restart for plan refresh. On each cycle: data sync → provider sync → link build → auto-approve new bindings → compile plan (excluding already-fired strategy keys) → start hotpath → wait `--refresh-interval` → stop → repeat. This allows newly created Polymarket markets (e.g., new totals lines added mid-game) to enter the compiled plan without manual intervention.

Key features:
- **`.env` auto-loading** — reads `.env` from the working directory at startup; env vars propagate to subprocess calls (data sync, provider sync, link build).
- **Dynamic `run_id`** — after each `link build`, resolves the latest `run_id` from the database. New link runs with new game bindings are picked up automatically.
- **Fired strategy key exclusion** — reads all `hotpath_*.jsonl` log files to collect strategy keys from successfully dispatched orders (`ev=order, ok=true`). These are passed to `compile_hotpath_plan(exclude_strategy_keys=...)` so already-traded targets are excluded from the new plan.
- **Auto-approve** — inserts `decision="approve"` for all pending game bindings in the link run scope. No manual review needed during live operation.

## Kalstrop Provider (Score Data)

Kalstrop has two separate APIs:

**V1 (Odds API)** — `sportsapi.kalstropservice.com/odds_v1/v1`. HMAC-signed GraphQL WS + REST. Betradar-backed. This is what the Rust hotpath connects to for live score streaming. Supports baseball and soccer.
- **WS:** `sportsMatchStateUpdatedV2` subscription by `fixtureIds` (UUIDs like `d3f41158-...`). Prematch games produce no WS frames — frames start at kickoff/first pitch.
- **REST catalog:** `/sports/{sport}/live` and `/sports/{sport}/popular` (live + prematch). The `upcoming` endpoint was broken (returns empty); `popular` is the current replacement. Config default is `catalog_types = ("live", "popular")`.
- **Breaking change (April 2026):** `eventState` field removed from WS `matchSummary` entirely (not renamed — absent). Game completion is now detected from `matchStatusDisplay[0].freeText` containing `"Ended"`. The Rust parser (`parse.rs`) and Python provider (`kalstrop.py`) use `is_completed_free_text()` / `_score_completed_from_free_text()` which check for "Ended", "Final", "Game Over", "Finished", "FT".

**V2 (Live Stats API)** — `stats.kalstropservice.com/api/v2`. No auth required. BetGenius-backed Socket.IO. Supports football (soccer), basketball, tennis. **Does not support baseball.**
- **REST:** `/sports` → `/sports/{slug}/competitions` → `/competitions/{cat}/{tourn}/fixtures` → `/fixtures/{event_id}/providers` to resolve BetGenius `fixture_id`.
- **Socket.IO:** `subscribe` with `{fixtureId, activeContent: "court"}` → `genius_update` events with `scoreboardInfo` (live scores, phases, match actions).
- **Provider resolution** can fail with 502 near kickoff; retries needed.

The V1 and V2 APIs use **different ID spaces**. V1 fixture IDs are UUIDs; V2 uses numeric `event_id`s. The V2 provider resolution maps `event_id` → BetGenius `fixture_id` (also numeric). Neither maps to the other directly.

For the Rust hotpath, only V1 is used (WS score streaming). V2 is used for soccer fixture discovery and Socket.IO live stats in the `soccer_full_capture.py` research tool.

Documentation: `research/kalstrop_api_documentation.md` covers the V2 API flow (steps 1-5).

## Environment Variables

Required for live execution: `POLY_EXEC_API_KEY`, `POLY_EXEC_API_SECRET`, `POLY_EXEC_API_PASSPHRASE`, `POLY_EXEC_PRESIGN_PRIVATE_KEY`, `POLY_EXEC_FUNDER`.

Provider credentials: `KALSTROP_CLIENT_ID`, `KALSTROP_SHARED_SECRET_RAW` (or legacy `CLIENT_ID`, `SHARED_SECRET_RAW`).

Database: `POLYBOT2_DB_PATH` (default: `../../data/prediction_markets.db` relative to working dir).

Log directory: `POLYBOT2_LOG_DIR` (default: current working directory). The hotpath writes a JSONL log file (`hotpath_{run_id}_{timestamp}.jsonl`) with tick states (logged by WS thread) and order outcomes (logged by submitter thread).

## Constraints

- Deployment target is Linux EC2 (eu-west-1). Dev is macOS (ARM).
- Python ≥ 3.11, Rust edition 2021, PyO3 0.22 with ABI3.
- `polymarket_client_sdk_v2` 0.5.1 (CLOB V2, migrated April 2026) pins `alloy` at 1.6.3 — do not add a different alloy version or traits will mismatch.
- `smallvec` is a hot-path dependency (`SubmitBatch` payload). Don't replace with `Vec` without measuring — the inline `[T; 4]` capacity covers the common 1–4-intent-per-frame case without heap allocation.
- Prefer deletion over compatibility shims. No backwards-compat wrappers for removed features.
- The field name is `amount_usdc` everywhere (not `notional_usdc` — that was the legacy name, fully removed).
- The old telemetry system (Unix DGRAM socket) was removed. Replaced by a structured JSONL log file (`log_writer.rs`) shared via `Arc<Mutex<>>`. The `polybot2 hotpath observe` command reads the JSONL log file and renders an in-place terminal scoreboard. Team abbreviations use Polymarket codes from `config/mappings.py`.
- SDK config uses `use_server_time(false)` to avoid a `GET /time` round-trip before every order POST. Host clock must be disciplined with chrono/NTP on the deployment target.
- Multi-intent frames batch in `process_decoded_frame_sync` (one Batch per frame). The submitter dispatcher `tokio::spawn`s each batch immediately as a concurrent task (max 3 in-flight via `Semaphore`), routing 1-item batches to `post_order` and larger batches to `post_orders` (chunked at 15). Empty `order_id` with `success: true` from the batch endpoint is treated as failure (`map_post_response`).
- Parsing uses zero-allocation byte-level scanning (`eq_ignore_ascii_case`, byte accumulator for numbers) — no `to_lowercase()`/`to_uppercase()` heap allocations on the tick path.
- Final-game cleanup (`cleanup_completed_game_idx`) is deferred until after intents are selected, not during `evaluate_final`. `final_resolved_games[gi] = true` blocks re-evaluation immediately; cleanup runs in `process_tick` before returning. Cleanup clears only lightweight row data (`rows`, `game_states`, `nrfi_first_inning_observed`); completion tombstones (`totals_final_under_emitted`, `nrfi_resolved_games`, `last_emit_ns`, `last_signature`) are preserved for the session to prevent duplicate emission from repeated final frames.
- Tests use temp-path `LogWriter`s (no actual log inspection in non-live tests). Live tests construct an `OrderSubmitter` directly and call `submit_order_async` or `submit_signed_chunked_async`, bypassing the channel.

## Latency Optimization Roadmap

Target: single-digit microsecond end-to-end on the WS thread (frame available → bytes sent on the channel). Status:

1. **Strategy evaluation — DONE.** Plan compiled into `GameIdx`/`TargetIdx` integer indices and per-game arrays. Evaluation is array indexing (~10ns), zero `format!()`, zero `HashMap` lookups except the single `fixture_id → GameIdx` resolve.

2. **Decoupled submitter — DONE.** WS thread does sync `pop + send`; submitter thread does HTTP. WS-thread cost per intent is ~100ns presign pop + amortized channel send. SDK client lives only on the submitter.

3. **Frame-preserving batch — DONE.** `process_decoded_frame_sync` builds one `SubmitWork::Batch(SmallVec<[(TargetIdx, SdkSignedOrder); 4]>)` per material frame. Multi-intent frames always reach the CLOB as one logical group. Submitter dispatcher spawns each batch as a concurrent task (up to 3 in-flight via Semaphore) — no cross-frame coalescing, no head-of-line blocking.

4. **Index-keyed payload + Arc<TargetRegistry> — DONE.** Pool indexed by `TokenIdx` (`Vec<Option<PreSignedOrderData>>`, depth=1, `Option::take()` for pop). Channel payload is `(TargetIdx, SdkSignedOrder)` — no string allocation on the WS success path. Strings reconstructed via the registry only at log time.

5. **Logging swap — DONE.** Tick logging happens after dispatch in `process_decoded_frame_sync`. Success path holds zero log locks before the channel send.

6. **Monotonic clock — DONE.** Engine dedup/cooldown uses `Instant::elapsed()`-derived nanos from a worker-local origin. Wall-clock is reserved for log timestamps and L2 auth.

7. **Submitter health surface — DONE.** `health_snapshot()` exposes `{present, running, last_error, posted_ok, posted_err}`. Outage blindness fixed.

8. **Empty `order_id` correctness — DONE.** `map_post_response` treats `success: true` with empty `order_id` as `Err`, preventing phantom-fill bookkeeping.

9. **SDK request construction (~5–20µs in the submitter) — investigated, deferred.** The SDK's `create_headers`, `hmac`, `to_message` are private and the inner `reqwest::Client` is private; pre-serialization without forking would require replicating the L2 HMAC scheme externally. The HMAC depends on the current timestamp (the CLOB validates recency), so pre-HMAC is impossible — only the JSON `to_vec` could be moved to presign time. Net win is ~2–5µs of submitter CPU shifted, which doesn't reduce WS-thread latency. See `AUDIT_RESPONSE.md` for the full reasoning. Re-evaluate if the submitter ever becomes the bottleneck or the SDK exposes a lower-level API.

10. **Per-order deadline + drop-stale — resolved via concurrent submitter.** Head-of-line blocking was the root cause of stale orders; with 3 concurrent submit slots, frame 2's HTTP call starts immediately without waiting for frame 1's round-trip. The channel is still unbounded but the stale-order window is now one RTT per slot (~100-200ms × 3) rather than serial (~N × 200ms). A deadline mechanism is no longer the priority fix.

11. **Deferred final-game cleanup — DONE.** `cleanup_completed_game_idx` now preserves all tombstones (`totals_final_under_emitted`, `nrfi_resolved_games`, `last_emit_ns`, `last_signature`) and only clears lightweight row data. The `all_target_indices()` Vec allocation is removed entirely.

12. **Benchmark harness — pending (Phase 4).** No `criterion` or `iai-callgrind` benchmarks exist. Required to validate Phase 2 didn't regress and to inform any future revisit of pre-serialization.

13. **CLOB V2 SDK migration — DONE.** `polymarket-client-sdk 0.4.4` → `polymarket_client_sdk_v2 0.5.1`. Same alloy 1.6.3 pin, same builder/sign/post API surface. V2 order format (removes `nonce`/`taker`/`expiration`/`feeRateBps`; adds `timestamp`/`metadata`/`builder`) is handled by the SDK internally. Collateral changed from USDC.e to pUSD (transparent to order construction). Live execution tests validated against V2 CLOB post-cutover.

14. **`send_batch` zero-allocation success path — DONE.** `DispatchHandle::send_batch` sends first, recovers the batch from `SendError` on failure for diagnostics. No `Vec<TargetIdx>` allocation before the channel send.

15. **Presign warmup parallelized — DONE.** Warmup Tokio runtime uses `new_multi_thread()` so `tokio::spawn`ed ECDSA tasks run on real OS threads. ~135 tokens warm up in ~1-2s instead of ~5s (single-threaded cooperative scheduling).

See `LATENCY_AUDIT.md` for the source-level audit and `AUDIT_RESPONSE.md` for per-finding rationale and phasing.
