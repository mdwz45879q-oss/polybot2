# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

polybot2 is a sports-trading bot for Polymarket. Hybrid architecture: Python control plane for CLI, data sync, linking, and orchestration; Rust native hotpath via PyO3/maturin for low-latency score ingest → decision → order dispatch. Supports MLB (baseball) and soccer (EPL, Bundesliga, UCL). Three score data providers: Kalstrop V1 (Sportradar, WS, baseball + lower-tier soccer), Kalstrop V2 (BetGenius, Socket.IO, top-tier soccer), BoltOdds (WS, broad coverage). One provider per league — configured in `LEAGUES[league]["provider"]`. Deployment: Linux EC2 (eu-west-1).

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
polybot2 market sync         → SQLite (pm_events, pm_markets, pm_market_tokens)
polybot2 provider sync       → SQLite (provider_games) — syncs all configured providers
polybot2 link build          → SQLite (link_runs, link_*_bindings) — one run_id across all leagues
polybot2 link review         → SQLite (link_review_decisions) — opt-out: reject bad matches
polybot2 hotpath live        → Compiled plan → Rust runtime → WS → engine → DispatchHandle → channel → submitter → CLOB
                               Start once → incremental Gamma API fetch → hot-patch new targets into running engine
```

### Rust Native Hotpath (`native/polybot2_native/src/`)

The hot path is split across two threads. The **WS thread** parses frames, evaluates the plan, pops presigned orders for all intents in the frame, and hands them to the submitter as one `SubmitWork::Batch` — no HTTP, no string allocation on the success path. The **submitter thread** owns the SDK client, spawns each batch as a concurrent task (up to 3 in-flight via Semaphore), posts to the CLOB, and logs outcomes.

| Module | Role |
|--------|------|
| `kalstrop_types.rs` | Zero-copy serde structs for Kalstrop WS frames (`KalstropFrame<'a>`, etc.) |
| `baseball/` | Sport-specific: `engine.rs` (NativeMlbEngine, process_tick_live, merge_plan), `eval.rs` (totals, NRFI, walkoff, moneyline, spreads), `parse.rs` (inning parsing), `frame_pipeline.rs` (zero-alloc live path), `types.rs` (GameState, GameTargets, etc.) |
| `soccer/` | Sport-specific: `engine.rs` (NativeSoccerEngine), `eval.rs` (totals, three-way moneyline, BTTS, spreads, corners, halftime result, exact score), `parse.rs` (half parsing), `frame_pipeline.rs`, `types.rs` |
| `boltodds_types.rs` | Byte-level extractor for BoltOdds frames (`fast_extract_boltodds`). Extracts `game_label`, goals, corners, match period from raw JSON without serde. |
| `boltodds_frame_pipeline.rs` | BoltOdds soccer frame pipeline: extract → dedup → eval → dispatch. Uses integer-based dedup (goals + corners + period). |
| `ws_boltodds.rs` | BoltOdds WS worker: plain WS connection (`?key=TOKEN`), subscribe by game labels, frame drain loop. Simpler protocol than V1 (no GraphQL). |
| `fast_extract.rs` | Byte-level extractor for Kalstrop V1 frames (`fast_extract_v1`). Extracts fixtureId, homeScore, awayScore, freeText without full serde parse. |
| `dispatch/flow.rs` | `DispatchHandle::pop_for_target(TargetIdx)` (sync, returns signed order or err) and `send_batch(SubmitBatch, &log)` (sync, sends one Batch on the channel). |
| `dispatch/presign_pool.rs` | Presign pool indexed by `TokenIdx` (`Vec<Option<Box<SdkSignedOrder>>>`). Depth is always 1 (one-shot intents). Boxed entries: `Option::take` moves 8 bytes (pointer) not ~450 bytes (struct). Pool fits in L1 (~1.1KB for 135 tokens). `warm_presign_startup_into` signs one order per token at startup. |
| `dispatch/sdk_exec.rs` | `OrderSubmitter`: holds `Option<SdkRuntime>`, signs and posts orders. Methods: `ensure_sdk_runtime_async`, `post_signed_order_async`, `submit_signed_chunked_async`, `build_signed_order_async`, `submit_order_async`. Also `map_post_response` helper that treats empty `order_id` with `success: true` as failure. |
| `dispatch/submitter.rs` | `run_submitter_async`: dispatcher loop that receives `SubmitWork::Batch` and `tokio::spawn`s each batch as a concurrent submit task, bounded by `Semaphore(3)`. Each task calls SDK `post_order`/`post_orders` and logs outcomes. No cross-frame coalescing — each frame gets its own HTTP call immediately. Writes `SubmitterHealth` on init/success/error/exit. |
| `dispatch/types.rs` | `DispatchHandle`, `OrderSubmitter`, `SubmitWork`, `SubmitBatch`, `OrderRequestData`. Presign pool stores `Box<SdkSignedOrder>` (8-byte pointer, not inline ~450-byte struct). |
| `ws.rs` | Kalstrop V1 live worker: GraphQL WS connect, subscription management, frame drain loop. Uses `worker_clock_origin: Instant` for monotonic timestamps. Dispatches to sport-specific frame pipeline via `SportEngine` enum (`Baseball`/`Soccer` variants). Drains `patch_rx` at quiescent points for hot-patch application. |
| `runtime.rs` | PyO3 `NativeHotPathRuntime`: builds both halves at startup with shared `Arc<TargetRegistry>`, runs presign warmup, spawns submitter and WS threads, lifecycle (`start`/`stop`/`patch_plan`). Provider-based worker dispatch: spawns `ws.rs` (Kalstrop V1) or `ws_boltodds.rs` (BoltOdds) based on `provider` in config. Sport engine selected from plan league (Baseball/Soccer). `health_snapshot()` exposes WS + submitter health. |
| `lib.rs` | Shared types (`GameIdx`, `TargetIdx`, `TokenIdx`, `OverLine`, `SpreadSide`, `Intent`, `RawIntent`), `SportEngine` enum, `PatchPayload`, `NativeHotPathRuntime`, config structs. Baseball-specific types live in `baseball/types.rs`, soccer in `soccer/types.rs`. |
| `log_writer.rs` | Structured JSONL log. Wrapped in `Arc<Mutex<LogWriter>>` and shared by WS thread (logs ticks) and submitter thread (logs order outcomes). |

### Hot Path Pipeline

```
WS thread (per frame, zero-alloc live path):
  serde_json::from_str<KalstropFrame<'a>>            (zero-copy)
  → extract borrowed fields from KalstropUpdate<'a>  (no Tick construction, no to_owned)
  → engine.check_duplicate / process_tick_live(GameIdx, ...)  (single FxHashMap lookup, eval into SmallVec)
                                                      returns LiveTickResult { game_idx, intents: SmallVec<[Intent; 4]> }
  → process_decoded_frame_sync builds one SubmitBatch per frame:
        for each Intent:
            DispatchHandle::pop_for_target(target_idx)   (Vec index + Option::take, ~100ns)
            batch.push((target_idx, signed_order))
        DispatchHandle::send_batch(batch, &log)          (one tx.send for the whole frame)
  → log tick using engine.game_ids[game_idx]         (borrowed, no clone)
  → return to ws.next()                              (0 heap allocs, <1µs per material frame)

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

`Arc<TargetRegistry>` is the read-only mapping `TargetIdx → TargetSlot { token_idx, strategy_key: Arc<str> }` and `TokenIdx → TokenSlot { token_id: Arc<str> }`. Built once in `engine.load_plan` and cloned into both `DispatchHandle` (WS thread) and `OrderSubmitter` (submitter thread). `Arc<str>` means registry cloning is ref-count bumps, not string copies.

The registry exists so the channel payload can be `(TargetIdx, SdkSignedOrder)` — strings (`strategy_key`, `token_id`) are reconstructed from the registry only at log time, on the submitter, after the order has been handed off. The WS thread allocates no strings on the success path.

The `tokens` vector is deduplicated by `token_id` at load time. A `TargetIdx` always references a `TokenIdx`; multiple targets pointing to the same token (rare in practice) share the single pool entry. This preserves the "one signed order per unique token" invariant — there is exactly one `Option` slot per `TokenIdx`, regardless of how many targets reference that token.

### Integer-Indexed Evaluator

At plan load, `engine.load_plan` interns each `provider_game_id` → `GameIdx(u16)` and each target → `TargetIdx(u16)`. Per-game compact arrays drive evaluation:

- `over_lines: Vec<OverLine { half_int: u16, target_idx }>` — sorted by `half_int`. On a score change from `prev` to `now`, iterate and match `half_int in [prev, now)`. A line of 5.5 is stored as `half_int = 5`.
- `under_lines: Vec<OverLine>` — same shape, fired at game completion when `half_int >= total`.
- `nrfi_yes`, `nrfi_no`: `Option<TargetIdx>` — direct slot access.
- `moneyline_home`, `moneyline_away`: `Option<TargetIdx>` — direct slot access.
- `spreads: Vec<(SpreadSide, f64, TargetIdx)>` — small, iterated at game end.

Per-game state (rows, GameState, resolution flags) is stored in `Vec<...>` indexed by `GameIdx`. One-shot gating is enforced by the presign pool (depth=1, `Option::take()`), not by the engine — there is no `attempted` bitset. No cooldown or debounce logic exists — the presign pool is the sole gate against duplicate intents.

The only string hash on the live path is `game_id_to_idx.get(fixture_id)` — one `FxHashMap::get` per tick, done once in `check_duplicate` (baseball) or `is_duplicate_boltodds` (soccer) and the resulting `GameIdx` passed through. After filtering, the engine emits `Intent { target_idx: TargetIdx }` (`Copy`, no strings). String resolution happens only at the FFI boundary and on the submitter (via the shared `Arc<TargetRegistry>` at log time). StateRow uses `InlineStr<N>` (stack-allocated, no heap) for dedup fields.

The Python compiler (`compiler.py`) still produces strategy keys (`"gid:TOTAL:OVER:5.5"`, etc.) and embeds them in the compiled plan JSON. Rust stores them in `TargetSlot.strategy_key` for log output but never parses or hashes them.

### Decoupled Submitter

Two structs back the dispatch path:

- **`DispatchHandle`** (WS thread): `cfg`, `registry: Arc<TargetRegistry>`, `presign_template_catalog: HashMap<String, OrderRequestData>`, `presign_templates: Vec<Option<OrderRequestData>>` indexed by `TokenIdx`, `presign_pool: Vec<Option<Box<SdkSignedOrder>>>` indexed by `TokenIdx` (depth=1, boxed: `Option::take()` moves 8 bytes), `submit_tx: Option<flume::Sender<SubmitWork>>`. All methods are synchronous.
- **`OrderSubmitter`** (submitter thread): `cfg`, `registry: Arc<TargetRegistry>`, `sdk_runtime: Option<...>`, `cached_signer: Option<...>`, `submit_rx`, `log: Arc<Mutex<LogWriter>>`, `health: Arc<Mutex<SubmitterHealth>>`. Async; owns the SDK client.

Channel: `flume::unbounded<SubmitWork>` where `SubmitWork::Batch(SmallVec<[(TargetIdx, SdkSignedOrder); 4]>)`. `flume::Sender::send` is sync and never blocks. The WS thread sends exactly one Batch per material frame in `process_decoded_frame_sync`. The submitter dispatcher `tokio::spawn`s each batch immediately as a concurrent task, bounded by `Semaphore(3)`. No cross-frame coalescing — frame 2's HTTP call starts without waiting for frame 1's round-trip. Each spawned task handles its own `post_order`/`post_orders` call and releases the semaphore permit when done.

`SubmitWork::Stop` is the shutdown signal. `run_submitter_async` drains the in-flight batch before exiting on Stop. Channel-closed (sender dropped) also exits cleanly.

In **noop mode** no submitter thread is spawned; `DispatchHandle::submit_tx` stays `None`; `process_decoded_frame_sync` short-circuits to log `"noop"` per intent inline, never touching the pool or channel.

`LogWriter` is wrapped in `Arc<Mutex<>>` and shared between the WS thread (logs ticks via `log.log_tick`, `log_ws_connect`, `log_ws_disconnect`) and the submitter thread (logs order outcomes via `log_order_ok`/`log_order_err`). Lock contention is negligible — writes are buffered, hold time is sub-µs. **The success path on the WS thread holds zero log locks before the channel send** — tick logging happens after dispatch.

### Presign Pool

Pre-signs one order per unique token at startup so the WS thread can pop in ~100ns instead of ECDSA-signing in ~10–50ms. Pool depth is always 1 per token (one-shot intents fire each token at most once). Pool ownership lives on `DispatchHandle` (WS thread) as `Vec<Option<Box<SdkSignedOrder>>>` indexed by `TokenIdx` (`Option::take()` for pop). The SDK client used to sign warmup orders lives on `OrderSubmitter` (submitter thread). At startup:

1. `OrderSubmitter::ensure_sdk_runtime_async` initializes the SDK client.
2. `warm_presign_startup_into(&cfg, &client, &signer, &templates_slice, &mut pool_slice)` signs one order per token in parallel (`tokio::spawn` per token) and writes results into the handle's pool by `TokenIdx`.
3. `DispatchHandle::install_submit_tx(submit_tx)` wires the channel.
4. Submitter thread is spawned with `OrderSubmitter` (and channel rx); WS thread is spawned with `DispatchHandle` (and channel tx).

Presign pool miss → fail-closed error logged on the WS thread; no fallback to inline sign-and-submit. Startup warmup failure → process won't trade.

For hot-patched targets (incremental refresh), new presign orders are signed by `patch_plan()` on the Python thread (using cached SDK client/signer clones, GIL released) and delivered pre-signed inside `PatchPayload`. The WS thread installs them into grown pool slots via `DispatchHandle::extend_for_patch()` — no signing on the WS thread.

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

The worker uses `worker_clock_origin: Instant` set at startup, and `source_recv_ns = worker_clock_origin.elapsed().as_nanos() as i64` per tick. This is the engine's monotonic clock — wall-clock (`now_unix_ns`) is reserved for log timestamps and L2 auth headers.

### Python Control Plane (`src/polybot2/`)

| Module | Role |
|--------|------|
| `_cli/` | argparse CLI: market, provider, link, hotpath subcommands |
| `data/` | Market sync from Polymarket CLOB API, SQLite storage |
| `linking/` | Deterministic provider↔Polymarket matching, review workflows |
| `execution/` | Config container for Rust dispatch (no order methods — Rust handles all dispatch) |
| `hotpath/` | Plan compiler, native service adapter, incremental market refresh |
| `hotpath/incremental.py` | `discover_new_markets()` — targeted Gamma API fetch for known event IDs, diff against current plan, insert new market targets, return delta for hot-patch |
| `hotpath/order_policy.py` | `OrderPolicy` dataclass — sport-generic execution profile (amount, size, price, time-in-force) |
| `sports/` | Provider catalog adapters: `KalstropV1Provider` (V1 REST catalog), `KalstropV2Provider` (V2 REST catalog), `BoltOddsProvider` (REST catalog). No Python-side streaming — all WS streaming is handled by Rust or standalone capture scripts. |
| `sports/kalstrop_v2.py` | Kalstrop V2 catalog discovery — REST hierarchy: sports → competitions → tournaments → fixtures |
| `config/` | `live_trading.py` (execution policy), `mappings.py` (league registry, provider aliases, league disambiguation via `PROVIDER_LEAGUE_COUNTRY`), `baseball_mappings.py` / `soccer_mappings.py` (team aliases per league) |
| `scripts/` | Standalone capture scripts for raw frame recording: `capture_kalstrop_v1.py`, `capture_kalstrop_v2.py`, `capture_boltodds.py`, `capture_multi.py` (multi-provider comparison). Not part of the `polybot2` package — run directly. |

### FFI Boundary (Python → Rust)

Python `NativeHotPathService` calls Rust `NativeHotPathRuntime` via PyO3:
- `start(config_json, compiled_plan_json, exec_config_json)` — all configs use `deny_unknown_fields`
- `stop()`, `set_subscriptions(ids)`, `prewarm_presign(templates_json)`, `health_snapshot()`
- `patch_plan(plan_json, templates_json)` — hot-patch: signs new orders (GIL released), sends `PatchPayload` to WS thread via dedicated `patch_tx` channel. WS thread calls `engine.merge_plan()` (append-only), extends dispatch pool, rebuilds `Arc<TargetRegistry>`, propagates to submitter via `SubmitWork::UpdateRegistry`.
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

8. **Zero-copy parsing:** The live WS path deserializes directly into borrowed `KalstropFrame<'a>` structs and extracts fields without constructing a `Tick` or allocating any strings. The `fixture_id` is looked up as `&str` directly from the serde struct.

9. **Submitter thread isolation:** The WS thread never owns or references the SDK client. Submitter ownership is established at startup; the channel is the only communication path. The `Arc<TargetRegistry>` is the only shared read-only state.

10. **Frame-preserving batch:** `process_decoded_frame_sync` builds exactly one `SubmitWork::Batch` per material WS frame. All intents from the same frame ride together in one channel send. The submitter dispatcher spawns each batch as a concurrent task (up to 3 in-flight) — no cross-frame coalescing, no head-of-line blocking.

11. **Monotonic clock:** Engine timestamps use `worker_clock_origin: Instant` set at WS-worker startup, sourced via `Instant::elapsed().as_nanos()`. Wall-clock (`now_unix_ns`) is used only for log timestamps and L2 auth headers — never for engine math.

12. **Pool sharing semantics:** The presign pool is indexed by `TokenIdx`, not `TargetIdx`. Two targets pointing to the same token share one queue entry. This is enforced structurally — `tokens` is deduplicated at plan load.

## CLI Commands

```bash
polybot2 market sync
polybot2 market sync --all                          # include resolved/closed markets
polybot2 provider sync                              # syncs all providers (kalstrop_v1, kalstrop_v2, boltodds)
polybot2 provider sync --provider kalstrop_v1       # sync a single provider
polybot2 link build                                 # build links for all live leagues (one run_id)
polybot2 link build --league-scope all              # include non-live leagues too
polybot2 link review --run-id N                     # interactive review (opt-out: reject bad matches)
polybot2 hotpath live --league mlb --execution-mode live  # auto-discovers latest run_id
polybot2 hotpath live --league epl --execution-mode live  # separate process per league
polybot2 hotpath observe --log-file path/to/hotpath_42_*.jsonl   # live terminal scoreboard
polybot2 hotpath observe --run-id 42 --link-run-id N --db path.sqlite  # auto-discover log, resolve team names
```

Raw score frame capture is handled by standalone scripts in `scripts/` (not part of the CLI):
```bash
python scripts/capture_multi.py --games-file games.json --out ./captures/2026_05_04 --duration 14400
python scripts/capture_kalstrop_v1.py --fixture-id <UUID> --out ./captures/v1 --duration 7200
```

The prerequisite pipeline: market sync → provider sync → link build → hotpath live. One hotpath process per league.

### Hotpath Live Orchestrator

`polybot2 hotpath live` starts the hotpath for a single league. Provider is derived from `config/mappings.py` (`LEAGUES[league]["provider"]`). Run_id is auto-discovered (latest for the league) unless `--link-run-id` is passed.

Daily workflow:
```bash
polybot2 market sync
polybot2 provider sync
polybot2 link build
polybot2 hotpath live --league mlb --execution-mode live
polybot2 hotpath live --league epl --execution-mode live  # separate process
```

Review is opt-out: all linked games enter the plan unless explicitly rejected via `link review`.

Refresh loop (every `refresh_interval_seconds` from `config/live_trading.py`, default 1800s):
1. `discover_new_markets_sync()` — fetches markets from the Gamma API for known event IDs only (5–20 targeted HTTP requests, ~2s)
2. Diffs against current plan by strategy_key — if no new targets, does nothing
3. `hotpath.apply_incremental_refresh()` — signs new presign orders (GIL released), sends `PatchPayload` to WS thread
4. WS thread applies `engine.merge_plan()` at a quiescent point — extends per-game arrays, rebuilds `Arc<TargetRegistry>`, propagates to submitter

Key features:
- **Single run_id for the session** — no link rebuild, no run_id incrementing, observer keeps working
- **No blind windows** — hotpath processes frames continuously during refresh
- **`.env` auto-loading** — reads `.env` from the working directory at startup
- **No fired-key tracking needed** — the presign pool's one-shot gate (`Option::take`) prevents any target from firing twice within a session, and `merge_plan` deduplicates by strategy_key via `HashSet`.

## Kalstrop Providers (Score Data)

V1 and V2 are treated as **separate providers** with distinct names (`kalstrop_v1`, `kalstrop_v2`), different ID spaces, and different streaming protocols. A game from V1 and the same game from V2 have different `provider_game_id` values.

**V1 (`kalstrop_v1`)** — `sportsapi.kalstropservice.com/odds_v1/v1`. HMAC-signed GraphQL WS + REST. Sportradar-backed. This is what the Rust hotpath connects to for live score streaming. Covers baseball and lower-tier soccer.
- **WS:** `sportsMatchStateUpdatedV2` subscription by `fixtureIds` (UUIDs like `d3f41158-...`). Prematch games produce no WS frames — frames start at kickoff/first pitch.
- **REST catalog:** `/sports/{sport}/live`, `/sports/{sport}/upcoming`, and `/sports/{sport}/popular`. The `popular` feed provides significant additional coverage for soccer (~60 extra fixtures). Per-feed `first` limits: `live=10`, `upcoming=30`, `popular=10`.
- **Breaking change (April 2026):** `eventState` field removed from WS `matchSummary` entirely (not renamed — absent). Game completion is now detected from `matchStatusDisplay[0].freeText` containing `"Ended"`. The Rust parser (`baseball/parse.rs`) uses `is_completed_free_text()`.
- **Python provider is catalog-only.** `kalstrop_v1.py` contains only `load_game_catalog()` and catalog helpers. All WS streaming code has been removed from the Python side — the Rust hotpath handles live score streaming, and standalone scripts in `scripts/` handle raw frame capture.

**V2 (`kalstrop_v2`)** — `stats.kalstropservice.com/api/v2`. No auth required. BetGenius-backed. Covers top-tier soccer (EPL, La Liga, Bundesliga, UCL). **Does not support baseball.** Currently catalog-only — live Socket.IO streaming not yet implemented.
- **REST catalog:** `/sports` → `/sports/{slug}/competitions` → `/sports/{sport}/competitions/{category}/{tournament}/fixtures`. Dynamically discovers all tournaments with matches.
- **Fixture IDs:** Numeric `event_id`s (e.g., `7490587`). Different ID space from V1 UUIDs.
- **Socket.IO (future):** `subscribe` with `{fixtureId, activeContent: "court"}` → `genius_update` events with `scoreboardInfo`.
- **Team names are shortened** compared to V1/Polymarket: `"Man Utd"` vs `"Manchester United FC"`, `"Wolves"` vs `"Wolverhampton Wanderers FC"`. Separate provider aliases needed in `config/soccer_mappings.py`.
- **Empty catalog protection:** If V2 API fails, `sync_provider_games` returns an error instead of wiping the existing snapshot.
- **V2 event_id instability:** The prematch `event_id` from the catalog cannot be resolved to a BetGenius `fixture_id` until the game goes live. The `/fixtures/{event_id}/providers` endpoint returns data from a different ID space for prematch games. Resolution must happen at runtime, not at sync time.
- **Available for:** `provider sync`, `link build`, `link review`. Not yet used by the Rust hotpath (requires Socket.IO runtime + runtime resolution of fixture IDs).

**BoltOdds** — `spro.agency/api`. API key via query param. Covers MLB, EPL, and other leagues. Currently used as the EPL provider.
- **REST catalog:** `GET /get_games?key=TOKEN` — returns all games with `universal_id`, `game`, `when` (ET timestamps), `orig_teams`, `sport`.
- **WS streaming:** `wss://spro.agency/api?key=TOKEN` — delivers `initial_state`, `game_update`, `line_update` messages.
- **Team names are abbreviated** (e.g., `"ATL Braves"` for baseball, `"Chelsea"` for soccer). Provider aliases needed in mappings.
- **Duplicate entries:** BoltOdds sometimes publishes two entries for the same game with different `universal_id`s and slightly different start times. Kickoff tolerance (`LEAGUE_MATCH_RULES`) must be calibrated to avoid linking the wrong entry.

Documentation: `research/kalstrop_api_documentation.md` covers the V2 API flow (steps 1-5). V1 docs: `docs/kalstrop_odds_v1.md`.

## Environment Variables

Required for live execution: `POLY_EXEC_API_KEY`, `POLY_EXEC_API_SECRET`, `POLY_EXEC_API_PASSPHRASE`, `POLY_EXEC_PRESIGN_PRIVATE_KEY`, `POLY_EXEC_FUNDER`.

Provider credentials: `KALSTROP_CLIENT_ID`, `KALSTROP_SHARED_SECRET_RAW` (or legacy `CLIENT_ID`, `SHARED_SECRET_RAW`) for V1. `BOLTODDS_API_KEY` for BoltOdds. V2 requires no auth.

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
- Final-game cleanup (`cleanup_completed_game_idx`) is deferred until after intents are selected, not during `evaluate_final`. `final_resolved_games[gi] = true` blocks re-evaluation immediately; cleanup runs in `process_tick` before returning. Cleanup clears only lightweight row data (`rows`, `game_states`, `nrfi_first_inning_observed`); completion tombstones (`totals_final_under_emitted`, `nrfi_resolved_games`) are preserved for the session to prevent duplicate emission from repeated final frames.
- Tests use temp-path `LogWriter`s (no actual log inspection in non-live tests). Live tests construct an `OrderSubmitter` directly and call `submit_order_async` or `submit_signed_chunked_async`, bypassing the channel.

## Latency Optimization Roadmap

Target: single-digit microsecond end-to-end on the WS thread (frame available → bytes sent on the channel). Status:

1. **Strategy evaluation — DONE.** Plan compiled into `GameIdx`/`TargetIdx` integer indices and per-game arrays. Evaluation is array indexing (~10ns), zero `format!()`, zero `HashMap` lookups except the single `fixture_id → GameIdx` resolve.

2. **Decoupled submitter — DONE.** WS thread does sync `pop + send`; submitter thread does HTTP. WS-thread cost per intent is ~100ns presign pop + amortized channel send. SDK client lives only on the submitter.

3. **Frame-preserving batch — DONE.** `process_decoded_frame_sync` builds one `SubmitWork::Batch(SmallVec<[(TargetIdx, SdkSignedOrder); 4]>)` per material frame. Multi-intent frames always reach the CLOB as one logical group. Submitter dispatcher spawns each batch as a concurrent task (up to 3 in-flight via Semaphore) — no cross-frame coalescing, no head-of-line blocking.

4. **Index-keyed payload + Arc<TargetRegistry> — DONE.** Pool indexed by `TokenIdx` (`Vec<Option<Box<SdkSignedOrder>>>`, depth=1, boxed: `Option::take()` moves 8 bytes). Channel payload is `(TargetIdx, Box<SdkSignedOrder>)` — no string allocation on the WS success path. Strings reconstructed via the registry only at log time.

5. **Logging swap — DONE.** Tick logging happens after dispatch in `process_decoded_frame_sync`. Success path holds zero log locks before the channel send.

6. **Monotonic clock — DONE.** Engine timestamps use `Instant::elapsed()`-derived nanos from a worker-local origin. Wall-clock is reserved for log timestamps and L2 auth.

7. **Submitter health surface — DONE.** `health_snapshot()` exposes `{present, running, last_error, posted_ok, posted_err}`. Outage blindness fixed.

8. **Empty `order_id` correctness — DONE.** `map_post_response` treats `success: true` with empty `order_id` as `Err`, preventing phantom-fill bookkeeping.

9. **SDK request construction (~5–20µs in the submitter) — investigated, deferred.** The SDK's `create_headers`, `hmac`, `to_message` are private and the inner `reqwest::Client` is private; pre-serialization without forking would require replicating the L2 HMAC scheme externally. The HMAC depends on the current timestamp (the CLOB validates recency), so pre-HMAC is impossible — only the JSON `to_vec` could be moved to presign time. Net win is ~2–5µs of submitter CPU shifted, which doesn't reduce WS-thread latency. See `AUDIT_RESPONSE.md` for the full reasoning. Re-evaluate if the submitter ever becomes the bottleneck or the SDK exposes a lower-level API.

10. **Per-order deadline + drop-stale — resolved via concurrent submitter.** Head-of-line blocking was the root cause of stale orders; with 3 concurrent submit slots, frame 2's HTTP call starts immediately without waiting for frame 1's round-trip. The channel is still unbounded but the stale-order window is now one RTT per slot (~100-200ms × 3) rather than serial (~N × 200ms). A deadline mechanism is no longer the priority fix.

11. **Deferred final-game cleanup — DONE.** `cleanup_completed_game_idx` now preserves resolution tombstones (`totals_final_under_emitted`, `nrfi_resolved_games`) and only clears lightweight row data. The `all_target_indices()` Vec allocation is removed entirely.

12. **Benchmark harness — pending (Phase 4).** No `criterion` or `iai-callgrind` benchmarks exist. Required to validate Phase 2 didn't regress and to inform any future revisit of pre-serialization.

13. **CLOB V2 SDK migration — DONE.** `polymarket-client-sdk 0.4.4` → `polymarket_client_sdk_v2 0.5.1`. Same alloy 1.6.3 pin, same builder/sign/post API surface. V2 order format (removes `nonce`/`taker`/`expiration`/`feeRateBps`; adds `timestamp`/`metadata`/`builder`) is handled by the SDK internally. Collateral changed from USDC.e to pUSD (transparent to order construction). Live execution tests validated against V2 CLOB post-cutover.

14. **`send_batch` zero-allocation success path — DONE.** `DispatchHandle::send_batch` sends first, recovers the batch from `SendError` on failure for diagnostics. No `Vec<TargetIdx>` allocation before the channel send.

15. **Presign warmup parallelized — DONE.** Warmup Tokio runtime uses `new_multi_thread()` so `tokio::spawn`ed ECDSA tasks run on real OS threads. ~135 tokens warm up in ~1-2s instead of ~5s (single-threaded cooperative scheduling).

16. **Zero-alloc WS live path — DONE.** `process_tick_live` takes borrowed `fixture_id: &str` from serde, evaluates into `SmallVec<[Intent; 4]>` (stack), returns `LiveTickResult { game_idx, state, intents }` with no owned strings. `frame_pipeline.rs` parses `KalstropFrame<'a>` and calls `process_tick_live` directly — no `Tick`, no `TickResult`, no `Vec` on the success path. Evaluators use `_into(&mut SmallVec)` variants. `LogWriter` uses a reusable `String` buffer. Result: zero heap allocations from frame receipt through `send_batch`.

17. **Hot-patch O(1) dedup + Arc<str> registry — DONE.** `strategy_keys: HashSet<String>` on engine for O(1) merge dedup (was O(N×M) linear scan). `TokenSlot.token_id` and `TargetSlot.strategy_key` changed to `Arc<str>` so registry clone is ref-count bumps. Deferred sort in `merge_plan` (dirty_games set, sort once per game after all targets inserted). Parallel patch presign via `new_multi_thread` + `tokio::spawn`.

18. **Release build profile — DONE.** `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `strip = "symbols"`.

See `latency_audit.md` for the source-level audit and `latency_improvements.md` for the response.

## Python Cleanup Audit

Systematic removal of dead code from the Python control plane. Guiding principle: Python no longer parses scores (Rust does all of it). Python's role is catalog sync, linking, compilation, and orchestration only.

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Sports providers (`sports/`) | Done — deleted all Python streaming code, provider classes are now catalog-only adapters. `boltodds.py` reduced from 1130→390 lines, `kalstrop_v1.py` from 1040→655 lines. Deleted `recorder.py`, dead contract types (`StreamEnvelope`, `ScoreUpdateEvent`, etc.). Removed `sport_key`/`league_key` from `ProviderGameRecord`. |
| 2 | CLI layer (`_cli/`) | Done — consolidated to 6 commands. Removed `--provider` from link/hotpath commands (derived from league config). Removed `--auto-approve` (review is opt-out). Provider sync defaults to all providers. |
| 3 | Linking layer (`linking/`) | Done — `build_links_multi` processes all leagues in one `run_id`. Deleted `SnapshotBuilder`, `report()`. Added doubleheader dedup. Review is opt-out: only rejected games excluded from plan. |
| 4 | Data layer (`data/`) | Done — deleted payload artifacts, dead DB methods, dead tables. Renamed `when_raw_et` → `when_raw`. Added `league` column to `link_runs`. Added `run_id` to `link_event_bindings`. Added `idx_pm_events_league_date` index. |
| 5 | Hotpath orchestration (`hotpath/`) | Done — deleted replay system, dead Protocol classes, `NativeMlbEngineBridge`, dead attributes. Removed MLB-only gate. Expanded `CANONICAL_MARKET_TYPES` for soccer. Fixed incremental refresh market type normalization. |
