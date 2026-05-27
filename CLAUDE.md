# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

polybot2 is a sports-trading bot for Polymarket. Hybrid architecture: Python control plane for CLI, data sync, linking, and orchestration; Rust native hotpath via PyO3/maturin for low-latency score ingest → decision → order dispatch. Supports MLB (baseball), soccer (EPL, La Liga, Bundesliga, UCL), and tennis (ATP, WTA, ITF — French Open men's singles as first league). Four score data providers: Kalstrop V1 (Sportradar, WS, baseball + soccer + tennis), Kalstrop V2 (BetGenius, Socket.IO, soccer + tennis), BoltOdds (WS, broad coverage), Kalstrop Opta (REST catalog, football + baseball — streaming pending for non-World-Cup). Multiplexed concurrent providers per league — configured in `LEAGUES[league]["provider"]` (string or list). Deployment: Linux EC2 (eu-west-1, c8gn.4xlarge).

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

Tests in `tests/live/` require real API credentials and `POLYBOT2_ENABLE_LIVE_*` env vars.

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

The hot path is split across two threads. The **WS thread** parses frames, evaluates the plan, pops pre-serialized order payloads for all intents in the frame, and pushes them to the submitter via a lock-free SPSC ring (`rtrb`) — no HTTP, no string allocation on the success path. The **submitter thread** owns a `FastClobSubmitClient` (custom `reqwest`-based HTTP client with cached L2 auth), spins on the ring, and posts to the CLOB inline (no `tokio::spawn` per batch). Orders are pre-serialized to JSON bytes at presign time — the submitter only computes HMAC + sends bytes.

| Module | Role |
|--------|------|
| `kalstrop_types.rs` | Zero-copy serde structs for Kalstrop WS frames (`KalstropFrame<'a>`, etc.) |
| `baseball/` | Sport-specific: `engine.rs` (NativeMlbEngine, process_tick_live, merge_plan), `eval.rs` (totals, NRFI, walkoff, moneyline, spreads), `parse.rs` (inning parsing), `frame_pipeline.rs` (zero-alloc live path), `types.rs` (GameState, GameTargets, etc.) |
| `soccer/` | Sport-specific: `engine.rs` (NativeSoccerEngine), `eval.rs` (totals, three-way moneyline, BTTS, spreads, corners, halftime result, exact score with early NO), `parse.rs` (half parsing), `frame_pipeline.rs`, `types.rs` |
| `tennis/` | Sport-specific: `engine.rs` (NativeTennisEngine, 6 evaluators: match totals, first-set totals, set totals, moneyline, first-set winner, set handicap), `frame_pipeline.rs`, `types.rs`. Per-game `sets_to_win` (2 for BO3, 3 for BO5). Own `SpreadSlot` (no cross-module dependency on soccer). |
| `kalstrop_v2_sio.rs` | Socket.IO/Engine.IO client for Kalstrop V2 (`SioConnection`, handshake, `subscribe`/`unsubscribe`, frame classification) |
| `kalstrop_v2_types.rs` | V2 frame extractor (`fast_extract_v2`): fixture_id, home/away scores, currentPhase. Prebuilt finders. |
| `kalstrop_v2_frame_pipeline.rs` | V2 soccer frame pipeline: extract → dedup → phase map → engine → dispatch |
| `ws_kalstrop_v2.rs` | V2 Socket.IO worker: reconnect loop, subscription management, frame drain |
| `ws_multiplexed.rs` | Multiplexed WS worker: manages V1+V2 connections in one `tokio::select!` loop. "Fastest wins" — whichever provider delivers a score change first triggers evaluation. Independent reconnection per provider. |
| `boltodds_types.rs` | Byte-level extractor for BoltOdds frames (`fast_extract_boltodds`). Extracts `game_label`, goals, corners, match period from raw JSON without serde. |
| `boltodds_frame_pipeline.rs` | BoltOdds soccer frame pipeline: extract → dedup → eval → dispatch. Uses integer-based dedup (goals + corners + period). |
| `ws_boltodds.rs` | BoltOdds WS worker: plain WS connection (`?key=TOKEN`), subscribe by game labels, frame drain loop. Simpler protocol than V1 (no GraphQL). |
| `fast_extract.rs` | Byte-level extractor for Kalstrop V1 frames. `fast_extract_v1` (soccer/baseball): fixtureId, homeScore, awayScore, freeText, corners. `fast_extract_tennis_v1` (tennis): adds nested currentPhase fields (games_home/away, phase number) and phases array scan for total_games/first_set_games. |
| `dispatch/flow.rs` | `DispatchHandle::pop_for_target(TargetIdx)` (sync, returns `Box<PreparedOrderPayload>` or err) and `send_batch(SubmitBatch, &log)` (sync, pushes one Batch onto the SPSC ring). `dispatch_intents(intents, handle, log)`: shared dispatch logic extracted from frame pipelines. Handles noop-mode logging and http-mode presign pop + batch build + send. Called by baseball, soccer (V1, BoltOdds, V2), and tennis frame pipelines. |
| `dispatch/presign_pool.rs` | Presign pool indexed by `TokenIdx` (`Vec<SmallVec<[Box<PreparedOrderPayload>; 2]>>`). Depth is 1-2 per token (primary + optional secondary order). `PreparedOrderPayload` contains pre-serialized order JSON bytes (serialized once at presign time). `warm_presign_startup_into` signs + serializes orders per token at startup. |
| `dispatch/fast_submit_client.rs` | `FastClobSubmitClient`: custom HTTP client bypassing the SDK for `POST /order` (single) and `POST /orders` (batch). Caches decoded API secret, uses stack-based `itoa` + base64 for HMAC, sends pre-serialized order bytes directly. Two methods: `post_order_bytes_single` (single order) and `post_orders_bytes` (batch, via generic `send_json_post` helper). `warmup_connection()` pre-establishes TCP + TLS + HTTP/2 via `GET /time`. Configured with `tcp_nodelay(true)`, `connect_timeout(5s)`, `timeout(10s)`, `pool_max_idle_per_host(30)`, `pool_idle_timeout(None)`, HTTP/2 via ALPN. |
| `dispatch/sdk_exec.rs` | `OrderSubmitter::new`, `ensure_sdk_runtime_async`, `sdk_client_ref`, `signer_ref` (SDK init for presign signing). `sign_order_batch` (presign warmup). `map_post_response` helper. The SDK client is used only for order signing at startup/patch — not for HTTP submission. |
| `dispatch/submitter.rs` | `run_submitter_async`: spin-loop that pops from SPSC ring and calls `submit_batch_task` inline (no `tokio::spawn`). 3-tier dispatch: `len==1` (single `POST /order`, bare `.await`), `2..=15` (one `POST /orders` batch — 1 HMAC, 1 HTTP request), `>15` (chunked into groups of `MAX_CLOB_BATCH=15`, concurrent `join_all`). Uses `ChunkScratch` for reusable body/index buffers. No semaphore. Keeps CLOB connection warm via `CLOB_KEEPALIVE_INTERVAL` (120s) pings during idle — no order ever pays cold-start TLS. |
| `dispatch/types.rs` | `DispatchHandle`, `OrderSubmitter`, `SubmitWork`, `SubmitBatch`, `PreparedOrderPayload`, `SharedRegistry` (ArcSwap). `PreparedOrderPayload` stores pre-serialized order JSON bytes + `time_in_force: OrderTimeInForce` (carried through to log output). |
| `ws.rs` | Kalstrop V1 live worker: GraphQL WS connect, subscription management, frame drain loop. Uses `worker_clock_origin: Instant` for monotonic timestamps. Dispatches to sport-specific frame pipeline via `SportEngine` enum (`Baseball`/`Soccer`/`Tennis` variants). Drains `patch_rx` at quiescent points for hot-patch application. |
| `runtime.rs` | PyO3 `NativeHotPathRuntime`: builds both halves at startup with shared `Arc<TargetRegistry>`, runs presign warmup, spawns submitter and WS threads, lifecycle (`start`/`stop`/`patch_plan`). Provider-based worker dispatch: spawns `ws.rs` (Kalstrop V1), `ws_kalstrop_v2.rs` (Kalstrop V2), `ws_boltodds.rs` (BoltOdds), or `ws_multiplexed.rs` (multi-provider) based on `provider`/`providers` in config. Sport engine selected from explicit `sport` field in plan JSON via `detect_sport_from_plan()` — returns `Result`, crashes on unknown sport (no silent fallback). CPU core pinning via `core_affinity` for WS + submitter threads (`ws_core_idx`/`submitter_core_idx` in config). `health_snapshot()` exposes WS + submitter health. |
| `lib.rs` | Shared types (`GameIdx`, `TargetIdx`, `TokenIdx`, `OverLine`, `SpreadSide`, `Intent`, `RawIntent`), `SportEngine` enum (`Baseball`/`Soccer`/`Tennis`), `PatchPayload`, `NativeHotPathRuntime`, config structs. Sport-specific types live in `baseball/types.rs`, `soccer/types.rs`, `tennis/types.rs`. |
| `log_writer.rs` | Structured JSONL log (schema V2). Wrapped in `Arc<Mutex<LogWriter>>` and shared by WS thread (logs ticks) and submitter thread (logs order outcomes). `LogWriter` stores `sport: &'static str` set at construction. `log_tick` takes `gid: &str` + `&TickPayload` enum (`Baseball`/`Soccer`/`Tennis`) — each variant carries sport-specific named score fields (`runs_home`/`goals_home`/`sets_home`), league (`lg`), game state, and source provider (`src`). Order events include `gid`, `tif` ("FAK"/"GTC"/"FOK"), and are keyed via `gid_from_sk(sk)`. Startup event emits `v:2`, `sport`, `leagues` array. Connection events carry `src` (provider name). |

### Hot Path Pipeline

```
WS thread (per frame, zero-alloc live path):
  fast_extract_v1(frame_text)                        (byte-level scan, no serde)
  → extract fixture_id, scores, freeText, corners    (borrowed slices, no allocation)
  → engine.check_duplicate / process_tick_live(GameIdx, ...)  (single FxHashMap lookup, eval into SmallVec)
                                                      returns LiveTickResult { game_idx, intents: SmallVec<[Intent; 32]> }
  → process_decoded_frame_sync builds one SubmitBatch per frame:
        for each Intent:
            DispatchHandle::pop_for_target(target_idx)   (Vec index + Option::take, ~100ns)
            batch.push((target_idx, signed_order))
        DispatchHandle::send_batch(batch, &log)          (one tx.send for the whole frame)
  → log tick using engine.game_ids[game_idx]         (borrowed, no clone)
  → return to ws.next()                              (0 heap allocs, <1µs per material frame)

Submitter thread (spin-loop, inline execution, 3-tier dispatch):
  submit_rx.pop()                                    (lock-free SPSC ring pop)
  submit_batch_task(batch, ...).await                (inline, no spawn)
    → Tier 1 (len==1): bare .await on post_order_bytes_single (no join_all overhead)
    → Tier 2 (2..=15): one POST /orders batch (1 HMAC, 1 HTTP request for all orders)
    → Tier 3 (>15):    chunk into groups of 15, concurrent join_all over post_orders_bytes
    → log_order_ok / log_order_err   (resolve sk/tok strings via ArcSwap<TargetRegistry>)
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

The only string hash on the live path is `game_id_to_idx.get(fixture_id)` — one `FxHashMap::get` per tick, done once in `check_duplicate` (baseball/soccer V1) or `check_boltodds_dedup` (soccer BoltOdds) and the resulting `GameIdx` passed through. After filtering, the engine emits `Intent { target_idx: TargetIdx }` (`Copy`, no strings). String resolution happens only at the FFI boundary and on the submitter (via the shared `Arc<TargetRegistry>` at log time). StateRow uses `InlineStr<N>` (stack-allocated, no heap) for dedup fields.

The Python compiler (`compiler.py`) produces strategy keys (`"gid:TOTAL:OVER:5.5"`, etc.) and embeds them in the compiled plan JSON. Rust stores them in `TargetSlot.strategy_key` for log output but never parses or hashes them.

### Compiler: Market-Type Dispatch Architecture

`_parse_outcome_semantic` in `compiler.py` dispatches on `sports_market_type` first. Each branch uses only its reliable signal — **no cross-type label matching**. This prevents bugs like "Sunderland" matching "under" via substring.

| Market type | Resolution method | Semantics produced |
|---|---|---|
| **Totals / corners** | Label exact match ("Over"/"Under") or outcome_index fallback | `over`, `under` |
| **Spreads** | Slug (`-spread-home-` / `-spread-away-`) + outcome_index | `home_covers`, `home_not_covers`, `away_covers`, `away_not_covers` |
| **Moneyline (baseball)** | Team name in label (substring match against canonical) | `home`, `away` |
| **Moneyline (soccer)** | Slug suffix (`-{team_code}` / `-draw`) + outcome_index | `home_yes`, `home_no`, `away_yes`, `away_no`, `draw_yes`, `draw_no` |
| **Halftime result** | Slug suffix (`-home` / `-away` / `-draw`) + outcome_index | same as soccer moneyline |
| **BTTS / NRFI** | outcome_index (0=yes, 1=no) | `yes`, `no` |
| **Exact score** | Slug (`-exact-score-{h}-{a}` or `-any-other`) + outcome_index | `exact_yes`, `exact_no`, `any_other_yes`, `any_other_no` |
| **Tennis match/first-set totals** | Label "Over"/"Under" or outcome_index fallback | `over`, `under` |
| **Tennis set totals** | Label contains "over"/"under" (includes line number) | `over`, `under` |
| **Tennis first-set winner** | outcome_index (0=home, 1=away) | `home`, `away` |
| **Tennis set handicap** | Slug (`-handicap-home-` / `-handicap-away-`) + outcome_index | `home_covers`, `home_not_covers`, `away_covers`, `away_not_covers` |
| **Moneyline (tennis)** | PM code in label (fallback after team-name match) | `home`, `away` |

Slug helpers: `_three_way_side_from_slug`, `_spread_side_from_slug`, `_parse_exact_score_from_slug`. Polymarket codes looked up from `TEAM_MAP_*` / `PLAYER_MAP_*` via the mapping at compile time.

**Strategy keys** are self-describing for all market types:
- `{gid}:MONEYLINE:HOME_YES`, `{gid}:SOCCER_HALFTIME_RESULT:DRAW_NO`
- `{gid}:SPREAD:HOME_COVERS:-1.5`, `{gid}:SPREAD:AWAY_NOT_COVERS:-2.5`
- `{gid}:BTTS:YES`, `{gid}:TOTAL_CORNERS:OVER:8.5`
- `{gid}:EXACT_SCORE:1_1:YES`, `{gid}:EXACT_SCORE:ANY_OTHER:NO`
- `{gid}:TENNIS_MATCH_TOTAL:OVER:36.5`, `{gid}:TENNIS_FIRST_SET_TOTAL:UNDER:9.5`
- `{gid}:TENNIS_SET_TOTAL:OVER:3.5`, `{gid}:TENNIS_FIRST_SET_WINNER:HOME`
- `{gid}:TENNIS_SET_HANDICAP:HOME_COVERS:-2.5`, `{gid}:TENNIS_SET_HANDICAP:AWAY_NOT_COVERS:-1.5`

The Rust plan loader has `eprintln!` warnings on unhandled semantics — visible at startup. The serializer emits per-target `line` (required for exact scores where `market.line` is NULL).

**`hotpath compile --league <X>`** — dry-run that prints the compiled plan with team-resolved semantics. Flags `⚠️ UNKNOWN SEMANTIC`, `⚠️ GENERIC KEY`, and `⚠️ DUPLICATE SEMANTIC`. Run before live trading to verify the compiler.

### Decoupled Submitter

Two structs back the dispatch path:

- **`DispatchHandle`** (WS thread): `cfg`, `registry: Arc<TargetRegistry>`, `shared_registry: SharedRegistry` (ArcSwap), `presign_pool: Vec<Option<Box<PreparedOrderPayload>>>` indexed by `TokenIdx` (depth=1), `submit_tx: Option<rtrb::Producer<SubmitWork>>`. All methods are synchronous.
- **`OrderSubmitter`** (submitter thread): `cfg`, `shared_registry: SharedRegistry`, `submit_rx: rtrb::Consumer<SubmitWork>`, `stop_flag: Arc<AtomicBool>`, `log`, `health`. Initializes `FastClobSubmitClient` at startup (custom HTTP + L2 auth, bypasses SDK for submission).

Channel: `rtrb::RingBuffer<SubmitWork>` (capacity 64), lock-free SPSC. `SubmitBatch` is `SmallVec<[(TargetIdx, Box<PreparedOrderPayload>); 32]>`. With dual-order, a frame with N intents produces up to 2N batch entries. The WS thread pushes one Batch per material frame. The submitter spins on `submit_rx.pop()` and processes batches inline (no `tokio::spawn`). 3-tier dispatch: single orders use bare `.await` on `POST /order`; batches of 2–15 use one `POST /orders` call (1 HMAC, 1 HTTP request — strictly faster than N individual calls); batches >15 are chunked into groups of `MAX_CLOB_BATCH` and fired concurrently via `join_all`. The tradeoff vs per-order submission: a deactivated token in a batch causes all orders in that batch to be rejected, but the common dual-order case saves ~8ms by eliminating the second HTTP round-trip.

Shutdown: `stop_flag.store(true)` from the runtime `stop()` method. The submitter checks `stop_flag` after each drain cycle and exits cleanly.

In **noop mode** no submitter thread is spawned; `DispatchHandle::submit_tx` stays `None`; `process_decoded_frame_sync` short-circuits to log `"noop"` per intent inline, never touching the pool or channel.

`LogWriter` is wrapped in `Arc<Mutex<>>` and shared between the WS thread (logs ticks via `log.log_tick`, `log_ws_connect`, `log_ws_disconnect`) and the submitter thread (logs order outcomes via `log_order_ok`/`log_order_err`). Lock contention is negligible — writes are buffered, hold time is sub-µs. **The success path on the WS thread holds zero log locks before the channel send** — tick logging happens after dispatch.

### Presign Pool

Signs 1-2 orders per unique token at startup, serializes to JSON bytes, and stores `PreparedOrderPayload`s so the WS thread can pop in ~100ns instead of ECDSA-signing in ~10–50ms. Pool depth is 1-2 per token (primary + optional secondary order). Pool ownership lives on `DispatchHandle` (WS thread) as `Vec<SmallVec<[Box<PreparedOrderPayload>; 2]>>` indexed by `TokenIdx` (`std::mem::take()` drains all orders at once). When `secondary_time_in_force` is configured, each intent fires two pre-signed orders with different parameters (e.g., FAK for immediate fill + GTC for resting liquidity). The SDK client used to sign warmup orders lives on `OrderSubmitter` (submitter thread). At startup:

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
| `hotpath/order_policy.py` | `OrderPolicy` dataclass — sport-generic execution profile (amount, size, price, time-in-force). `market_overrides` dict for per-market-type sizing (e.g., smaller bets on exact score). `for_market_type()` resolves overrides. Supports dual-order via `secondary_*` fields — when configured, each intent fires two pre-signed orders (primary + secondary). |
| `hotpath/v2_resolver.py` | V2 fixture ID resolver: `build_pending_games`, `try_resolve_games` (polls V2 tournament fixtures, matches by teams+time), `compile_for_resolved_game` (single-game plan with fixture_id substitution). Detects finished games. |
| `sports/` | Provider catalog adapters: `KalstropV1Provider` (V1 REST catalog), `KalstropV2Provider` (V2 REST catalog), `KalstropOptaProvider` (Opta REST catalog), `BoltOddsProvider` (REST catalog). No Python-side streaming — all WS streaming is handled by Rust or standalone capture scripts. |
| `sports/kalstrop_v2.py` | Kalstrop V2 catalog discovery — REST hierarchy: sports → competitions → tournaments → fixtures |
| `sports/kalstrop_opta.py` | Kalstrop Opta catalog discovery — REST hierarchy: sports → competitions (numeric IDs) → fixtures. Covers football + baseball. Composite `"Name|ID"` encoding in `category_name`/`league_raw` columns. |
| `sports/kalstrop_auth.py` | Shared HMAC auth helper for V1, V2, and Opta REST + WS auth headers |
| `config/` | `live_trading.py` (execution policy), `mappings.py` (league registry, provider aliases, league disambiguation via `PROVIDER_LEAGUE_COUNTRY`), `baseball_mappings.py` / `soccer_mappings.py` / `tennis_mappings.py` (team/player aliases per league). Tennis uses `PLAYER_MAP_*` dicts with PM code derivation rule: last word of PM name, lowercased, truncated to 7 chars. |
| `guardian/` | Overturn detection for soccer. `manager.py` (orchestrator integration), `tracker.py` (log tailing + WS + state), `overturn.py` (dual-signal detector), `executor.py` (sell/cancel). See Guardian section below. |
| `scripts/` | Standalone capture scripts for raw frame recording: `capture_kalstrop_v2.py`, `capture_opta.py`, `capture_multi.py` (multi-provider comparison with V1+V2+BoltOdds+Opta), `build_capture_plan.py` (auto-generates `games.json` from DB). Not part of the `polybot2` package — run directly. |

### FFI Boundary (Python → Rust)

Python `NativeHotPathService` calls Rust `NativeHotPathRuntime` via PyO3:
- `start(config_json, compiled_plan_json, exec_config_json)` — all configs use `deny_unknown_fields`
- `stop()`, `set_subscriptions(provider_subs: HashMap<String, Vec<String>>)`, `prewarm_presign(templates_json)`, `health_snapshot()`
- `patch_plan(plan_json, templates_json)` — hot-patch: signs new orders (GIL released), sends `PatchPayload` to WS thread via dedicated `patch_tx` channel. WS thread calls `engine.merge_plan()` (append-only), extends dispatch pool, rebuilds registry, stores into `ArcSwap` (submitter sees new registry on next batch).
- Compiled plan serialized via `serialize_compiled_plan()` in `native_engine.py`. Top-level fields: `provider`, `league`, `sport` (explicit: "baseball"/"soccer"/"tennis"), `run_id`, `games[]`. Per-game: `provider_game_id`, `kickoff_ts_utc`, `markets[]`, `alternate_provider_game_ids[]`, `sets_to_win` (tennis only, 2=BO3, 3=BO5). Rust reads `sport` via `detect_sport_from_plan()` — unknown/missing sport is a hard error.
- `health_snapshot()` returns `{running, subscriptions, reconnects, last_error, submitter: {present, running, last_error}}`. The nested `submitter` object is populated in HTTP mode and absent (`present: false`) in paper mode.

### Order Types

The dispatch layer supports three order types, configured via `time_in_force` in `HOTPATH_EXECUTION_POLICY` (parsed into `OrderTimeInForce` at startup):
- **FAK/FOK** (market orders) → `client.market_order().amount(SdkAmount::usdc(...))` — uses `amount_usdc`
- **GTC** (limit orders) → `client.limit_order().size(...).price(...)` — uses `size_shares`

GTD is not supported (presigned GTD orders cannot carry runtime-computed expiration). Limit prices must be `.normalize()`d to strip trailing zeros, or the SDK rejects them for exceeding the token's tick-size precision.

## Critical Invariants

1. **Hotpath ordering:** Match update → decision → channel send → tick log. The WS thread does no HTTP work and acquires no log locks before the channel send on the success path.

2. **Fail-closed:** Presign pool miss → error logged on the WS thread (no fallback to unsigned submit). Startup warmup failure → process won't trade. Empty `order_id` with `success: true` from the CLOB → treated as `Err` (not a phantom fill).

3. **Spread evaluation:** Each spread line has a `SpreadSlot { side, line, covers_idx, not_covers_idx }`. At game end: `(margin as f64) + slot.line > 0` → fire `covers_idx`, else fire `not_covers_idx`. Four distinct semantics per line: `home_covers`, `home_not_covers`, `away_covers`, `away_not_covers`. `margin = home - away` for HOME, `-margin` for AWAY.

4. **Totals over crossing:** For a score change from `prev` to `now`, iterate `over_lines` and fire any with `half_int in [prev, now)`. Direct array indexing — no string keys, no HashMap.

5. **One-shot intents:** Each token can fire at most once per session, enforced by the presign pool (depth=1, `Option::take()`). Once `pop_for_target` takes the signed order, the pool slot is `None` and any repeat intent fails closed at dispatch time. There is no engine-side `attempted` bitset — the pool is the sole gate.

6. **Independent evaluation:** `evaluate_totals`, `evaluate_nrfi`, `evaluate_final` all run on every tick (not an if/else chain). Each takes `&mut self` only to update its own resolution flags (`Vec<bool>` writes); no string allocations.

7. **NRFI first-inning gate:** `nrfi_first_inning_observed: Vec<bool>` indexed by `GameIdx`. Late subscriptions (inning > 1) are permanently skipped. Ticks without inning data defer evaluation. Extra innings use `inning_number = Some(10)` as sentinel (walkoff `>= 9` fires correctly). Kalstrop freeText patterns: `"Extra inning top"`, `"Extra inning bottom"`, `"Break top EI bottom 9"`, `"Break top EI bottom EI"` — all map to `(Some(10), half)`. Regular inning: `"Break top 1 bottom 1"` = mid-inning break (bottom of 1st not yet played, first inning NOT over); `"Break top 2 bottom 1"` = first inning fully done (NRFI NO can fire here if total=0).

8. **Zero-copy parsing:** The live WS path deserializes directly into borrowed `KalstropFrame<'a>` structs and extracts fields without constructing a `Tick` or allocating any strings. The `fixture_id` is looked up as `&str` directly from the serde struct.

9. **Submitter thread isolation:** The WS thread never owns or references the SDK client or HTTP client. Submitter ownership is established at startup; the SPSC ring is the only communication path. `ArcSwap<TargetRegistry>` is the shared registry pointer (updated atomically on patch, loaded per batch by submitter for log attribution).

10. **Frame-preserving batch:** `process_decoded_frame_sync` builds exactly one `SubmitWork::Batch` per material WS frame. All intents from the same frame ride together in one ring push. The submitter processes batches inline (strict serialized queue) via 3-tier dispatch: single order → `POST /order`, 2–15 orders → one `POST /orders` batch, >15 → chunked concurrent `join_all`.

11. **Monotonic clock:** Engine timestamps use `worker_clock_origin: Instant` set at WS-worker startup, sourced via `Instant::elapsed().as_nanos()`. Wall-clock (`now_unix_ns`) is used only for log timestamps and L2 auth headers — never for engine math.

12. **Pool sharing semantics:** The presign pool is indexed by `TokenIdx`, not `TargetIdx`. Two targets pointing to the same token share one queue entry. This is enforced structurally — `tokens` is deduplicated at plan load.

13. **Exact score early NO (slice approach):** Since soccer scores only increase, predicted scorelines become impossible mid-game. On each goal, only the newly-impossible "slice" is fired — not all currently-impossible slots. Home goal (`x-y → x+1-y`): fire NO on slots where `home_pred == x AND away_pred >= y`. Away goal (`x-y → x-y+1`): fire NO on slots where `away_pred == y AND home_pred >= x`. At full time: fire YES on matching score, NO on remaining unresolved slots (`home_pred >= home AND away_pred >= away AND NOT exact match`). `any_other_score` fires at full time only. `SoccerGameState` tracks `prev_home`/`prev_away` for delta detection. No per-slot tracking needed — the presign pool prevents double-fire.

## CLI Commands

```bash
polybot2 market sync
polybot2 market sync --all                          # include resolved/closed markets
polybot2 provider sync                              # syncs all providers (kalstrop_v1, kalstrop_v2, boltodds)
polybot2 provider sync --provider kalstrop_v1       # sync a single provider
polybot2 link build                                 # build links for all live leagues (one run_id), default horizon per league
polybot2 link build --horizon-hours 6               # only link games within 6 hours
polybot2 link build --league-scope all              # include non-live leagues too
polybot2 link review --run-id N                     # interactive review (opt-out: reject bad matches)
polybot2 hotpath compile --league epl                # dry-run: print compiled plan with team-resolved semantics
polybot2 hotpath compile --league mlb --link-run-id 1  # verify plan before trading
polybot2 hotpath live --league mlb --execution-mode live  # auto-discovers latest run_id
polybot2 hotpath live --league epl --execution-mode live  # single league
polybot2 hotpath live --sport soccer --execution-mode live # all soccer leagues in one process
polybot2 hotpath live --league epl laliga ucl --execution-mode live  # explicit multi-league
polybot2 hotpath observe --log-file path/to/hotpath_42_*.jsonl   # live terminal scoreboard
polybot2 hotpath observe --run-id 42 --link-run-id N --db path.sqlite  # auto-discover log, resolve team names
```

Raw score frame capture is handled by standalone scripts in `scripts/` (not part of the CLI):
```bash
python scripts/capture_multi.py --games-file games.json --out ./captures/2026_05_04 --duration 14400
python scripts/capture_kalstrop_v1.py --fixture-id <UUID> --out ./captures/v1 --duration 7200
```

The prerequisite pipeline: market sync → provider sync → link build → hotpath live. One hotpath process per sport (or per league for backward compat).

### Multiplexed Concurrent Providers

Soccer leagues can use multiple providers simultaneously ("fastest wins"). Configured via `LEAGUES[league]["provider"]` as a list (e.g., `["kalstrop_v2", "kalstrop_v1"]`). The Rust multiplexed worker (`ws_multiplexed.rs`) manages all connections in one `tokio::select!` loop with three branches: V1 (Kalstrop WS), V2 (BetGenius Socket.IO), and BoltOdds (plain WS):

- V2 is ~1.5s faster for goals → fires totals, exact score, BTTS first
- V1 is faster for halftime/match-end → fires halftime result, moneyline, spreads first
- V1 provides corners; V2 does not
- BoltOdds provides broad coverage with simple WS protocol; subscribes by game labels from plan
- All providers' frames resolve to the same `GameIdx` via `game_id_to_idx` (which holds all provider IDs)
- Dedup is per-provider (V1 string-based, V2 integer-based, BoltOdds integer-based) but the engine's state-level delta detection prevents duplicate intents from slower providers
- If one connection drops, the others keep streaming; reconnection is independent per provider
- V2 subscription failures are tracked — only successfully-subscribed IDs are marked active; failed IDs are retried on the next `SetCandidateSubscriptions`
- BoltOdds connection uses `try_connect_boltodds()` helper: handshake (`socket_connected` ack), subscribe by game labels, then frame drain via `process_boltodds_frame_sync`

The `providers` field in `RuntimeStartConfig` triggers the multiplexed worker. When absent, the single-provider worker branches (V1/V2/BoltOdds) run as before.

**Per-provider subscription routing:** Subscriptions are sent as `HashMap<String, Vec<String>>` (provider name → game IDs), not a flat list. The multiplexed worker extracts V1/V2 subs directly from the map (`candidate_subs.get("kalstrop_v1")`, `.get("kalstrop_v2")`). BoltOdds uses static `game_labels` from the plan. Non-multiplexed workers flatten the map. The orchestrator builds the map from the plan's `provider_game_id` + `alternate_provider_game_ids` and maintains a `_cumulative_provider_subs` dict that grows as V2 games are resolved.

### Multi-League Process

`--sport soccer` runs all live soccer leagues in one process (one engine, one presign pool, one submitter). Non-V2 leagues (EPL via BoltOdds) start immediately. V2 leagues (La Liga, UCL, Bundesliga) resolve via an interleaved V2 resolution loop. `compile_multi_league_plan()` merges per-league plans into one `CompiledPlan` with `provider` and `league` derived from the first league in the input (not hardcoded). The Rust `detect_league_from_plan` reads `league` to determine the sport engine (Baseball vs Soccer). Mixed-sport processes (e.g., `--league mlb epl`) are rejected at startup.

**Per-league order policies:** `HOTPATH_EXECUTION_POLICY` in `config/live_trading.py` is keyed by league. The orchestrator loads policies for all leagues in the process and passes them as `dict[str, OrderPolicy]`. Template generation and incremental refresh route each game's targets through its league's policy via `game.canonical_league`. Supports `market_overrides` per market type (e.g., smaller bets on exact score).

### Hotpath Live Orchestrator

`polybot2 hotpath live` starts the hotpath for one or more leagues. Supports `--league` (one or more keys) or `--sport` (all live leagues for that sport in one process). Provider(s) derived from `config/mappings.py` (`LEAGUES[league]["provider"]` — string or list for multiplexed). Run_id is auto-discovered (latest for the primary league) unless `--link-run-id` is passed.

Daily workflow:
```bash
polybot2 market sync
polybot2 provider sync
polybot2 link build
polybot2 hotpath live --league mlb --execution-mode live
polybot2 hotpath live --sport soccer --execution-mode live  # all soccer in one process
```

Review is opt-out: all linked games enter the plan unless explicitly rejected via `link review`. Link build applies a default `--horizon-hours` per league (MLB: 12h, EPL/UCL: 24h from `HOTPATH_RUNTIME_POLICY`) to scope to games starting soon. Soccer linking enforces strict home/away ordering against Polymarket event team order — rejects provider games with flipped designation.

**Postponed games:** The linker uses a supplementary `kickoff_ts_utc` range query alongside the `game_date_et` query, catching games whose Polymarket events retain the original date but have updated kickoff timestamps. Both query results are merged and deduped by `event_id`.

**Alternate provider game IDs:** The compiled plan carries `alternate_provider_game_ids` per game — other providers' IDs for the same canonical game. The Rust engine inserts these into `game_id_to_idx` at plan load so frames from any provider resolve to the same `GameIdx`.

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
- **Breaking change (April 2026):** `eventState` field removed from WS `matchSummary` entirely (not renamed — absent). Game completion is now detected from `matchStatusDisplay[0].freeText` containing `"Ended"`. Each sport's frame pipeline detects completion inline via `free_text.trim().eq_ignore_ascii_case("Ended")`.
- **Python provider is catalog-only.** `kalstrop_v1.py` contains only `load_game_catalog()` and catalog helpers. All WS streaming code has been removed from the Python side — the Rust hotpath handles live score streaming, and standalone scripts in `scripts/` handle raw frame capture.

**V2 (`kalstrop_v2`)** — `stats.kalstropservice.com/api/v2/genius`. HMAC auth (shared credentials with V1 via `kalstrop_auth.py`). BetGenius-backed. Covers top-tier soccer (EPL, La Liga, Bundesliga, UCL) and tennis (ATP, WTA, ITF). **Does not support baseball.** V2 provider sync uses `catalog_sport_slugs=("football", "tennis")` to pull both soccer and tennis fixtures.
- **REST catalog:** `/genius/sports` → `/genius/sports/{slug}/competitions` → `/genius/sports/{sport}/competitions/{category}/{tournament}/fixtures`. Legacy paths without `/genius/` prefix also work.
- **Fixture IDs:** Numeric `event_id`s (e.g., `7490587`). Different ID space from V1 UUIDs.
- **Socket.IO streaming:** `genius_subscribe` with `{fixtureId, activeContent: "court"}` → `genius_update` events with `scoreboardInfo`. Implemented in Rust (`kalstrop_v2_sio.rs`, `ws_kalstrop_v2.rs`). Requires `product=genius-stats` query param on the Socket.IO connection. Unsubscribe via `genius_unsubscribe`.
- **Team names are shortened** compared to V1/Polymarket: `"Man Utd"` vs `"Manchester United FC"`, `"Wolves"` vs `"Wolverhampton Wanderers FC"`. Separate provider aliases needed in `config/soccer_mappings.py`.
- **Empty catalog protection:** If V2 API fails, `sync_provider_games` returns an error instead of wiping the existing snapshot.
- **V2 event_id instability:** The prematch `event_id` from the catalog cannot be resolved to a BetGenius `fixture_id` until the game goes live. The `/fixtures/{event_id}/providers` endpoint returns data from a different ID space for prematch games. Resolution happens at runtime via `v2_resolver.py`: Python polls tournament fixtures, matches by team names + start time, resolves fixture_id via `/providers`, then compiles a single-game plan and hot-patches it into the running Rust engine. V2 leagues use a deferred-start orchestrator (hotpath waits for game kickoff before resolving and starting).

**BoltOdds** — `spro.agency/api`. API key via query param. Covers MLB, EPL, and other leagues. Currently used as the EPL provider.
- **REST catalog:** `GET /get_games?key=TOKEN` — returns all games with `universal_id`, `game`, `when` (ET timestamps), `orig_teams`, `sport`.
- **WS streaming:** `wss://spro.agency/api/livescores?key=TOKEN` — delivers `match_update` frames with `designation: {"A":"home","B":"away"}`. The `/livescores` path is required (not `/api` alone).
- **Team names are abbreviated** (e.g., `"ATL Braves"` for baseball, `"Chelsea"` for soccer). Provider aliases needed in mappings.
- **Duplicate entries:** BoltOdds sometimes publishes two entries for the same game with flipped home/away (e.g., "Sunderland vs Man Utd" and "Man Utd vs Sunderland"). The linker's strict home/away ordering check (soccer only) rejects the flipped duplicate by comparing against the Polymarket event's team ordering.

**Kalstrop Opta** — `stats.kalstropservice.com/api/v2/opta`. Same HMAC auth. Opta/Sportradar-backed. Covers football (EPL, La Liga, Bundesliga, UCL, MLS, World Cup) + baseball (MLB, NPB, KBO). Currently catalog-only for live streaming (pending World Cup activation).
- **REST catalog:** `/sports` → `/sports/{sport}/competitions` (numeric IDs) → `/sports/{sport}/competitions/{category_id}/{tournament_id}/fixtures`
- **Fixture IDs:** Colon-prefixed format (e.g., `2:7799988`). Must URL-encode the colon.
- **Provider resolution:** `/fixtures/{event_id}/providers?sport={sport}` → `providers.opta.running_ball.fixture_id` (required for Socket.IO streaming). `running_ball` only present for top-tier football; absent for baseball and lower-tier leagues.
- **Socket.IO streaming:** Product slug `opta-stats`. Events: `opta_subscribe`/`opta_unsubscribe`/`opta_message`. Subscribe payload: `{fixtureId, room: "stats"}`. Score format: `stats.score[].{contestantId, value}` (no home/away label — resolve via `/match` REST call).
- **Composite encoding:** `category_name` stores `"England|14"`, `league_raw` stores `"Premier League|102841"`. Parsing: `rsplit("|", 1)` → `(name, id)`.
- **Currently inactive for streaming** — Kalstrop confirmed Opta streaming only supports World Cup games for now.

Documentation: `docs/providers/kalstrop_v2/` covers auth, genius, and opta APIs. V1 docs: `docs/kalstrop_odds_v1.md`.

## Environment Variables

Required for live execution: `POLY_EXEC_API_KEY`, `POLY_EXEC_API_SECRET`, `POLY_EXEC_API_PASSPHRASE`, `POLY_EXEC_PRESIGN_PRIVATE_KEY`, `POLY_EXEC_FUNDER`.

Provider credentials: `KALSTROP_CLIENT_ID`, `KALSTROP_SHARED_SECRET_RAW` (or legacy `CLIENT_ID`, `SHARED_SECRET_RAW`) for V1, V2, and Opta (shared HMAC auth). `BOLTODDS_API_KEY` for BoltOdds.

Database: `POLYBOT2_DB_PATH` (default: `../../data/prediction_markets.db` relative to working dir).

Log directory: `POLYBOT2_LOG_DIR` (default: current working directory). Logs are organized per-run: `{log_dir}/{run_id}/hotpath_{sport}_{timestamp}.jsonl`. The JSONL log (schema V2) contains self-describing events with `sport`, `lg` (league), and `src` (provider) fields. Tick events use sport-specific score field names (`runs_home`/`goals_home`/`sets_home`). Order events include `gid` for direct game correlation. Python consumers (`live_observer.py`, `tracker.py`, `log_reader.py`) support both V2 and V1 log formats via field-name fallback.

## Constraints

- Deployment target is Linux EC2 (eu-west-1, c8gn.4xlarge — 16 vCPUs, 32 GB RAM). Dev is macOS (ARM). CPU core pinning configured per league in `HOTPATH_RUNTIME_POLICY` (`ws_core_idx`, `submitter_core_idx`). Core 0 avoided (IRQ handler).
- Python ≥ 3.11, Rust edition 2021, PyO3 0.22 with ABI3.
- `polymarket_client_sdk_v2` 0.6.0-canary.1 (CLOB V2). Supports `SignatureType::Poly1271` (value 3) for deposit wallet accounts with ERC-7739 wrapped signatures. Pins `alloy` at 1.6.3 — do not add a different alloy version or traits will mismatch.
- `smallvec` is a hot-path dependency (`SubmitBatch` payload). Don't replace with `Vec` without measuring — the inline `[T; 32]` capacity covers the common dual-order case without heap allocation.
- Gamma API caps results at 100 per request regardless of `limit` parameter. `batch_size` in `sync_config.py` is set to 100 to match.
- Prefer deletion over compatibility shims. No backwards-compat wrappers for removed features.
- The field name is `amount_usdc` everywhere (not `notional_usdc` — that was the legacy name, fully removed).
- The old telemetry system (Unix DGRAM socket) was removed. Replaced by a structured JSONL log file (`log_writer.rs`, schema V2) shared via `Arc<Mutex<>>`. The `polybot2 hotpath observe` command reads the JSONL log file via `live_observer.py` and renders an in-place terminal scoreboard. Sport-aware: baseball shows `AWAY-HOME` with inning (`T3`, `B7`); soccer shows `HOME-AWAY` with half (`1H`, `HT`, `2H`, `FT`); tennis shows `HOME-AWAY` with set (`SET`). Team abbreviations use Polymarket codes from `config/mappings.py`. Tick events carry per-side corners (`corners_home`/`corners_away`) for soccer and per-game league (`lg`) for all sports.
- Python canonical form for BTTS is `"btts"` (not `"both_teams_to_score"`). Must match the Rust `canonical_soccer_market_type` which also normalizes to `"btts"`.
- SDK config uses `use_server_time(false)` to avoid a `GET /time` round-trip before every order POST. Host clock must be disciplined with chrono/NTP on the deployment target.
- Multi-intent frames batch in `process_decoded_frame_sync` (one Batch per frame). The submitter processes batches inline (no spawn) via 3-tier dispatch: single order → `POST /order` (bare `.await`), 2–15 orders → one `POST /orders` batch (1 HMAC, 1 HTTP), >15 → chunked concurrent `join_all`. No semaphore. Empty `order_id` with `success: true` is treated as failure (`map_post_response`). Batch tradeoff: a deactivated token poisons its batch, but the dual-order case saves ~8ms vs individual submission.
- Parsing uses zero-allocation byte-level scanning (`eq_ignore_ascii_case`, byte accumulator for numbers) — no `to_lowercase()`/`to_uppercase()` heap allocations on the tick path.
- Final-game cleanup (`cleanup_completed_game_idx`) is deferred until after intents are selected, not during `evaluate_final`. `final_resolved_games[gi] = true` blocks re-evaluation immediately; cleanup runs in `process_tick` before returning. Cleanup clears only lightweight row data (`rows`, `game_states`, `nrfi_first_inning_observed`); completion tombstones (`totals_final_under_emitted`, `nrfi_resolved_games`) are preserved for the session to prevent duplicate emission from repeated final frames.
- Tests use temp-path `LogWriter`s (no actual log inspection in non-live tests).

## Latency Optimization Roadmap

Target: single-digit microsecond end-to-end on the WS thread (frame available → bytes sent on the channel). Status:

1. **Strategy evaluation — DONE.** Plan compiled into `GameIdx`/`TargetIdx` integer indices and per-game arrays. Evaluation is array indexing (~10ns), zero `format!()`, zero `HashMap` lookups except the single `fixture_id → GameIdx` resolve.

2. **Decoupled submitter — DONE.** WS thread does sync `pop + send`; submitter thread does HTTP. WS-thread cost per intent is ~100ns presign pop + amortized channel send. SDK client lives only on the submitter.

3. **Frame-preserving batch — DONE.** `process_decoded_frame_sync` builds one `SubmitWork::Batch(SmallVec<[(TargetIdx, Box<PreparedOrderPayload>); 32]>)` per material frame. With dual-order presign, a frame with N intents produces up to 2N batch entries. Multi-intent frames always reach the CLOB as one logical group. Submitter processes batches inline — no cross-frame coalescing, no head-of-line blocking.

4. **Index-keyed payload + Arc<TargetRegistry> — DONE.** Pool indexed by `TokenIdx` (`Vec<Option<Box<SdkSignedOrder>>>`, depth=1, boxed: `Option::take()` moves 8 bytes). Channel payload is `(TargetIdx, Box<SdkSignedOrder>)` — no string allocation on the WS success path. Strings reconstructed via the registry only at log time.

5. **Logging swap — DONE.** Tick logging happens after dispatch in `process_decoded_frame_sync`. Success path holds zero log locks before the channel send.

6. **Monotonic clock — DONE.** Engine timestamps use `Instant::elapsed()`-derived nanos from a worker-local origin. Wall-clock is reserved for log timestamps and L2 auth.

7. **Submitter health surface — DONE.** `health_snapshot()` exposes `{present, running, last_error, posted_ok, posted_err}`. Outage blindness fixed.

8. **Empty `order_id` correctness — DONE.** `map_post_response` treats `success: true` with empty `order_id` as `Err`, preventing phantom-fill bookkeeping.

9. **SDK bypass + pre-serialization — DONE.** `FastClobSubmitClient` replaces the SDK's `post_order`/`post_orders` for submission. Order JSON is serialized once at presign time (`PreparedOrderPayload`). Submitter caches decoded API secret bytes and uses stack-based `itoa` + base64 for HMAC — no per-request allocations except the `reqwest` request builder. Quote normalization for HMAC parity handled via `memchr` scan (no `String::replace`).

10. **SPSC ring + spawnless submitter — DONE.** `flume::unbounded` replaced with `rtrb::RingBuffer` (capacity 64, lock-free SPSC). Submitter runs a spin-loop (`std::hint::spin_loop()` when idle) and processes batches inline — no `tokio::spawn` overhead (~12µs saved per batch). Backpressure: ring full → orders dropped (fail-safe; submitter overwhelmed means stale orders). Shutdown via `AtomicBool` stop flag.

11. **Deferred final-game cleanup — DONE.** `cleanup_completed_game_idx` now preserves resolution tombstones (`totals_final_under_emitted`, `nrfi_resolved_games`) and only clears lightweight row data.

12. **Benchmark harness — DONE.** Implemented as in-crate tests behind `bench-support` feature flag (`src/bench_support.rs`). Run via `cargo test --manifest-path native/polybot2_native/Cargo.toml --features bench-support --release -- bench_ --nocapture`.

13. **CLOB V2 SDK migration — DONE.** `polymarket-client-sdk 0.4.4` → `polymarket_client_sdk_v2 0.6.0-canary.1`. Supports deposit wallet accounts via `SignatureType::Poly1271` (ERC-7739 wrapped signatures). V2 order format (removes `nonce`/`taker`/`expiration`/`feeRateBps`; adds `timestamp`/`metadata`/`builder`) is handled by the SDK internally. Collateral changed from USDC.e to pUSD (transparent to order construction). Live execution tests validated against V2 CLOB with both proxy (type 1) and deposit wallet (type 3) accounts.

14. **`send_batch` zero-allocation success path — DONE.** `DispatchHandle::send_batch` sends first, recovers the batch from `SendError` on failure for diagnostics. No `Vec<TargetIdx>` allocation before the channel send.

15. **Presign warmup parallelized — DONE.** Warmup Tokio runtime uses `new_multi_thread()` so `tokio::spawn`ed ECDSA tasks run on real OS threads. All orders are spawned at once in one `join_all`; a `Semaphore(50)` caps concurrent SDK `.build()` calls (which hit `GET /tick-size` on first call per token — cached after). No inter-batch sleep. The old batch-of-5 + 500ms sleep caused 5+ minute warmup for ~150 orders; the semaphore approach brings it to seconds while staying safe at 1000+ targets.

16. **Zero-alloc WS live path — DONE.** `process_tick_live` takes borrowed `fixture_id: &str` from serde, evaluates into `SmallVec<[Intent; 32]>` (stack), returns `LiveTickResult { game_idx, state, intents }` with no owned strings. `frame_pipeline.rs` parses `KalstropFrame<'a>` and calls `process_tick_live` directly — no `Tick`, no `TickResult`, no `Vec` on the success path. Evaluators use `_into(&mut SmallVec)` variants. `LogWriter` uses a reusable `String` buffer. Result: zero heap allocations from frame receipt through `send_batch`.

17. **Hot-patch O(1) dedup + Arc<str> registry — DONE.** `strategy_keys: HashSet<String>` on engine for O(1) merge dedup (was O(N×M) linear scan). `TokenSlot.token_id` and `TargetSlot.strategy_key` changed to `Arc<str>` so registry clone is ref-count bumps. Deferred sort in `merge_plan` (dirty_games set, sort once per game after all targets inserted). Parallel patch presign via `new_multi_thread` + `tokio::spawn`.

18. **Release build profile — DONE.** `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `strip = "symbols"`.

19. **TCP_NODELAY + request timeouts — DONE.** `tcp_nodelay(true)` on reqwest client (eliminates Nagle delay, 0-40ms saved per order). `connect_timeout(5s)` + `timeout(10s)` prevents hung CLOB responses from blocking the submitter indefinitely. V2 Socket.IO connection also uses `disable_nagle: true` via `connect_async_tls_with_config`.

20. **Zero-copy WS frame delivery — DONE.** `tokio-tungstenite` upgraded from 0.24 to 0.29. `Message::Text` now uses `Utf8Bytes` (backed by `bytes::Bytes`) instead of `String` — zero-copy from the read buffer, eliminates per-frame malloc (~100-200ns saved per frame, invisible to instrumentation since it happens before the timestamp).

21. **Timer-free burst drain — DONE.** Frame drain loops in all workers (V1, V2, BoltOdds, multiplexed) use `futures_util::FutureExt::now_or_never()` for burst reads instead of `tokio::time::timeout(Duration::ZERO)`. Eliminates timer future allocation during frame bursts. Multiplexed worker uses sequential `now_or_never()` polls per provider (V2 first for goal priority) instead of `select!` with `else` (which hung indefinitely due to `std::future::pending()` keeping `else` from firing).

22. **Batch order submission restored (3-tier dispatch) — DONE.** Rolled back from per-order `POST /order` to batch `POST /orders` for the common multi-order case. 3-tier dispatch: `len==1` (single `POST /order`, bare `.await`), `2..=15` (one `POST /orders` — 1 HMAC, 1 HTTP request regardless of order count), `>15` (chunked into groups of 15, concurrent `join_all`). Benchmarked ~28ms batch vs ~36ms for 2 individual orders. Tradeoff accepted: deactivated token poisons the batch, but this is rare and the latency win is material. `ChunkScratch` reuses body/index buffers across batches. `SmallVec<[TargetIdx; 32]>` for tier 2 index tracking. `Arc::clone` only in tier 3 futures (where ownership is needed for concurrent chunks).

23. **Submitter Arc/semaphore cleanup — DONE.** Removed `Semaphore(30)` (never gated), removed per-batch `Arc::clone` of client (pass by reference since `submit_batch_task` is `.await`ed inline). Single-order path: zero atomic ops, zero heap allocs.

24. **HTTP/2 via ALPN — DONE.** Added `"http2"` feature to reqwest. Cloudflare (which fronts the CLOB) negotiates HTTP/2 via ALPN, enabling multiplexed requests over a single TCP connection. Eliminates head-of-line blocking for concurrent tier-3 chunk submissions and saves connection setup overhead.

25. **Connection pool idle timeout + keepalive — DONE.** `pool_idle_timeout` set to `None` (client never proactively drops connections; server decides via GOAWAY). Connection warmup at submitter startup (`GET /time` pre-establishes TCP + TLS + HTTP/2). Keepalive ping every 120s during idle (`CLOB_KEEPALIVE_INTERVAL`) keeps the connection alive across Cloudflare's ~300s idle timeout. Result: no order ever pays the ~9ms cold-start TLS handshake — not the first order, not after hour-long quiet periods.

26. **V2 pong fix in multiplexed burst drain — DONE.** Engine.IO `Ping` frames received during `now_or_never()` burst drain in `ws_multiplexed.rs` were silently dropped (`SioFrame::Ping => {}`). This caused V2 disconnects after ~25s (Engine.IO ping timeout). Fixed: pong is now sent inline during burst drain, with connection reset on pong failure.

See `latency_audit.md` for the source-level audit, `latency_improvements.md` for the response, and `network_latency_audit.md` for the comprehensive network audit.

## Guardian (Overturn Detection)

Python system that monitors the hotpath's positions and detects VAR goal overturns in soccer. Integrated into the hotpath orchestrator via `GuardianManager` — launches automatically for soccer leagues as a daemon thread (dry-run by default). The standalone `polybot2 guardian watch --snapshot` CLI remains for one-shot log inspection.

### Architecture

```
guardian/
├── manager.py         # GuardianManager: orchestrator integration, daemon thread lifecycle
├── log_tailer.py      # JSONL log tail-follow (yields parsed events)
├── state.py           # In-memory state: TrackedOrder, GameState, ScoreEvent, OverturnAlert
├── tracker.py         # Main loop: tails log + WebSocket events + overturn detection
│                      # Game-ID normalization via _game_id_map (alternate → canonical)
├── clob_client.py     # Authenticated CLOB REST client (order queries, cancellation, sell orders)
├── polymarket_ws.py   # Polymarket user channel WS (real-time fill notifications)
├── market_ws.py       # Polymarket market channel WS (orderbook monitoring, no auth)
├── overturn.py        # Dual-signal overturn detector
├── executor.py        # Automated sell/cancel execution (dry-run by default)
└── cli.py             # CLI: polybot2 guardian watch --snapshot (snapshot mode only)
```

### Orchestrator Integration

The `GuardianManager` (in `manager.py`) is created by the hotpath orchestrator after plan compilation for soccer leagues only. It:
- Requires `POLY_EXEC_*` credentials — raises `RuntimeError` on missing credentials (single log line, hotpath continues without guardian)
- Builds all dependencies (ClobClient, WS clients, OverturnDetector, OverturnExecutor) from env
- Computes `token_to_condition` and `game_id_map` from the compiled plan
- Runs `tracker.run_watch()` in a daemon thread with its own asyncio event loop
- Receives `update_plan(compiled_plan)` calls after incremental refresh or V2 resolution
- Stopped cleanly in the orchestrator's finally block

Game-ID normalization (`_game_id_map`) maps alternate provider game IDs (e.g., V2 fixture IDs) to the canonical game ID (used in strategy keys), fixing the V2 fixture-ID ↔ event-ID mismatch that caused orders to be orphaned from score data.

### Overturn Detection (Dual-Signal)

Two mandatory signals must BOTH confirm before acting:

1. **Score reversal** (from hotpath log ticks): score decreased and held for >10s. Disarms automatically if score restores within the window (wobble).
2. **Market activity resumption** (from Polymarket market channel WS): best bid on affected tokens drops below threshold (default $0.80).

When both signals confirm: cancel all resting GTC orders triggered by the overturned goal, sell filled positions at current best bid.

## Deposit Wallet Accounts

New Polymarket accounts use deposit wallets (ERC-1967 proxies) with signature type 3 (`Poly1271`). The SDK handles ERC-7739 signature wrapping automatically.

**Configuration for deposit wallet accounts:**
```
POLY_EXEC_SIGNATURE_TYPE=3
POLY_EXEC_FUNDER=<deposit wallet address>
POLY_EXEC_PRESIGN_PRIVATE_KEY=<EOA private key that owns the deposit wallet>
```

The `map_sdk_signature_type` function in `dispatch/mod.rs` maps integer 3 to `SdkSignatureType::Poly1271`. The funder validation in `sdk_exec.rs` accepts both `Proxy` (1) and `Poly1271` (3) with a funder address.

## Tennis Integration

Third sport alongside baseball and soccer. Currently supports French Open men's singles (`rolgar` league, BO5). Documentation in `docs/tennis/`.

### Scoring Hierarchy

Point → Game → Set → Match. Markets count **games** (not points). A 7-6 tiebreak set = 13 games. BO3 range: 12-39 total games. BO5 range: 18-65.

### Tennis-Specific Frame Extraction

`fast_extract_tennis_v1` handles nested JSON structure: top-level `homeScore`/`awayScore` = sets won, `currentPhase.homeScore`/`awayScore` = games in current set. The `phases` array is scanned to compute `total_games` (sum of all sets' games) and `first_set_games`. At match end, `currentPhase` is `null`. Dedup at game-level granularity (sets + games + freeText) — point-level changes are ignored since all markets resolve at game/set/match level.

### Match Format

Per-game `sets_to_win` (from `LEAGUES[league]["sets_to_win"]` in config): 2 for BO3 (regular ATP/WTA), 3 for BO5 (Grand Slam men's). Flows through `CompiledGamePlan.sets_to_win` → serialized JSON → Rust `NativeTennisEngine.sets_to_win[GameIdx]`.

### Market Types

6 tennis market types: `tennis_match_totals` (progressive over/under on total games), `tennis_first_set_totals` (progressive during set 1), `tennis_set_totals` (over/under on sets played), `moneyline` (match winner), `tennis_first_set_winner`, `tennis_set_handicap` (like spreads on set margin).

### Timeliness Optimizations

- **Match totals OVER**: progressive, fires on each game crossing (same as soccer totals)
- **First-set totals OVER**: progressive during set 1, fires on set-completion frame (no `!first_set_completed` guard — phases[0] freezes naturally)
- **Set totals OVER**: fires at guaranteed certainty using `min(sets_home, sets_away) >= N + 1 - sets_to_win`, not when the set actually completes
- **Set handicap not_covers**: fires mid-match when `max_margin + line <= 0` (favored player can no longer cover even if they win). `covers` waits for match end.
- **Moneyline, set handicap covers**: fire when `sets_home >= sets_to_win || sets_away >= sets_to_win` (match decided), not waiting for `freeText = "Ended"`. 45-159ms edge in 13% of matches.
- **All under lines and set handicap at match end**: fire on `freeText = "Ended"` only

### Cold-Start Protection

`prev_total_games`, `prev_first_set_games`, `prev_total_sets` are `Option<i64>` — `None` on first observation (detected via `rows[gi].is_none()`). Progressive evaluators skip entirely when prev is `None`. Event-based evaluators (first-set under/winner) have tombstones set on first observation when the event has already occurred.

### Set Handicap Side Detection

The slug determines the favored side, not `outcome_index`. Slugs contain `-handicap-home-` or `-handicap-away-` (~49% are away-favored). Compiler uses slug to produce `home_covers`/`home_not_covers` or `away_covers`/`away_not_covers`. Each sport's `SpreadSlot` is defined independently (tennis owns its own, no import from soccer).

### Player Mappings

Tennis uses player names instead of team names. `config/tennis_mappings.py` contains `PLAYER_MAP_*` dicts with Kalstrop V1 aliases (`"Last, First"`), V2 aliases (`"First Last"`), and PM aliases. PM code derivation: last word of PM full name, lowercased, truncated to 7 chars. Same-surname disambiguation uses shorter truncation (e.g., two Cerundolo brothers).

### Sport Isolation

Each sport's frame pipeline owns its own completion detection — inlined `free_text.trim().eq_ignore_ascii_case("Ended")` per pipeline. No shared `parse_common.rs`. No cross-sport type imports. Each sport's engine, types, and frame pipeline are fully self-contained. Adding or modifying one sport cannot silently affect another.

## Python Cleanup Audit

Systematic removal of dead code from the Python control plane. Guiding principle: Python no longer parses scores (Rust does all of it). Python's role is catalog sync, linking, compilation, and orchestration only.

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Sports providers (`sports/`) | Done — deleted all Python streaming code, provider classes are now catalog-only adapters. `boltodds.py` reduced from 1130→328 lines, `kalstrop_v1.py` from 1040→610 lines. Deleted `recorder.py`, dead contract types (`StreamEnvelope`, `ScoreUpdateEvent`, etc.). Removed `sport_key`/`league_key` from `ProviderGameRecord`. |
| 2 | CLI layer (`_cli/`) | Done — consolidated to 6 commands. Removed `--provider` from link/hotpath commands (derived from league config). Removed `--auto-approve` (review is opt-out). Provider sync defaults to all providers. |
| 3 | Linking layer (`linking/`) | Done — `build_links_multi` processes all leagues in one `run_id`. Deleted `SnapshotBuilder`, `report()`. Added doubleheader dedup. Review is opt-out: only rejected games excluded from plan. |
| 4 | Data layer (`data/`) | Done — deleted payload artifacts, dead DB methods, dead tables. Renamed `when_raw_et` → `when_raw`. Added `league` column to `link_runs`. Added `run_id` to `link_event_bindings`. Added `idx_pm_events_league_date` index. |
| 5 | Hotpath orchestration (`hotpath/`) | Done — deleted replay system, dead Protocol classes, `NativeMlbEngineBridge`, dead attributes. Removed MLB-only gate. Expanded `CANONICAL_MARKET_TYPES` for soccer. Fixed incremental refresh market type normalization. |
