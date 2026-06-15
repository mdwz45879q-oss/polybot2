# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

polybot2 is a sports-trading bot for Polymarket. Hybrid architecture: Python control plane for CLI, data sync, linking, and orchestration; Rust native hotpath via PyO3/maturin for low-latency score ingest → decision → order dispatch. Supports MLB (baseball), soccer (EPL, La Liga, Bundesliga, UCL), tennis (ATP, WTA — 34+ tournaments via consolidated registry), CS2 esports (full Rust hotpath with closed-form map winner detection), and MOBA esports (LoL + Dota2 — shared `NativeMobaEngine`, BoltOdds-only, maps-won evaluation). Five sports, four score data providers: Kalstrop V1 (Sportradar, WS, baseball + soccer + tennis + CS2), Kalstrop V2 (BetGenius, Socket.IO, soccer + tennis), BoltOdds (WS, broad coverage including esports — primary for MOBA), Kalstrop Opta (REST catalog, football + baseball — streaming pending for non-World-Cup). Multiplexed concurrent providers per league — configured in `LEAGUES[league]["provider"]` (string or list). Deployment: Linux EC2 (eu-west-1, c8gn.4xlarge).

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
| `baseball/` | Sport-specific: `engine.rs` (NativeMlbEngine, process_tick_live, merge_plan), `eval.rs` (totals, NRFI, walkoff, moneyline, spreads, F5 totals/winner/spreads, extra innings), `parse.rs` (inning parsing), `frame_pipeline.rs` (zero-alloc live path), `types.rs` (GameState, GameTargets, etc.) |
| `soccer/` | Sport-specific: `engine.rs` (NativeSoccerEngine), `eval.rs` (totals, three-way moneyline, BTTS, spreads, corners, halftime result, exact score with early NO), `parse.rs` (half parsing), `frame_pipeline.rs`, `types.rs` |
| `tennis/` | Sport-specific: `engine.rs` (NativeTennisEngine, 4 evaluators: set totals, moneyline, first-set winner, set handicap — match totals and first-set totals evaluators removed, those market types are retirement-pool-only), `frame_pipeline.rs` (includes `detect_retirement` for 4-string exact match), `types.rs` (includes `RetirementTarget`, `RetirementResult`). Per-game `sets_to_win` (2 for BO3, 3 for BO5). Own `SpreadSlot`. Separate retirement presign pool for 50/50 orders on retirement. Option A: set totals and set handicap fire only at match end (no mid-match guaranteed-certainty over or early not_covers). |
| `cs2/` | Sport-specific: `engine.rs` (NativeCs2Engine, process_tick_live, merge_plan), `eval.rs` (closed-form map winner via `map_winner()`, child moneyline with dual-signal detection, match moneyline, totals with guaranteed-certainty, map handicap with early not_covers — 40 tests), `frame_pipeline.rs` (uses `"Closed"` not `"Ended"` for match completion), `types.rs` (Cs2GameTargets, Cs2GameState, SpreadSlot). Per-game `maps_to_win` (2 for BO3, 3 for BO5). Own `SpreadSlot` (no cross-sport imports). Effective-maps optimization: calls `map_winner()` directly in `process_tick_live` (no dependency on `map_winner_resolved` flag) to fire match-end bets on round-13 tick for ALL maps, including those without child_moneyline markets (BO3 map 3, BO1). |
| `moba/` | Sport-specific: `engine.rs` (NativeMobaEngine — shared by LoL + Dota2), `eval.rs` (child moneyline via maps-won only, moneyline, totals with guaranteed-certainty, map handicap with early not_covers — 11 tests), `types.rs` (MobaGameTargets, MobaGameState, SpreadSlot). Simplified CS2 engine: no round-level data, no `map_winner()`, no effective_state. BoltOdds-only (no V1). |
| `kalstrop_v2_sio.rs` | Socket.IO/Engine.IO client for Kalstrop V2 (`SioConnection`, handshake, `subscribe`/`unsubscribe`, frame classification) |
| `kalstrop_v2_types.rs` | V2 frame extractor (`fast_extract_v2`): fixture_id, home/away scores, currentPhase. Prebuilt finders. |
| `kalstrop_v2_frame_pipeline.rs` | V2 soccer frame pipeline: extract → dedup → phase map → engine → dispatch |
| `ws_kalstrop_v2.rs` | V2 Socket.IO worker: reconnect loop, subscription management, frame drain |
| `ws_multiplexed.rs` | Multiplexed WS worker: manages V1+V2+BoltOdds connections in one `tokio::select!` loop. "Fastest wins" — whichever provider delivers a score change first triggers evaluation. Independent reconnection per provider with per-provider backoff timers (`v1_reconnect_at`, `v2_reconnect_at`, `bo_reconnect_at`). BoltOdds keepalive ping every ~60s detects dead connections. Rich disconnect logging includes which provider triggered and connection states. BoltOdds dispatch branches on `SportEngine::Baseball` vs `SportEngine::Soccer` for sport-specific frame processing. |
| `boltodds_types.rs` | Byte-level extractor for BoltOdds frames (`fast_extract_boltodds`). Extracts `game_label`, goals, corners, match period from raw JSON without serde. |
| `boltodds_frame_pipeline.rs` | BoltOdds soccer frame pipeline: extract → dedup → eval → dispatch. Uses integer-based dedup (goals + corners + period). |
| `boltodds_baseball_frame_pipeline.rs` | BoltOdds baseball frame pipeline: extract → serde fallback → engine tick → dispatch. Thin glue layer called per-frame from the WS worker. |
| `boltodds_baseball_types.rs` | Byte-level extractor for BoltOdds baseball frames (`BoltOddsBaseballExtract`) + serde fallback (`serde_extract_boltodds_baseball`). Resilient to field-reordering changes. |
| `boltodds_moba_frame_pipeline.rs` | BoltOdds MOBA (LoL/Dota2) frame pipeline: extract → serde fallback → engine tick → dispatch. Always passes `match_completed=false` (MOBA detects match end via `maps >= mtw`). |
| `boltodds_moba_types.rs` | Byte-level extractor for BoltOdds MOBA frames (`BoltOddsMobaExtract`) + serde fallback. Extracts `event` (game label), `teams.home.score`, `teams.away.score` from `new_play` frames. |
| `ws_boltodds.rs` | BoltOdds WS worker: plain WS connection (`?key=TOKEN`), subscribe by game labels, frame drain loop. Dispatches to `SportEngine::Soccer`, `Baseball`, or `Moba` per-frame. Simpler protocol than V1 (no GraphQL). Per-provider reconnection timers with exponential backoff. |
| `fast_extract.rs` | Byte-level extractor for Kalstrop V1 frames. `fast_extract_v1` (soccer/baseball): fixtureId, homeScore, awayScore, freeText, corners. `fast_extract_tennis_v1` (tennis): adds nested currentPhase fields (games_home/away, phase number) and phases array scan for total_games/first_set_games. `fast_extract_cs2_v1` (CS2): extracts fixture_id, maps_home/away, rounds_home/away, free_text, current_phase. No phases array scanning (unlike tennis). |
| `dispatch/flow.rs` | `DispatchHandle::pop_for_target(TargetIdx)` (sync, returns `Box<PreparedOrderPayload>` or err) and `send_batch(SubmitBatch, &log)` (sync, pushes one Batch onto the SPSC ring). `dispatch_intents(intents, handle, log)`: shared dispatch logic extracted from frame pipelines. `pop_for_target_retirement(TargetIdx)`: same as `pop_for_target` but reads from the separate retirement presign pool. Tennis frame pipeline routes moneyline intents to normal pool and 50/50 retirement intents to retirement pool via `normal_intent_count` boundary. |
| `dispatch/presign_pool.rs` | Presign pool indexed by `TokenIdx` (`Vec<SmallVec<[Box<PreparedOrderPayload>; 2]>>`). Depth is 1-2 per token (primary + optional secondary order). `PreparedOrderPayload` contains pre-serialized order JSON bytes (serialized once at presign time). `warm_presign_startup_into` signs + serializes orders per token at startup. |
| `dispatch/fast_submit_client.rs` | `FastClobSubmitClient`: custom HTTP client bypassing the SDK for `POST /order` (single) and `POST /orders` (batch). Caches decoded API secret, uses stack-based `itoa` + base64 for HMAC, sends pre-serialized order bytes directly. Two methods: `post_order_bytes_single` (single order) and `post_orders_bytes` (batch, via generic `send_json_post` helper). `warmup_connection()` pre-establishes TCP + TLS + HTTP/2 via `GET /time`. Configured with `tcp_nodelay(true)`, `connect_timeout(5s)`, `timeout(10s)`, `pool_max_idle_per_host(30)`, `pool_idle_timeout(None)`, HTTP/2 via ALPN. |
| `dispatch/sdk_exec.rs` | `OrderSubmitter::new`, `ensure_sdk_runtime_async`, `sdk_client_ref`, `signer_ref` (SDK init for presign signing). `sign_order_batch` (presign warmup). `map_post_response` helper. The SDK client is used only for order signing at startup/patch — not for HTTP submission. |
| `dispatch/submitter.rs` | `run_submitter_async`: spin-loop that pops from SPSC ring and calls `submit_batch_task` inline (no `tokio::spawn`). 3-tier dispatch: `len==1` (single `POST /order`, bare `.await`), `2..=15` (one `POST /orders` batch — 1 HMAC, 1 HTTP request), `>15` (chunked into groups of `MAX_CLOB_BATCH=15`, concurrent `join_all`). Uses `ChunkScratch` for reusable body/index buffers. No semaphore. Keeps CLOB connection warm via `CLOB_KEEPALIVE_INTERVAL` (120s) pings during idle — no order ever pays cold-start TLS. |
| `dispatch/types.rs` | `DispatchHandle`, `OrderSubmitter`, `SubmitWork`, `SubmitBatch`, `PreparedOrderPayload`, `SharedRegistry` (ArcSwap). `PreparedOrderPayload` stores pre-serialized order JSON bytes + `time_in_force: OrderTimeInForce` (carried through to log output). |
| `ws.rs` | Kalstrop V1 live worker: GraphQL WS connect, subscription management, frame drain loop. Uses `worker_clock_origin: Instant` for monotonic timestamps. Dispatches to sport-specific frame pipeline via `SportEngine` enum (`Baseball`/`Soccer`/`Tennis`/`Cs2`/`Moba` variants — Moba is a no-op since it uses BoltOdds only). Drains `patch_rx` at quiescent points for hot-patch application. |
| `runtime.rs` | PyO3 `NativeHotPathRuntime`: builds both halves at startup with shared `Arc<TargetRegistry>`, runs presign warmup, spawns submitter and WS threads, lifecycle (`start`/`stop`/`patch_plan`). Provider-based worker dispatch: spawns `ws.rs` (Kalstrop V1), `ws_kalstrop_v2.rs` (Kalstrop V2), `ws_boltodds.rs` (BoltOdds), or `ws_multiplexed.rs` (multi-provider) based on `provider`/`providers` in config. Sport engine selected from explicit `sport` field in plan JSON via `detect_sport_from_plan()` (accepts `"baseball"`/`"soccer"`/`"tennis"`/`"cs2"`/`"moba"`) — returns `Result`, crashes on unknown sport (no silent fallback). CPU core pinning via `core_affinity` for WS + submitter threads (`ws_core_idx`/`submitter_core_idx` in config). `health_snapshot()` exposes WS + submitter health. |
| `lib.rs` | Shared types (`GameIdx`, `TargetIdx`, `TokenIdx`, `OverLine`, `SpreadSide`, `Intent`, `RawIntent`), `SportEngine` enum (`Baseball`/`Soccer`/`Tennis`/`Cs2`/`Moba`), `PatchPayload`, `NativeHotPathRuntime`, config structs. Sport-specific types live in `baseball/types.rs`, `soccer/types.rs`, `tennis/types.rs`, `cs2/types.rs`, `moba/types.rs`. |
| `log_writer.rs` | Structured JSONL log (schema V2). Wrapped in `Arc<Mutex<LogWriter>>` and shared by WS thread (logs ticks) and submitter thread (logs order outcomes). `LogWriter` stores `sport: &'static str` set at construction. `log_tick` takes `gid: &str` + `&TickPayload` enum (`Baseball`/`Soccer`/`Tennis`/`Cs2`) — each variant carries sport-specific named score fields (`runs_home`/`goals_home`/`sets_home`/`maps_home`), league (`lg`), game state, and source provider (`src`). CS2 variant includes `maps_home`/`maps_away`, `rounds_home`/`rounds_away`, `current_map`. Order events include `gid`, `tif` ("FAK"/"GTC"/"FOK"), and are keyed via `gid_from_sk(sk)`. Startup event emits `v:2`, `sport`, `leagues` array. Connection events carry `src` (provider name). |

### Hot Path Pipeline

```
WS thread (per frame, zero-alloc live path):
  fast_extract_v1(frame_text)                        (byte-level scan, no serde)
  → extract fixture_id, scores, freeText, corners    (borrowed slices, no allocation)
  → engine.check_duplicate / process_tick_live(GameIdx, ...)  (single FxHashMap lookup, eval into SmallVec)
                                                      returns LiveTickResult { game_idx, intents: SmallVec<[Intent; 32]> }
  → process_decoded_frame_sync builds one SubmitBatch per frame:
        for each Intent:
            DispatchHandle::pop_for_target(target_idx)   (Vec index + std::mem::take, ~100ns)
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

The `tokens` vector is deduplicated by `token_id` at load time. A `TargetIdx` always references a `TokenIdx`; multiple targets pointing to the same token (rare in practice) share the single pool entry. This preserves the "one signed order per unique token" invariant — there is exactly one `SmallVec` slot per `TokenIdx`, regardless of how many targets reference that token.

### Integer-Indexed Evaluator

At plan load, `engine.load_plan` interns each `provider_game_id` → `GameIdx(u16)` and each target → `TargetIdx(u16)`. Per-game compact arrays drive evaluation:

- `over_lines: Vec<OverLine { half_int: u16, target_idx }>` — sorted by `half_int`. On a score change from `prev` to `now`, iterate and match `half_int in [prev, now)`. A line of 5.5 is stored as `half_int = 5`.
- `under_lines: Vec<OverLine>` — same shape, fired at game completion when `half_int >= total`.
- `nrfi_yes`, `nrfi_no`: `Option<TargetIdx>` — direct slot access.
- `moneyline_home`, `moneyline_away`: `Option<TargetIdx>` — direct slot access.
- `spreads: Vec<(SpreadSide, f64, TargetIdx)>` — small, iterated at game end.

Per-game state (rows, GameState, resolution flags) is stored in `Vec<...>` indexed by `GameIdx`. One-shot gating is enforced by the presign pool (`std::mem::take()` drains all pre-signed orders for the token), not by the engine — there is no `attempted` bitset. No cooldown or debounce logic exists — the presign pool is the sole gate against duplicate intents.

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
| **Child moneyline (CS2)** | Map number from slug suffix + team name in label, cross-validated against question text | `home`, `away` |
| **Map handicap (CS2)** | Slug (`-handicap-home-` / `-handicap-away-`) + question text determines favored side for both outcomes | `home_covers`, `home_not_covers`, `away_covers`, `away_not_covers` |
| **F5 winner (baseball)** | `_three_way_side_from_slug` (`-home`/`-away`/`-draw`) + outcome_index | `home_yes`, `home_no`, `away_yes`, `away_no`, `draw_yes`, `draw_no` |
| **F5 total (baseball)** | Label "Over"/"Under" or outcome_index fallback | `over`, `under` |
| **F5 spread (baseball)** | `_spread_side_from_slug` (`-f5-spread-home-`/`-f5-spread-away-`) + outcome_index | `home_covers`, `home_not_covers`, `away_covers`, `away_not_covers` |
| **Extra innings (baseball)** | outcome_index (0=yes, 1=no) | `yes`, `no` |

Slug helpers: `_three_way_side_from_slug`, `_spread_side_from_slug`, `_parse_exact_score_from_slug`. Polymarket codes looked up from `TEAM_MAP_*` / `PLAYER_MAP_*` via the mapping at compile time.

**Strategy keys** are self-describing for all market types:
- `{gid}:MONEYLINE:HOME_YES`, `{gid}:SOCCER_HALFTIME_RESULT:DRAW_NO`
- `{gid}:SPREAD:HOME_COVERS:-1.5`, `{gid}:SPREAD:AWAY_NOT_COVERS:-2.5`
- `{gid}:BTTS:YES`, `{gid}:TOTAL_CORNERS:OVER:8.5`
- `{gid}:EXACT_SCORE:1_1:YES`, `{gid}:EXACT_SCORE:ANY_OTHER:NO`
- `{gid}:TENNIS_MATCH_TOTAL:OVER:36.5`, `{gid}:TENNIS_FIRST_SET_TOTAL:UNDER:9.5`
- `{gid}:TENNIS_SET_TOTAL:OVER:3.5`, `{gid}:TENNIS_FIRST_SET_WINNER:HOME`
- `{gid}:TENNIS_SET_HANDICAP:HOME_COVERS:-2.5`, `{gid}:TENNIS_SET_HANDICAP:AWAY_NOT_COVERS:-1.5`
- `{gid}:CHILD_MONEYLINE:MAP1:HOME`, `{gid}:CHILD_MONEYLINE:MAP2:AWAY`
- `{gid}:MAP_HANDICAP:HOME_COVERS:-1.5`, `{gid}:MAP_HANDICAP:AWAY_NOT_COVERS:1.5`
- `{gid}:F5_WINNER:HOME_YES`, `{gid}:F5_WINNER:DRAW_NO`
- `{gid}:F5_TOTAL:OVER:4.5`, `{gid}:F5_TOTAL:UNDER:4.5`
- `{gid}:F5_SPREAD:HOME_COVERS:-0.5`, `{gid}:F5_SPREAD:AWAY_NOT_COVERS:-0.5`
- `{gid}:EXTRA_INNINGS:YES`, `{gid}:EXTRA_INNINGS:NO`

The Rust plan loader has `eprintln!` warnings on unhandled semantics — visible at startup. The serializer emits per-target `line` (required for exact scores where `market.line` is NULL).

**`hotpath compile --league <X>`** — dry-run that prints the compiled plan with team-resolved semantics. Flags `⚠️ UNKNOWN SEMANTIC`, `⚠️ GENERIC KEY`, and `⚠️ DUPLICATE SEMANTIC`. Run before live trading to verify the compiler.

### Decoupled Submitter

Two structs back the dispatch path:

- **`DispatchHandle`** (WS thread): `cfg`, `registry: Arc<TargetRegistry>`, `shared_registry: SharedRegistry` (ArcSwap), `presign_template_catalog: HashMap<String, SmallVec<[OrderRequestData; 2]>>`, `presign_templates: Vec<SmallVec<[OrderRequestData; 2]>>` (indexed by `TokenIdx`), `presign_pool: Vec<SmallVec<[Box<PreparedOrderPayload>; 2]>>` (indexed by `TokenIdx`, depth 0–2), `submit_tx: Option<rtrb::Producer<SubmitWork>>`. All methods are synchronous.
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
| `linking/` | Deterministic provider↔Polymarket matching, review workflows. `sport_raw` fallback in `_resolve_provider_game` for esports league resolution (when `league_raw` is empty). |
| `execution/` | Config container for Rust dispatch (no order methods — Rust handles all dispatch) |
| `hotpath/` | Plan compiler, native service adapter, incremental market refresh + game discovery |
| `hotpath/incremental.py` | Two incremental refresh functions: `discover_new_markets()` — targeted Gamma API fetch for known event IDs, diff against current plan, insert new market targets, return delta for hot-patch. `discover_new_games()` — refreshes provider catalog + PM events by league tag, runs incremental link (`build_links_incremental`) for newly discovered games, compiles via `compile_multi_league_plan`, returns delta for hot-patch. Controlled by `game_refresh_interval_seconds` in runtime policy (default 3600s for tennis, 0 = disabled). |
| `hotpath/order_policy.py` | `OrderPolicy` dataclass — sport-generic execution profile (amount, size, price, time-in-force). `market_overrides` dict for per-market-type sizing (e.g., smaller bets on exact score). `for_market_type()` resolves overrides. Supports dual-order via `secondary_*` fields — when configured, each intent fires two pre-signed orders (primary + secondary). |
| `hotpath/v2_resolver.py` | V2 fixture ID resolver: `build_pending_games`, `try_resolve_games` (polls V2 tournament fixtures, matches by teams+time), `compile_for_resolved_game` (single-game plan with fixture_id substitution). Detects finished games. |
| `sports/` | Provider catalog adapters: `KalstropV1Provider` (V1 REST catalog), `KalstropV2Provider` (V2 REST catalog), `KalstropOptaProvider` (Opta REST catalog), `BoltOddsProvider` (REST catalog, with esports label resolution via `_fetch_esports_pbp_labels()` from `/api/playbyplay/esports`). No Python-side streaming — all WS streaming is handled by Rust or standalone capture scripts. |
| `sports/kalstrop_v2.py` | Kalstrop V2 catalog discovery — REST hierarchy: sports → competitions → tournaments → fixtures |
| `sports/kalstrop_opta.py` | Kalstrop Opta catalog discovery — REST hierarchy: sports → competitions (numeric IDs) → fixtures. Covers football + baseball. Composite `"Name|ID"` encoding in `category_name`/`league_raw` columns. |
| `sports/kalstrop_auth.py` | Shared HMAC auth helper for V1, V2, and Opta REST + WS auth headers. `kalstrop_livestats_auth_query()` for LiveStats Socket.IO endpoint (no Bearer prefix). |
| `config/` | `live_trading.py` (execution policy with sport-family fallback — e.g., single `"tennis"` key covers all tennis leagues), `mappings.py` (league registry, provider aliases, league disambiguation via `PROVIDER_LEAGUE_COUNTRY`), `baseball_mappings.py` / `soccer_mappings.py` / `tennis_mappings.py` / `cs2_mappings.py` (team/player aliases per league). Tennis uses a consolidated `PLAYER_MAP_TENNIS` (one global dict for all players) + `TENNIS_LEAGUES` registry (one entry per tournament with V1/BoltOdds aliases). `mappings.py` and `live_trading.py` auto-expand from these — adding a tournament or player only touches `tennis_mappings.py`. PM code derivation rule: last word of PM name, lowercased, truncated to 7 chars. |
| `guardian/` | Overturn detection for soccer. `manager.py` (orchestrator integration), `tracker.py` (log tailing + WS + state), `overturn.py` (dual-signal detector), `executor.py` (sell/cancel). See Guardian section below. |
| `scripts/` | Standalone capture scripts for raw frame recording: `capture_kalstrop_v1.py`, `capture_kalstrop_v2.py`, `capture_opta.py`, `capture_multi.py` (multi-provider comparison with V1+V2+BoltOdds+Opta), `capture_boltodds.py`, `capture_boltodds_debug.py` (standalone BoltOdds debug with `--resolve-esports`), `capture_livestats.py` (multi-source: LiveStats Socket.IO + V1 + BoltOdds, dynamic subscription based on `start_ts_utc`), `build_capture_plan.py` (auto-generates `games.json` from DB, `--resolve-livestats` flag). Not part of the `polybot2` package — run directly. |

### FFI Boundary (Python → Rust)

Python `NativeHotPathService` calls Rust `NativeHotPathRuntime` via PyO3:
- `start(config_json, compiled_plan_json, exec_config_json)` — all configs use `deny_unknown_fields`
- `stop()`, `set_subscriptions(provider_subs: HashMap<String, Vec<String>>)`, `prewarm_presign(templates_json)`, `health_snapshot()`
- `patch_plan(plan_json, templates_json)` — hot-patch: signs new orders (GIL released), sends `PatchPayload` to WS thread via dedicated `patch_tx` channel. WS thread calls `engine.merge_plan()` (append-only), extends dispatch pool, rebuilds registry, stores into `ArcSwap` (submitter sees new registry on next batch).
- Compiled plan serialized via `serialize_compiled_plan()` in `native_engine.py`. Top-level fields: `provider`, `league`, `sport` (explicit: "baseball"/"soccer"/"tennis"/"cs2"), `run_id`, `games[]`. Per-game: `provider_game_id`, `kickoff_ts_utc`, `markets[]`, `alternate_provider_game_ids[]`, `sets_to_win` (tennis: 2=BO3, 3=BO5; CS2: reused as `maps_to_win`, 2=BO3, 3=BO5). Rust reads `sport` via `detect_sport_from_plan()` — unknown/missing sport is a hard error.
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

5. **One-shot intents:** Each token can fire at most once per session, enforced by the presign pool (`std::mem::take()` drains all pre-signed orders). Once drained, the SmallVec is empty and any repeat intent fails closed at dispatch time. There is no engine-side `attempted` bitset — the pool is the sole gate.

6. **Independent evaluation:** `evaluate_totals`, `evaluate_nrfi`, `evaluate_final` all run on every tick (not an if/else chain). Each takes `&mut self` only to update its own resolution flags (`Vec<bool>` writes); no string allocations.

7. **NRFI first-inning gate:** `nrfi_first_inning_observed: Vec<bool>` indexed by `GameIdx`. Late subscriptions (inning > 1) are permanently skipped. Ticks without inning data defer evaluation. Extra innings use `inning_number = Some(10)` as sentinel (walkoff `>= 9` fires correctly). Kalstrop freeText patterns: `"Extra inning top"`, `"Extra inning bottom"`, `"Break top EI bottom 9"`, `"Break top EI bottom EI"` — all map to `(Some(10), half)`. Regular inning: `"Break top 1 bottom 1"` = mid-inning break (bottom of 1st not yet played, first inning NOT over); `"Break top 2 bottom 1"` = first inning fully done (NRFI NO can fire here if total=0).

8. **F5 (first 5 innings) two-tier evaluation:** Same pattern as walkoff partial spreads. After top 5th, away's score is locked — home margin can only grow. **Tier 1 (mid-5th):** fire F5 winner (home leads only) + mathematically locked F5 spread sides. **Tier 2 (F5 completion, bottom 5th ends):** fire all remaining F5 winner/spread/under/over tokens. Presign pool prevents double-fire across tiers. Detection is provider-split: BoltOdds requires `outs==3 && inning==5 && top` OR confirming `period_detail` (`AT_MID_5TH_INNING`/`AT_BOT_5TH_INNING` for mid-5th, `AT_END_5TH_INNING` for completion); V1 uses `inning==5 && half=="bottom"` for mid-5th, `inning>=6` for completion. The bare `(inning==5 && half=="bottom")` condition is unsafe on BoltOdds because BoltOdds advances `state.inning` before the period string updates — an `AT_END_4TH` frame with `inning=5, top=false` would match prematurely. Cold-start gate: `f5_first_five_observed` — late subscriptions (inning > 5) permanently skip all F5 evaluation. F5 totals have no tie guarantee (F5 can end tied). Design document: `docs/baseball_f5_design.md`.

9. **Extra innings evaluation:** YES fires when game enters extras — BoltOdds primary (`outs==3 && inning==9 && bottom && home==away`), V1/BoltOdds fallback (`inning >= 10`). `evaluate_extra_innings_into` runs BEFORE `evaluate_walkoff_into` in both chains so YES fires before any game-end evaluator can fire NO on the same frame. NO fires alongside existing game-end evaluators, guarded by `has_extra_innings[gi] && !extra_innings_resolved[gi] && state.inning_number.is_some_and(|i| i <= 9)`. The inning guard blocks NO on V1 cold-start at "Ended" (`inning=None`) and on extras games (`inning >= 10`). The walkoff NO site additionally checks `!period_detail.contains("_EXTRA_")` as belt-and-suspenders. When game goes to extras then ends, game-end evaluators fire moneyline/spreads/unders but NOT extra_innings NO (resolved flag blocks). BoltOdds uses `AT_TOP_EXTRA_INNING`/`AT_MID_EXTRA_INNING`/`AT_BOT_EXTRA_INNING` period strings for all extra innings (no numbered inning in the period — the numeric `state.inning` field still carries the actual inning number 10, 11, etc.).

10. **BoltOdds period-based fallbacks:** BoltOdds sometimes skips the `outs=3` frame and jumps straight to a period transition (break frame). The break guard suppresses all evaluators on break frames. Three fallback evaluator calls run **after** the `if !is_break` block: `evaluate_f5_mid5_into` (catches `AT_MID_5TH_INNING`), `evaluate_f5_completion_into` (catches `AT_END_5TH_INNING` via `period_detail` parameter), `evaluate_nrfi_period_into` (catches `AT_END_1ST_INNING` with `total==0`). `MATCH_COMPLETED` is handled earlier by setting `match_completed: Some(true)` on the GameState. `BoltOddsBaseballRow` includes `is_break: bool` so break frames always pass dedup even when outs/inning/scores match the preceding active frame. Resolution flags prevent double-fire when both `outs=3` and the subsequent period transition are received. BoltOdds advances `state.inning` before the period string updates (captures show `AT_END_4TH_INNING` with `inning=5` and "limbo" frames like `AT_BOT_8TH` with `inning=9`); the walkoff evaluator and F5 mid-5th evaluator have provider-split detection guards to reject these frames on BoltOdds.

10a. **BoltOdds state-ahead invariant:** BoltOdds advances `state.inning` and clears `topOfInning` before the `matchPeriod` string updates. All break frames have `topOfInning=false` and `out=0`. The engine maps `topOfInning=false` to `inning_half="bottom"`, so between-innings frames masquerade as bottom-half frames. Evaluators that gate on `inning_half=="bottom"` (walkoff, F5 mid-5th) must validate via `period_detail` on BoltOdds frames (`state.outs.is_some()`) to avoid premature firing. `period_inning_number()` helper extracts the inning number from the period string for cross-validation; returns `None` for `_EXTRA_INNING` strings (no embedded number), causing the inning-match check to be skipped — safe because home can never lead at a live extras between-innings boundary.

11. **Zero-copy parsing:** The live WS path deserializes directly into borrowed `KalstropFrame<'a>` structs and extracts fields without constructing a `Tick` or allocating any strings. The `fixture_id` is looked up as `&str` directly from the serde struct.

12. **Submitter thread isolation:** The WS thread never owns or references the SDK client or HTTP client. Submitter ownership is established at startup; the SPSC ring is the only communication path. `ArcSwap<TargetRegistry>` is the shared registry pointer (updated atomically on patch, loaded per batch by submitter for log attribution).

13. **Frame-preserving batch:** `process_decoded_frame_sync` builds exactly one `SubmitWork::Batch` per material WS frame. All intents from the same frame ride together in one ring push. The submitter processes batches inline (strict serialized queue) via 3-tier dispatch: single order → `POST /order`, 2–15 orders → one `POST /orders` batch, >15 → chunked concurrent `join_all`.

14. **Monotonic clock:** Engine timestamps use `worker_clock_origin: Instant` set at WS-worker startup, sourced via `Instant::elapsed().as_nanos()`. Wall-clock (`now_unix_ns`) is used only for log timestamps and L2 auth headers — never for engine math.

15. **Pool sharing semantics:** The presign pool is indexed by `TokenIdx`, not `TargetIdx`. Two targets pointing to the same token share one queue entry. This is enforced structurally — `tokens` is deduplicated at plan load.

16. **Exact score early NO (slice approach):** Since soccer scores only increase, predicted scorelines become impossible mid-game. On each goal, only the newly-impossible "slice" is fired — not all currently-impossible slots. Home goal (`x-y → x+1-y`): fire NO on slots where `home_pred == x AND away_pred >= y`. Away goal (`x-y → x-y+1`): fire NO on slots where `away_pred == y AND home_pred >= x`. At full time: fire YES on matching score, NO on remaining unresolved slots (`home_pred >= home AND away_pred >= away AND NOT exact match`). `any_other_score` fires at full time only. `SoccerGameState` tracks `prev_home`/`prev_away` for delta detection. No per-slot tracking needed — the presign pool prevents double-fire.

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
polybot2 hotpath live --league cs2 --execution-mode live  # CS2 esports
polybot2 hotpath live --sport moba --execution-mode live  # LoL + Dota2 in one process
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

Soccer and baseball leagues can use multiple providers simultaneously ("fastest wins"). Configured via `LEAGUES[league]["provider"]` as a list (e.g., `["kalstrop_v2", "kalstrop_v1"]`). The Rust multiplexed worker (`ws_multiplexed.rs`) manages all connections in one `tokio::select!` loop with three branches: V1 (Kalstrop WS), V2 (BetGenius Socket.IO), and BoltOdds (plain WS). BoltOdds frame dispatch branches on `SportEngine::Baseball` (via `process_boltodds_baseball_frame_sync`) vs `SportEngine::Soccer` (via `process_boltodds_frame_sync`):

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

**Per-league order policies:** `HOTPATH_EXECUTION_POLICY`, `HOTPATH_RUNTIME_POLICY`, and `LIVE_BETTING_MARKET_TYPES` in `config/live_trading.py` support sport-family fallback. Lookup chain: `league_key → sport_family → hardcoded defaults`. A single `"tennis"` key covers all 27 tennis leagues; per-league overrides (e.g., different sizing for Grand Slams) take priority when present. The orchestrator resolves `sport_family` from `LEAGUES[league]["sport_family"]` in `mappings.py` and passes it to the lookup functions. Template generation and incremental refresh route each game's targets through its league's policy via `game.canonical_league`. Supports `market_overrides` per market type (e.g., smaller bets on exact score).

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

Two independent refresh loops run during a live session:

**Market refresh** (every `refresh_interval_seconds`, default 1800s):
1. `discover_new_markets_sync()` — fetches markets from the Gamma API for known event IDs only (5–20 targeted HTTP requests, ~2s)
2. Diffs against current plan by strategy_key — if no new targets, does nothing
3. `hotpath.apply_incremental_refresh()` — signs new presign orders (GIL released), sends `PatchPayload` to WS thread
4. WS thread applies `engine.merge_plan()` at a quiescent point — extends per-game arrays, rebuilds `Arc<TargetRegistry>`, propagates to submitter

**Game discovery** (every `game_refresh_interval_seconds`, default 3600s for tennis, 0 = disabled):
1. `discover_new_games_sync()` — refreshes V1 catalog via `load_provider_catalog()` (upsert, not full replace) + PM events via Gamma API `?tag={league_code}` per league
2. `build_links_incremental()` — runs full linker matching on new catalog games, appends to existing `run_id` (does not delete existing bindings)
3. `compile_multi_league_plan()` with `include_inactive=True` — recompiles all leagues, extracts delta
4. `apply_incremental_refresh()` + subscription update via `set_subscriptions()`

Key features:
- **Single run_id for the session** — no link rebuild, no run_id incrementing, observer keeps working
- **No blind windows** — hotpath processes frames continuously during refresh
- **`.env` auto-loading** — reads `.env` from the working directory at startup
- **No fired-key tracking needed** — the presign pool's one-shot gate (`std::mem::take`) prevents any target from firing twice within a session, and `merge_plan` deduplicates by strategy_key via `HashSet`.

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
- **Esports support:** `_ESPORTS_SPORTS` frozenset in `boltodds.py` identifies esports titles (CS2, Dota, LoL, Valorant). Esports use different labels from `/api/playbyplay/esports` (not `/api/get_games`). WS frames use `"event"` field instead of `"game"` for the game label. The Python provider's `load_game_catalog()` replaces standard esports labels with PBP labels automatically.

**Kalstrop Opta** — `stats.kalstropservice.com/api/v2/opta`. Same HMAC auth. Opta/Sportradar-backed. Covers football (EPL, La Liga, Bundesliga, UCL, MLS, World Cup) + baseball (MLB, NPB, KBO). Currently catalog-only for live streaming (pending World Cup activation).
- **REST catalog:** `/sports` → `/sports/{sport}/competitions` (numeric IDs) → `/sports/{sport}/competitions/{category_id}/{tournament_id}/fixtures`
- **Fixture IDs:** Colon-prefixed format (e.g., `2:7799988`). Must URL-encode the colon.
- **Provider resolution:** `/fixtures/{event_id}/providers?sport={sport}` → `providers.opta.running_ball.fixture_id` (required for Socket.IO streaming). `running_ball` only present for top-tier football; absent for baseball and lower-tier leagues.
- **Socket.IO streaming:** Product slug `opta-stats`. Events: `opta_subscribe`/`opta_unsubscribe`/`opta_message`. Subscribe payload: `{fixtureId, room: "stats"}`. Score format: `stats.score[].{contestantId, value}` (no home/away label — resolve via `/match` REST call).
- **Composite encoding:** `category_name` stores `"England|14"`, `league_raw` stores `"Premier League|102841"`. Parsing: `rsplit("|", 1)` → `(name, id)`.
- **Currently inactive for streaming** — Kalstrop confirmed Opta streaming only supports World Cup games for now.

Documentation: `docs/providers/kalstrop_v2/` covers auth, genius, and opta APIs. V1 docs: `docs/kalstrop_odds_v1.md`.

### Provider Latency Hierarchy (Empirical)

Measured from dual-capture recordings (same machine, simultaneous connections):

| Sport | Fastest | Edge vs runner-up | Notes |
|-------|---------|-------------------|-------|
| **Soccer (goals)** | V2 (BetGenius) | ~1.5s faster than V1 | V2 fires totals, exact score, BTTS first |
| **Soccer (halftime/end)** | V1 (Sportradar) | faster than V2 | V1 fires halftime result, moneyline, spreads first |
| **Tennis** | V1 (Sportradar) | ~14s faster than V2, ~50ms faster than BoltOdds | V2 uses a significantly delayed secondary feed; BoltOdds relays the same Sportradar feed with ~50ms hop |
| **CS2** | V1 (Sportradar) | 15–24s edge for map completion (round-level detection) | BoltOdds has round-level data too but relays ~15-24s later |
| **MOBA (LoL/Dota2)** | BoltOdds | 30–547s faster than V1 | V1 delays deciding map score until `freeText="Closed"`. BoltOdds reports immediately. |

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
- The old telemetry system (Unix DGRAM socket) was removed. Replaced by a structured JSONL log file (`log_writer.rs`, schema V2) shared via `Arc<Mutex<>>`. The `polybot2 hotpath observe` command reads the JSONL log file via `live_observer.py` and renders an in-place terminal scoreboard. Sport-aware: baseball shows `AWAY-HOME` with inning (`T3`, `B7`); soccer shows `HOME-AWAY` with half (`1H`, `HT`, `2H`, `FT`); tennis shows `HOME-AWAY` with set (`SET`); CS2 shows `HOME-AWAY` with map (`MAP`). Team abbreviations use Polymarket codes from `config/mappings.py`. Tick events carry per-side corners (`corners_home`/`corners_away`) for soccer and per-game league (`lg`) for all sports.
- Python canonical form for BTTS is `"btts"` (not `"both_teams_to_score"`). Must match the Rust `canonical_soccer_market_type` which also normalizes to `"btts"`.
- SDK config uses `use_server_time(false)` to avoid a `GET /time` round-trip before every order POST. Host clock must be disciplined with chrono/NTP on the deployment target.
- Multi-intent frames batch in `process_decoded_frame_sync` (one Batch per frame). The submitter processes batches inline (no spawn) via 3-tier dispatch: single order → `POST /order` (bare `.await`), 2–15 orders → one `POST /orders` batch (1 HMAC, 1 HTTP), >15 → chunked concurrent `join_all`. No semaphore. Empty `order_id` with `success: true` is treated as failure (`map_post_response`). Batch tradeoff: a deactivated token poisons its batch, but the dual-order case saves ~8ms vs individual submission.
- Parsing uses zero-allocation byte-level scanning (`eq_ignore_ascii_case`, byte accumulator for numbers) — no `to_lowercase()`/`to_uppercase()` heap allocations on the tick path.
- Final-game cleanup (`cleanup_completed_game_idx`) is deferred until after intents are selected, not during `evaluate_final`. `final_resolved_games[gi] = true` blocks re-evaluation immediately; cleanup runs in `process_tick` before returning. Cleanup clears only lightweight row data (`rows`, `game_states`, `nrfi_first_inning_observed`); completion tombstones (`totals_final_under_emitted`, `nrfi_resolved_games`) are preserved for the session to prevent duplicate emission from repeated final frames.
- Tests use temp-path `LogWriter`s (no actual log inspection in non-live tests).
- CS2 compiler error messages say "baseball/soccer/tennis" but accept "cs2" — cosmetic inconsistency, not a correctness bug.
- CS2 match completion uses `freeText = "Closed"` (not `"Ended"` like other sports). Each sport's frame pipeline owns its own completion keyword.

## Latency Optimization Roadmap

Target: single-digit microsecond end-to-end on the WS thread (frame available → bytes sent on the channel). Status:

1. **Strategy evaluation — DONE.** Plan compiled into `GameIdx`/`TargetIdx` integer indices and per-game arrays. Evaluation is array indexing (~10ns), zero `format!()`, zero `HashMap` lookups except the single `fixture_id → GameIdx` resolve.

2. **Decoupled submitter — DONE.** WS thread does sync `pop + send`; submitter thread does HTTP. WS-thread cost per intent is ~100ns presign pop + amortized channel send. SDK client lives only on the submitter.

3. **Frame-preserving batch — DONE.** `process_decoded_frame_sync` builds one `SubmitWork::Batch(SmallVec<[(TargetIdx, Box<PreparedOrderPayload>); 32]>)` per material frame. With dual-order presign, a frame with N intents produces up to 2N batch entries. Multi-intent frames always reach the CLOB as one logical group. Submitter processes batches inline — no cross-frame coalescing, no head-of-line blocking.

4. **Index-keyed payload + Arc<TargetRegistry> — DONE.** Pool indexed by `TokenIdx` (`Vec<SmallVec<[Box<PreparedOrderPayload>; 2]>>`, depth 0–2, `std::mem::take()` drains all orders). Channel payload is `(TargetIdx, Box<PreparedOrderPayload>)` — no string allocation on the WS success path. Strings reconstructed via the registry only at log time.

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

Python system that monitors the hotpath's positions and detects VAR goal overturns in soccer. Integrated into the hotpath orchestrator via `GuardianManager` — launches automatically for soccer leagues as a daemon thread. Mode follows `--execution-mode` by default, overridable with `--guardian-mode live|dry-run|off`. The standalone `polybot2 guardian watch --snapshot` CLI remains for one-shot log inspection.

### Architecture

```
guardian/
├── manager.py         # GuardianManager: orchestrator integration, daemon thread lifecycle
├── guardian_logger.py  # Self-contained JSONL logger (event log + price log, two files)
├── log_tailer.py      # JSONL log tail-follow (yields parsed events)
├── state.py           # In-memory state: TrackedOrder, GameState, ScoreEvent, OverturnAlert
├── tracker.py         # Main loop: tails log + WebSocket events + overturn detection
│                      # Game-ID normalization via _game_id_map (alternate → canonical)
├── clob_client.py     # Authenticated CLOB REST client (order queries, cancellation, sell orders)
├── polymarket_ws.py   # Polymarket user channel WS (real-time fill notifications)
├── market_ws.py       # Polymarket market channel WS (orderbook monitoring, no auth)
├── overturn.py        # Dual-signal overturn detector + VAR action detection
├── executor.py        # Automated sell/cancel execution
└── cli.py             # CLI: polybot2 guardian watch --snapshot (snapshot mode only)
```

### Guardian Logging

Self-contained JSONL logs in `guardian_logs/` directory (two files per session):
- `guardian_{ts}.jsonl` — events: session_start (full token→game/market mapping), score_change, order_attempted, order_filled, var_action, overturn_armed, overturn_confirmed, order_cancelled, position_sold. Low volume, human-scannable.
- `guardian_prices_{ts}.jsonl` — price snapshots every 1 second with best bid/ask for all tracked tokens. High volume, for programmatic analysis. Both files include the `session_start` header so each is self-contained.

### Orchestrator Integration

The `GuardianManager` (in `manager.py`) is created by the hotpath orchestrator after plan compilation for soccer leagues only. It:
- Requires `POLY_EXEC_*` credentials — raises `RuntimeError` on missing credentials (single log line, hotpath continues without guardian)
- Builds all dependencies (ClobClient, WS clients, OverturnDetector, OverturnExecutor, GuardianLogger) from env
- Computes `token_to_condition` and `game_id_map` from the compiled plan
- Subscribes to ALL plan tokens on the market WS at startup (not reactively on order fills)
- Runs `tracker.run_watch()` in a daemon thread with its own asyncio event loop
- Receives `update_plan(compiled_plan)` calls after incremental refresh or V2 resolution
- Stopped cleanly in the orchestrator's finally block

Game-ID normalization (`_game_id_map`) maps alternate provider game IDs (e.g., V2 fixture IDs) to the canonical game ID (used in strategy keys), fixing the V2 fixture-ID ↔ event-ID mismatch that caused orders to be orphaned from score data.

### Guardian V2 Compatibility (patch_plan)

Two ID spaces coexist for V2 games: the **prematch event ID** (PM event identifier, e.g., `7737375` — used as the strategy key prefix in Rust log order events) and the **V2 fixture ID** (BetGenius live ID, e.g., `13941517` — used in Rust log tick events). `_game_id_map` must map both to the same canonical ID so ticks and orders land in the same `GameState`. Without this, `triggered_by` is always `None` and overturn alerts are never armed.

The orchestrator must call `guardian.add_game_id_alias(prematch_event_id, fixture_id)` for EVERY V2-resolved game — both the startup game (first resolution) and subsequent patch games. The patch path (`commands_hotpath_runtime.py` ~line 595) calls `update_plan` + `add_game_id_alias`. The startup path (~line 563) must also call `add_game_id_alias` after `guardian.start()`.

`update_plan(game_plan)` updates `_token_to_condition`, `_game_id_map`, and `_token_to_sk`, and triggers a market WS reconnect for new tokens. Polymarket's market channel ignores mid-session subscription updates — only the initial subscription at connection time takes effect. `PolymarketMarketWS.request_reconnect()` forces a clean disconnect so the `run()` reconnect loop re-subscribes with the full token set.

`OverturnAlert.execution_in_progress` guards against duplicate sell/cancel from the 1s `check_confirmations` timer re-entering `_check_and_trigger` while async execution is in-flight. Cleared in `_on_execution_done` to allow retry on failure.

### Overturn Detection (Dual-Signal + VAR)

Two mandatory signals must BOTH confirm before acting:

1. **Score reversal** (from hotpath log ticks): score decreased and held for >10s. Disarms automatically if score restores within the window (wobble).
2. **Market activity resumption** (from Polymarket market channel WS): best bid on affected tokens drops below threshold (default $0.80).

**VAR detection** (from V2 `court.matchActions[]`): The Rust V2 extractor extracts `type`/`subType` from the first match action. `type: "Var", subType: "Goal"` = review started. `type: "VarEnded", subType: "NoGoal"` or `subType: "NoPenalty"` = goal/penalty overturned (instant Signal 1 confirmation, skips 10s hold). `type: "VarEnded", subType: "GoalAwarded"` or `"PenaltyAwarded"` = VAR confirmed the goal — disarms any armed alert. Logged as `var_action` events in the guardian log and passed to the detector via `on_var_action()`. Only top-tier V2 leagues emit VAR match actions; lower-tier leagues fall back to the 10s timer.

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

Third sport alongside baseball and soccer. Supports 34+ tournaments across ATP, WTA, and Challengers, managed via `TENNIS_LEAGUES` registry in `config/tennis_mappings.py`. All BO3 except `rgm` (BO5). Tournaments are added progressively as they appear in the V1 catalog — adding a new tournament is one dict entry in `TENNIS_LEAGUES` (no other files to change). Documentation in `docs/tennis/` and `tennis_upgrade.md`.

### Scoring Hierarchy

Point → Game → Set → Match. Markets count **games** (not points). A 7-6 tiebreak set = 13 games. BO3 range: 12-39 total games. BO5 range: 18-65.

### Tennis-Specific Frame Extraction

`fast_extract_tennis_v1` handles nested JSON structure: top-level `homeScore`/`awayScore` = sets won, `currentPhase.homeScore`/`awayScore` = games in current set. The `phases` array is scanned to compute `total_games` (sum of all sets' games) and `first_set_games`. At match end, `currentPhase` is `null`. Dedup at game-level granularity (sets + games + freeText, `InlineStr<32>` for freeText to cover retirement strings up to 30 chars).

### Match Format

Per-game `sets_to_win` (from `LEAGUES[league]["sets_to_win"]` in config): 2 for BO3 (regular ATP/WTA/Challengers), 3 for BO5 (Grand Slam men's). Flows through `CompiledGamePlan.sets_to_win` → serialized JSON → Rust `NativeTennisEngine.sets_to_win[GameIdx]`.

### Market Types

4 active evaluators + 2 retirement-pool-only types:
- **Active:** `moneyline` (match winner), `tennis_first_set_winner`, `tennis_set_totals` (over/under on sets played), `tennis_set_handicap` (spreads on set margin).
- **Retirement-pool-only (no evaluator):** `tennis_match_totals`, `tennis_first_set_totals` — tokens loaded for retirement 50/50 orders but V1 data unreliable for normal evaluation.

### Evaluation Timing (Option A — Match-End-Only)

Set totals and set handicap fire ONLY at match end to eliminate retirement risk. Mid-match firing (guaranteed-certainty over, early not_covers) is disabled.

- **Set totals OVER + UNDER**: both fire when `match_decided = match_completed || sets >= sets_to_win`. No mid-match guaranteed-certainty over.
- **Set handicap covers + not_covers**: both fire when `match_decided`. No mid-match early not_covers.
- **Moneyline**: fires when `match_decided` (sets reach threshold or `freeText = "Ended"`). 45-159ms edge over waiting for "Ended".
- **First-set winner**: fires once when `first_set_completed = true`.

### Retirement Handling

When a player retires mid-match, most markets resolve 50/50 ($0.50 per token). The tennis hotpath has a completely separate retirement code path from normal match completion. Reference: `tennis_upgrade.md`.

**Detection:** `detect_retirement()` in `tennis/frame_pipeline.rs` matches freeText against exactly 4 normalized strings:
- `"player 1 retired, player 2 won"` / `"player 2 won, player 1 retired"` → away wins
- `"player 2 retired, player 1 won"` / `"player 1 won, player 2 retired"` → home wins

No fuzzy matching, no regex. Player 1 = home, Player 2 = away.

**Separate paths:** When retirement is detected, `process_tick_live` calls `handle_retirement()` instead of the normal evaluators. The normal evaluators (`evaluate_set_totals_into`, etc.) are NEVER called on a retirement tick. After retirement, `final_resolved_games[gi] = true` is set and subsequent ticks early-return.

**What fires on retirement:**
1. **Moneyline** from the **normal presign pool** — winner determined from freeText (NOT set counts, since sets may be tied on retirement).
2. **50/50 orders** from the **retirement presign pool** — both sides of every eligible market at $0.49 limit. Eligible: set_handicap (always), set_totals (always, except O/U 3.5 in BO5 set 5), match_totals (always), first_set_winner (only if retirement during set 1), first_set_totals (only if retirement during set 1).

**Retirement presign pool:** Parallel pool on `DispatchHandle` (`presign_pool_retirement`, `presign_templates_retirement`, `presign_template_catalog_retirement`). Populated from Python via `prewarm_presign_retirement()`. Orders are presigned at startup with limit price from the `"retirement"` key in `HOTPATH_EXECUTION_POLICY`.

**Resolution rules on retirement:**

| Market | On retirement |
|---|---|
| Moneyline | Normal — opponent wins |
| First-set winner | Normal if set 1 complete; 50/50 if during set 1 |
| First-set totals | Normal if set 1 complete; 50/50 if during set 1 |
| Set handicap | Always 50/50 |
| Set totals | Always 50/50 (skip O/U 3.5 in BO5 set 5 — ambiguous) |
| Match totals | Always 50/50 |

### Cold-Start Protection

`prev_total_sets` is `Option<i64>` — `None` on first observation (detected via `prev.maps_home.is_none()` pattern). `process_tick_live` has a `final_resolved_games` early return at the top (matching CS2 pattern) to prevent re-processing completed/retired games.

### Set Handicap Side Detection

The slug determines the favored side, not `outcome_index`. Slugs contain `-handicap-home-` or `-handicap-away-` (~49% are away-favored). Compiler uses slug to produce `home_covers`/`home_not_covers` or `away_covers`/`away_not_covers`. Each sport's `SpreadSlot` is defined independently (tennis owns its own, no import from soccer).

### Player Mappings

Tennis uses player names instead of team names. `config/tennis_mappings.py` contains `PLAYER_MAP_TENNIS` — a single global dict shared by all tournaments — and `TENNIS_LEAGUES` — a registry of tournament metadata (PM league code, V1/BoltOdds aliases, `sets_to_win`). PM codes are globally consistent across tournaments (unique within gender, not globally — e.g., "jones" = Francesca Jones in WTA, Jack Pinnington Jones in ATP). PM code derivation: last word of PM full name, lowercased, truncated to 7 chars. Adding a player = one entry in `PLAYER_MAP_TENNIS` (covers all tournaments). Adding a tournament = one entry in `TENNIS_LEAGUES` (no player work unless a genuinely new player appears).

### Sport Isolation

Each sport's frame pipeline owns its own completion detection — inlined `free_text.trim().eq_ignore_ascii_case("Ended")` per pipeline (CS2 uses `"Closed"`, tennis also checks retirement via `detect_retirement`). No shared `parse_common.rs`. No cross-sport type imports. Retirement handling (`detect_retirement`, `handle_retirement`, `RetirementTarget`, `RetirementResult`, `pop_for_target_retirement`) is tennis-only — no retirement code in baseball, soccer, CS2, or MOBA modules.

## CS2 Integration

Fourth sport alongside baseball, soccer, and tennis. Full Rust hotpath with closed-form map winner detection. Currently supports CS2 (`cs2` league, BO3/BO5). Documentation in `docs/cs_map_win_condition.md`.

### Scoring Hierarchy

Round → Map → Match. **MR12 format:** 13 rounds to win regulation (first half 12 rounds, sides swap, second half until one team reaches 13). Overtime uses MR3: 3 rounds per OT half, first to 4 OT rounds wins. If OT ties at 3-3, another OT set begins. Winning totals form the sequence {13, 16, 19, 22, ...} (≡ 1 mod 3).

### Match Format

Per-game `maps_to_win` (from `(BON)` in PM event title): 2 for BO3, 3 for BO5. Missing BO format is a hard error — no silent default. Stored as `sets_to_win` in the compiled plan JSON (reusing the tennis field). Flows through `CompiledGamePlan.sets_to_win` → Rust `NativeCs2Engine.maps_to_win[GameIdx]`.

### Market Types

4 CS2 market types: `moneyline` (match winner — fires when `maps_home >= maps_to_win || maps_away >= maps_to_win`), `child_moneyline` (per-map winner with dual-signal detection), `totals` (maps played — progressive over, under at match end), `map_handicap` (map spread — covers at match end, not_covers fires mid-match when mathematically eliminated).

### Closed-Form Map Winner Condition

`map_winner()` in `cs2/eval.rs` detects regulation AND overtime wins from round scores alone — no state tracking needed:

```
let decided =
    (big == 13 && small <= 11)                            // regulation
    || (big >= 16 && big % 3 == 1 && d >= 2 && d <= 4);  // overtime
```

Why each clause is load-bearing: `big % 3 == 1` kills 17-15 (OT2 at 2-0, undecided); `d >= 2` kills 13-12 and 16-15 (1-0 in OT set); `d <= 4` caps OT margin (4-0 sweep max); `small <= 11` makes the regulation branch self-documenting. Verified against all 1,054 reachable scores with zero false positives.

### Dual-Signal Child Moneyline

Two detection signals for per-map winners:
- **Signal 1 (primary):** Round-level scores from `currentPhase` detect map winner via `map_winner()`. Fires 30–100s before the maps-won counter updates in `homeScore`/`awayScore`.
- **Signal 2 (fallback):** Maps-won increment in top-level scores. Catches cases where V1 skips the winning round frame entirely (jumps from pre-win to post-map state).

Both signals resolve to the same `GameIdx` and are gated by the presign pool — whichever fires first consumes the order.

### CS2-Specific Frame Extraction

`fast_extract_cs2_v1` in `fast_extract.rs`: extracts fixture_id, maps_home/away (from top-level `homeScore`/`awayScore`), rounds_home/away (from `currentPhase`), free_text, current_phase number. No phases array scanning (unlike tennis — CS2 doesn't need per-map round history). Match completion detected via `free_text = "Closed"` (not `"Ended"` like other sports). Dedup at round-level granularity (maps + rounds + freeText).

### Outcome Label Resolution

PM outcome_index 0/1 ordering is **inconsistent** across CS2 events — home is not always index 0. Resolved by label matching against team names from `TEAM_MAP_CS2`, cross-validated against the market's question text. For map handicap, `"home"` in the slug refers to the underdog (+1.5), not the favorite — the favored side is determined once from the question text and applied to both outcomes.

## MOBA Integration (LoL + Dota2)

Fifth sport family. LoL and Dota2 are both MOBA (multiplayer online battle arena) games with identical match structures — they share a single Rust engine (`NativeMobaEngine`). Uses BoltOdds as the sole provider (30-547s faster than V1 for map-won detection). Documentation in `lol_dota2_design_document.md`.

### Key Differences from CS2

- **No round-level data** — map winners come only from the `teams.home/away.score` counter in BoltOdds `new_play` frames. No `map_winner()` condition, no Signal 1, no effective_state.
- **BoltOdds-only** — V1 is too slow (delays deciding map until `"Closed"`). Single provider, standalone `ws_boltodds.rs` worker (not multiplexed).
- **`sport = "moba"`** — both LoL and Dota2 are leagues within the "moba" sport. `--sport moba` runs both in one process.
- **Child moneyline for all maps** — PM lists markets for maps 1-5 in BO5 (unlike CS2 which omits the deciding map). Unreached maps are never fired.
- **BoltOdds uses `"event"` field** (not `"game"`) for esports game labels.

### Market Types (same 4 as CS2)

`moneyline`, `child_moneyline`, `totals` (guaranteed-certainty for over), `map_handicap` (early not_covers). Strategy keys: `{gid}:MONEYLINE:HOME`, `{gid}:CHILD_MONEYLINE:MAP1:AWAY`, `{gid}:TOTAL:OVER:2.5`, `{gid}:MAP_HANDICAP:HOME_COVERS:-1.5`.

### Configuration

Leagues `"lol"` and `"dota2"` in `config/mappings.py` with `sport_family: "moba"`, `provider: "boltodds"`. Team mappings in `config/lol_mappings.py` and `config/dota2_mappings.py` (populated progressively). `(BON)` format parsed from PM event title (same as CS2).

## Python Cleanup Audit

Systematic removal of dead code from the Python control plane. Guiding principle: Python no longer parses scores (Rust does all of it). Python's role is catalog sync, linking, compilation, and orchestration only.

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Sports providers (`sports/`) | Done — deleted all Python streaming code, provider classes are now catalog-only adapters. `boltodds.py` reduced from 1130→328 lines, `kalstrop_v1.py` from 1040→610 lines. Deleted `recorder.py`, dead contract types (`StreamEnvelope`, `ScoreUpdateEvent`, etc.). Removed `sport_key`/`league_key` from `ProviderGameRecord`. |
| 2 | CLI layer (`_cli/`) | Done — consolidated to 6 commands. Removed `--provider` from link/hotpath commands (derived from league config). Removed `--auto-approve` (review is opt-out). Provider sync defaults to all providers. |
| 3 | Linking layer (`linking/`) | Done — `build_links_multi` processes all leagues in one `run_id`. Deleted `SnapshotBuilder`, `report()`. Added doubleheader dedup. Review is opt-out: only rejected games excluded from plan. |
| 4 | Data layer (`data/`) | Done — deleted payload artifacts, dead DB methods, dead tables. Renamed `when_raw_et` → `when_raw`. Added `league` column to `link_runs`. Added `run_id` to `link_event_bindings`. Added `idx_pm_events_league_date` index. |
| 5 | Hotpath orchestration (`hotpath/`) | Done — deleted replay system, dead Protocol classes, `NativeMlbEngineBridge`, dead attributes. Removed MLB-only gate. Expanded `CANONICAL_MARKET_TYPES` for soccer. Fixed incremental refresh market type normalization. |
