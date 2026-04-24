# polybot2 Comprehensive Audit Report

**Date:** 2026-04-23
**Scope:** Full Python + Rust codebase audit
**Test baseline:** 15 Rust tests passing, 155 Python tests passing

---

## 1. Executive Summary

polybot2 is architecturally sound — the hybrid Python/Rust design cleanly separates control-plane orchestration from latency-sensitive execution, with well-defined FFI contracts and fail-closed semantics throughout. However, the package has **two critical bugs blocking live-readiness** (presign warmup timeout, observe mode broken), **two high-severity stability issues** (cancel-replace state leak, subscription-refresh warmup termination), and significant **test coverage gaps** in the most critical runtime paths. The dead code footprint from the Python-native hotpath era is moderate — several execution modules exist as config containers with unused execution logic. Fixing the two critical bugs and the two high-severity issues, together with targeted tests, would make the package live-ready for MLB v1.

**Finding count:** 2 CRITICAL, 3 HIGH, 4 MEDIUM, 3 LOW

---

## 2. Critical Findings

### F1: Presign Startup Warmup Timeout (CRITICAL)

**Symptom:** `warm_presign_startup_async()` times out at depth 4/8 under >10 active tokens with error `presign_startup_warm_timeout:target_depth=8 timeout_s=5.000`.

**Root cause:** Sequential signing across all presign keys with a fixed timeout.

**Code trace:**

1. `presign_pool.rs:89-125` — `warm_presign_startup_async()` loops calling `refill_presign_keys_async`
2. `presign_pool.rs:181-189` — `refill_presign_keys_async` iterates keys **sequentially**: `for key in keys { self.refill_presign_key_async(key, force).await? }`
3. `presign_pool.rs:164-166` — Each key's refill calls `build_signed_order_async` in a sequential loop (batch of 4)
4. `sdk_exec.rs:82-116` — Each `build_signed_order_async` does `market_order().build().await` + `sign(&signer, order).await` (~50-100ms per call)
5. With defaults (target=8, batch=4): 2 rounds per key × 4 signs each = 8 sequential SDK calls per token
6. 10 tokens × 8 signs × ~75ms = ~6 seconds — exceeds the 5s timeout
7. **No parallelism** across keys or within batches

**Secondary constraint:** `build_signed_order_async` takes `&mut self` (for lazy SDK init via `ensure_sdk_runtime`), and creates a new `SdkLocalSigner` from the private key string on every single call (`sdk_exec.rs:95-97`). After SDK init, the signing operations only need `&self`, so parallelization is structurally possible but requires extracting the signing logic to avoid the `&mut self` borrow.

**Callsites:**
- `runtime.rs:107` — startup (blocks Python main thread via `block_on`)
- `ws.rs:176` — subscription refresh in live worker (see F2)

**Fix approach:** Parallelize signing across keys using `tokio::task::JoinSet` or `futures::future::join_all`. Cache the signer instead of recreating from private key each call. Make timeout adaptive: `base_timeout + per_key_allowance × num_keys`.

**Acceptance check:** Warmup with 20 keys completes within adaptive timeout; per-key depth diagnostics in timeout error message.

---

### F2: Observe Mode Empty Scoreboard (CRITICAL)

**Symptom:** Scoreboard shows `tracked=0, live=0` despite active WS/exec connections.

**Root cause:** Startup ordering race + no state recovery mechanism.

**Code trace:**

1. **Rust telemetry worker** (`telemetry.rs:184`) creates an **unbound** `UnixDatagram` and sends to `/tmp/polybot2_hotpath_telemetry.sock` via `send_to`. If no listener is bound, `send_to` fails silently (`telemetry.rs:203: Err(_) => stats.inc_dropped()`).

2. **Python observe monitor** (`observe.py:585-599`) binds a DGRAM socket at the same path. It **deletes any existing socket file first** (`observe.py:591: os.unlink(path)`), then binds.

3. **If observe starts AFTER hotpath** (common operator workflow): all initial events (`ws_connected`, `exec_connected`, initial `game_state_changed`, `score_changed`) are already emitted and lost. The observe store only populates games from incoming events — no retroactive state.

4. **If observe restarts** while hotpath is running: `os.unlink(path)` destroys the socket the Rust worker is sending to. The Rust worker does not detect this — it keeps sending to the dead path, incrementing `dropped`.

5. **No heartbeat:** The Rust side never periodically re-emits current state. Score/game_state events only fire on **change** (`engine.rs:629-661`), so if no state changes after observe connects, nothing arrives.

6. **45-second minimum refresh** (`observe.py:24`) compounds the problem — even arriving events are not visible for up to 45 seconds.

**Additional factor:** `run_hotpath_observe` (`commands_hotpath_runtime.py:330-335`) is always a separate CLI process. No `--with-observe` flag exists on `hotpath run`.

**Fix approach:** Add a periodic `runtime_heartbeat` event (every ~30s) from the live worker loop emitting current subscription list, game states, and connection status. The observe store ingests this to bootstrap/refresh state regardless of start order. Alternatively, add `--with-observe` flag to embed the monitor as a daemon thread within `hotpath run`.

**Acceptance check:** Start hotpath, wait 10s, start observe — verify `tracked > 0` within 60s.

---

## 3. High-Severity Findings

### F3: Subscription Refresh Warmup Terminates Live Worker (HIGH)

**Symptom:** A runtime subscription change can kill a running live session.

**Code:** `ws.rs:152-179` — `refresh_active_subscriptions` calls `warm_presign_startup_async()` on line 176 whenever the active subscription set changes. If warmup fails (same timeout bug as F1), the error propagates to `ws.rs:231-258` where the worker returns (terminates), setting `health.running = false`.

**Impact:** During a 24h MLB session, subscription refreshes happen every 120 seconds. If token count is high at any refresh point, the entire live worker terminates with no recovery.

**Fix approach:** Decouple subscription refresh from blocking warmup. Instead, mark new tokens as `pending_refill` and let `refill_presign_tick_async()` fill them incrementally during the main frame loop. If warmup is needed, catch errors and log rather than terminating.

**Acceptance check:** Subscription change with 20 new tokens does not terminate live worker.

---

### F4: Cancel-Replace State Leak (HIGH)

**Symptom:** After a cancel-replace where cancel succeeds but re-submit fails, `active_orders_by_strategy` retains a stale entry pointing to an already-canceled exchange order.

**Code:** `flow.rs:202-336` — In `dispatch_intent_http_async`:
1. Line 220: `cancel_order_async(target_exchange)` returns `Ok(true)` — order canceled on exchange
2. Lines 229-237: Emits `order_cancel_called` event
3. Line 239: `submit_with_policy_async(&request)` fails
4. Line 280: Returns `Err(err)` — **without cleaning up `active_orders_by_strategy`**
5. Next intent for this strategy → finds stale entry → tries to cancel an already-canceled order → unnecessary delay

**Mitigating factor:** `refresh_active_state_from_broker_async` (`flow.rs:444-477`) polls the exchange on a 0.25s interval and self-heals stale entries by calling `mark_active_state` with the terminal status. So the gap is time-bounded (~250ms). Still, this creates noisy error telemetry and a one-cycle trade delay.

**Fix approach:** After successful cancel but failed submit, call `self.active_orders_by_strategy.remove(&strategy_key)` before returning the error.

**Acceptance check:** After cancel-ok + submit-fail, `active_orders_by_strategy` does not contain the stale strategy key.

---

### F5: Test Coverage Gaps for Critical Paths (HIGH)

**Missing tests:**

| Gap | Impact | Priority |
|-----|--------|----------|
| No presign warmup multi-key timeout test | F1 regression risk | With F1 fix |
| No end-to-end telemetry → observe pipeline test | F2 regression risk | With F2 fix |
| No cancel-replace failure recovery test | F4 regression risk | With F4 fix |
| No subscription refresh → warmup failure → worker termination test | F3 regression risk | With F3 fix |
| No engine decision logic unit tests (evaluate_totals/nrfi/final) | Decision correctness unverified | After critical fixes |
| No cooldown/debounce/one-shot filtering tests | Filter correctness unverified | After critical fixes |
| No engine state accumulation test | Memory leak unverified | Low priority |
| No full Rust runtime lifecycle integration test | Runtime behavior unverified | After critical fixes |

**Current coverage:**
- Rust: 15 tests — presign key shape, fail-closed miss, TIF enforcement, SDK side mapping, WS commands, subscription scheduling, telemetry filtering, JSON parsing
- Python: 155 tests — CLI, data sync, linking, review, hotpath compiler/launch gate/replay/observe, providers, execution imports, token resolution, schema

---

## 4. Medium-Severity Findings

### F6: Engine State Tracking Memory Accumulation (MEDIUM)

**Symptom:** 7 runtime state tracking maps/sets grow unbounded across a session.

**Code:** `engine.rs:21-45` — `NativeMlbEngine` fields:
- `attempted_strategy_keys: HashSet<String>` — one entry per unique strategy key that emits an intent (one-shot gate)
- `nrfi_resolved_games: HashSet<String>` — one per game with NRFI resolution
- `final_resolved_games: HashSet<String>` — one per completed game
- `totals_final_under_emitted: HashSet<String>` — one per game with under-final evaluation
- `rows: HashMap<String, StateRow>` — one per game ID seen
- `game_states: HashMap<String, GameState>` — one per game ID seen
- `last_emit_ns / last_signature: HashMap<String, _>` — one per strategy key

**Cleanup:** `reset_runtime_state()` (`engine.rs:48-57`) clears all, but is only called at startup.

**Practical impact for MLB:** ~15 games/day × ~4-6 markets each = ~200 strategy keys/day. Over a full season: ~2000 entries. Manageable but not scalable.

**Fix approach:** Add `prune_completed_games(active_game_ids: &[String])` method, call after subscription refresh.

---

### F7: NRFI Evaluation Semantic Mapping Needs Verification (MEDIUM)

**Concern:** In `engine.rs:773-807`, the NRFI evaluation buys the `"yes"` target when a run scores in the first inning, and the `"no"` target when the first inning completes with zero runs.

If the Polymarket NRFI market asks "Will there be no run in the first inning?":
- "Yes" token pays out when there IS no run → should buy when first inning completes with 0 runs
- "No" token pays out when there IS a run → should buy when a run scores

The code appears to do the **opposite** — buying "yes" when a run scores and "no" when no run scores. This is either:
1. An inverted market structure (the market asks "Will a run score?" rather than NRFI), or
2. An intentional semantic mapping in the compiled plan where `outcome_semantic: "yes"` = "yes, a run was scored", or
3. A bug

**Action required:** Verify the actual Polymarket NRFI market wording and the `outcome_semantic` mapping in the compiled plan against this evaluation logic.

---

### F8: Dead Code Inventory (MEDIUM)

The following Python modules contain execution logic that is **never invoked** in the native hotpath path. They remain instantiated as config/policy containers.

| Module | Classes | Status |
|--------|---------|--------|
| `execution/presign.py` | `PreSignWorker`, `PreSignedOrderPool`, `Eip712OrderSigner`, `NonceAllocator` | Instantiated in `FastExecutionService.__init__` but execution never invoked — Rust dispatch handles presigning |
| `execution/broker.py` | `ExecutionBroker`, `PolymarketExecutionBroker` | Instantiated in `FastExecutionService.__init__` — HTTP order dispatch handled by Rust `sdk_exec.rs` |
| `execution/user_channel.py` | `UserChannelListener` | Instantiated in `FastExecutionService.__init__` — order lifecycle events handled by Rust `flow.rs` |
| `hotpath/mlb/triggers.py` | `MlbTotalsTriggerEngine`, `MlbNrfiTriggerEngine`, `MlbFinalResultTriggerEngine` | Instantiated in CLI, but `NativeHotPathService.register_trigger_engine` only extracts `_order_policy` — decision logic lives in Rust `engine.rs` |
| `hotpath/mlb/reducer.py` | `MlbScoreStateReducer` | Passed to `set_state_reducer()` which is a no-op in native mode (`native_service.py:90-91`) |

**Note:** These are not orphaned — they're part of `FastExecutionService` which serves dual-purpose (config container for Rust + Python execution path). The Python execution path is itself unused but not broken.

---

### F9: `_address_hint` Parsed but Never Used (MEDIUM)

**Code:** `dispatch/mod.rs:52-55` — `_address_hint` reads from `exec_cfg.address` / `POLY_EXEC_ADDRESS` env var, but is never written to `DispatchConfig` or used anywhere.

Similarly, `native_service.py:225` passes `"address"` in the exec config payload, which Rust parses but discards.

---

## 5. Low-Severity Findings

### F10: HotPathConfig Stale Fields (LOW)

`contracts.py:21-49` — `HotPathConfig` contains 16 fields unused by the Rust native path:

`idle_sleep_seconds`, `ingest_queue_maxlen`, `route_queue_maxlen`, `sidecar_queue_maxlen`, `reconnect_max_sleep_seconds`, `reconnect_jitter_seconds`, `reconnect_min_interval_seconds`, `handshake_503_fail_threshold`, `handshake_503_cooldown_seconds`, `metrics_flush_interval_seconds`, `metrics_local_flush_every`, `profiling_max_samples`, `lifecycle_poll_interval_seconds`, `gc_tuning_enabled`, `affinity_cpu`, `latency_mode`

Only used by native: `dedup_ttl_seconds`, `decision_cooldown_seconds`, `decision_debounce_seconds`, `reconnect_base_sleep_seconds`, `native_engine_enabled`, `native_engine_required`, `run_scores`, `run_odds`, `profiling_enabled`, `read_timeout_seconds`

The stale fields have validation in `__post_init__` that runs on every instantiation, inflating the config surface unnecessarily.

### F11: `SdkLocalSigner` Recreated on Every Sign Call (LOW)

**Code:** `sdk_exec.rs:95-97` — `build_signed_order_async` creates a new `SdkLocalSigner::from_str(self.cfg.presign_private_key)` on every single call, parsing the private key string and setting chain_id each time. This should be cached after first SDK initialization.

### F12: `unknown_by_game` Counter Never Read Externally (LOW)

**Code:** `engine.rs:34` — `unknown_by_game: HashMap<String, i64>` tracks unresolved market targets per game. It is only read inside `evaluate_final` for the `unresolved=` diagnostic string. Never exposed to Python or telemetry. Harmless but adds noise to the struct.

---

## 6. Prioritized Implementation Backlog

| Priority | Finding | Severity | Effort | Fix Scope | Acceptance Check |
|----------|---------|----------|--------|-----------|------------------|
| 1 | F1: Presign warmup parallel signing | CRITICAL | Medium | `presign_pool.rs`, `sdk_exec.rs` | 20 keys warm within adaptive timeout |
| 2 | F3: Decouple subscription refresh from blocking warmup | HIGH | Low | `ws.rs:152-179` | Subscription change does not terminate worker |
| 3 | F2: Runtime heartbeat for observe | CRITICAL | Medium | `ws.rs` (emit), `telemetry.rs` (filter), `observe.py` (ingest) | Observe started after hotpath shows `tracked > 0` within 60s |
| 4 | F4: Cancel-replace state cleanup | HIGH | Low | `flow.rs:241-280` | `active_orders_by_strategy` empty after cancel-ok + submit-fail |
| 5 | F7: Verify NRFI semantic mapping | MEDIUM | Low | `engine.rs:757-807`, compiled plan | Confirmed correct against actual Polymarket market |
| 6 | F6: Engine state pruning | MEDIUM | Low | `engine.rs` | Tracking maps shrink after game completion |
| 7 | F11: Cache SdkLocalSigner | LOW | Low | `sdk_exec.rs:95-97` | Signer created once, reused |
| 8 | F10: Clean stale HotPathConfig fields | LOW | Low | `contracts.py` | Unused fields removed, tests pass |
| 9 | F8: Document dead code status | LOW | Low | Inline comments or removal | Clear intent documented |
| 10 | F9+F12: Remove `_address_hint` and expose `unknown_by_game` | LOW | Low | `dispatch/mod.rs`, `engine.rs` | Dead code removed |

**Implementation sequence:** F1 and F3 should be done together (same presign subsystem). F2 follows. F4 is a one-line fix. F7 is a verification task with no code change unless the mapping is wrong. Remaining items are cleanup.

---

## 7. Test Plan

### Tests to write alongside fixes:

**With F1 fix:**
- Rust unit test: warmup with 20 mock keys completes within adaptive timeout
- Rust unit test: warmup timeout produces per-key depth diagnostics
- Rust unit test: signer caching — only one signer creation per runtime

**With F2 fix:**
- Python integration test: start hotpath (noop mode), wait, start observe, verify `tracked > 0`
- Rust unit test: heartbeat event is emitted at configured interval
- Python unit test: ObserveStore correctly ingests heartbeat events

**With F3 fix:**
- Rust unit test: subscription change with warmup failure does not terminate worker
- Rust unit test: `refill_presign_tick_async` incrementally fills new tokens

**With F4 fix:**
- Rust unit test: after cancel-ok + submit-fail, strategy key removed from `active_orders_by_strategy`

**With F6 fix:**
- Rust unit test: `prune_completed_games` shrinks tracking maps
- Rust unit test: completed games not re-subscribed

### Regression suite (existing, verify green):
- `cargo test` — 15 Rust tests (all passing)
- `pytest tests/ --ignore=tests/live` — 155 Python tests (all passing)
