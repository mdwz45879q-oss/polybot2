# Rust Hotpath Latency Audit

Date: 2026-04-26

Scope: `native/polybot2_native/src`, with dependency behavior checked where it directly affects the Rust hotpath. This is a source-level latency audit, not a measured benchmark report.

The target standard here is not "fast enough"; it is "hard to beat in a competitive order race." Under that standard, the current architecture has good foundations, but there are several places where the live path still pays avoidable network, scheduling, allocation, and serialization costs.

## Executive Summary

The biggest issue is still in dispatch: `ensure_sdk_runtime` configures the Polymarket SDK with `use_server_time(true)` in `dispatch/sdk_exec.rs`. The SDK's authenticated `post_order` and `post_orders` paths call `create_headers`, and with this flag enabled they perform `GET /time` before submitting orders. The new batch path reduces multiple order submissions to one `POST /orders` request for up to 15 intents, but that request still pays a full CLOB round trip before the actual batch submission.

The batch implementation is a real improvement over the previous serial per-intent dispatch: `replay.rs` now gathers all intents from a decoded frame and calls `dispatch_orders_batch` when there are multiple intents. However, WebSocket reading, parsing, evaluation, HTTP submission, and logging still run through one awaited chain. When an intent fires, the frame drain stops until the single order or batch request returns. For more than 15 orders, chunks are submitted sequentially.

CPU-side latency is also meaningfully higher than it needs to be. The plan is compiled into string strategy keys, and the live evaluator allocates and formats strings on every probe. Parsing lowercases strings per tick, creates owned `String`s for IDs, and returns heap-backed `Vec`s between stages. These are small costs individually, but they are directly on the decision path.

## Current Strengths

- The live path stays in Rust after startup and avoids Python at tick time.
- Kalstrop frame structs borrow from the WebSocket frame and avoid a full JSON DOM.
- Presigned order construction removes ECDSA signing from the intended dispatch hotpath.
- Multiple intents in one decoded frame now use `POST /orders` through `dispatch_orders_batch`, up to the CLOB batch limit of 15 per request.
- The runtime disables Nagle for the Kalstrop WebSocket via `connect_async_tls_with_config(..., true, ...)`.
- Dispatch fails closed on presign misses and does not fall back to hot signing.
- Telemetry has been removed from the pre-dispatch path.

## Findings

### P0: Presigned Dispatch Still Pays an Extra CLOB Round Trip

Evidence:

- `native/polybot2_native/src/dispatch/sdk_exec.rs:57` builds the SDK config with `use_server_time(true)`.
- `native/polybot2_native/src/dispatch/flow.rs:40-43` calls `sdk.client.post_order(presigned.signed_order).await` for one intent.
- `native/polybot2_native/src/dispatch/sdk_exec.rs:199-202` calls `sdk.client.post_orders(signed_orders).await` for multi-intent batches.
- In `polymarket-client-sdk 0.4.4`, both `post_order` and `post_orders` build the request, call `create_headers`, and `create_headers` calls `self.server_time().await` when `use_server_time` is enabled.

Impact:

The fastest single-order live path is currently:

1. Pop presigned order.
2. `GET /time`.
3. Build L2 auth headers.
4. `POST /order`.
5. Await and parse response.

Step 2 is a full network round trip before the order is even sent. This likely dominates all Rust-side parse/eval costs and makes the presign optimization much less effective in a competitive race.

The new batch path improves the multi-intent case from roughly `N * (GET /time + POST /order)` to one `GET /time + POST /orders` for up to 15 orders. That is materially better, but the hot `/time` call is still the first avoidable network cost to remove.

Recommendation:

- Change SDK config to `SdkConfig::builder().use_server_time(false).build()`.
- Require host clock discipline with chrony/NTP and fail startup if clock skew is too large.
- If server time validation is still desired, run a background skew sampler off the hotpath and store an atomic offset. Do not fetch `/time` inside `post_order`.

Validation:

- Confirm with SDK tracing, packet capture, or a local mock CLOB that hot `dispatch_presigned_async` emits only `POST /order`, and hot `dispatch_orders_batch` emits only `POST /orders`, not `GET /time` followed by the submit request.
- Measure decision-to-first-byte-written before and after. This should be the largest single win.

### P0: Order Submission Still Blocks WebSocket Drain

Evidence:

- `native/polybot2_native/src/ws.rs:429-436` awaits `process_decoded_frame_async` inside the frame drain loop.
- `native/polybot2_native/src/replay.rs:25-40` awaits either `dispatch_order` for one intent or `dispatch_orders_batch` for multiple intents.
- `native/polybot2_native/src/dispatch/flow.rs:136-152` sends batch chunks sequentially when there are more than 15 signed orders.
- `native/polybot2_native/src/replay.rs:42-61` logs only after the awaited dispatch outcomes are collected.

Impact:

Once an action fires, no more Kalstrop frames are read by this worker until the single order or batch request returns. The batch path fixes the old "wait for order 1 before submitting order 2" issue for the common multi-intent case, but a second important provider frame still waits behind the full order/batch response latency. If more than 15 intents are ever emitted, the second chunk waits behind the first chunk response.

Recommendation:

- Split dispatch into `prepare_submission` and `submit_prepared`.
- On the WS/eval thread, pop the presigned order and mark the strategy attempted immediately.
- Hand the prepared signed order to a dedicated submitter task or thread and return to frame draining.
- Keep the new `POST /orders` path for `2..=15` intents if measurement confirms it wins. For more than 15, submit chunks concurrently unless exchange semantics or rate limits make sequential chunks preferable.
- Benchmark individual concurrent `POST /order` calls versus SDK `post_orders`; choose based on earliest accepted order and tail latency, not only total batch completion time.
- Keep fail-closed semantics: if the presign pop fails, do not sign on the hotpath.

Implementation sketch:

```text
WS thread:
  parse frame
  evaluate
  for each intent:
      prepared = dispatch_runtime.prepare_presigned(token_id)?
      submit_tx.try_send(prepared)
  enqueue minimal log event
  continue reading WS

Submitter:
  receive prepared signed order
  build L2 headers without /time
  POST /order or POST /orders
  send outcome to log writer
```

This preserves one-shot behavior while removing HTTP response latency from the score-ingest path.

### P1: Presigning Does Not Precompute the Full HTTP Submission

Evidence:

- `dispatch/presign_pool.rs` stores SDK `SignedOrder` values only.
- SDK `post_order` and `post_orders` still serialize JSON and create authenticated L2 headers on every order submission.
- With `use_server_time(true)`, it also fetches server time; after fixing that, header HMAC and JSON serialization remain hot.

Impact:

ECDSA signing is removed, but the hotpath still does request construction, serde JSON serialization, and L2 HMAC work before bytes are written to the socket. These are much smaller than a network round trip, but they matter in a race.

Recommendation:

- First fix `use_server_time`.
- Then benchmark whether SDK request construction is material.
- If it is, fork or wrap the SDK to support prepared submit requests:
  - pre-serialized signed order body,
  - pre-serialized signed order batch body,
  - precomputed request path and stable headers,
  - L2 auth generated from local timestamp with minimal allocation,
  - direct reuse of the underlying HTTP client.
- Consider precomputing L2 headers for the current and next second if Polymarket accepts second-level timestamps and local skew is tightly controlled. Validate carefully before relying on this.

### P1: Strategy Evaluation Is Allocation-Heavy

Evidence:

- `Tick`, `DeltaEvent`, `Intent`, and `TickResult` own `String`s in `lib.rs:36-97`.
- `parse_tick_from_kalstrop_update` allocates `fixture_id` with `to_owned()` in `parse.rs:24-26`.
- `process_tick`, `apply_delta`, and `update_game_state` clone game IDs at `engine.rs:265`, `engine.rs:353`, and `engine.rs:406`.
- `line_key` uses `format!` in `eval.rs:5-12`.
- Totals, NRFI, moneyline, and spread probes build strategy keys with `format!` in `eval.rs:35`, `eval.rs:54`, `eval.rs:97`, `eval.rs:109`, `eval.rs:155`, and `eval.rs:171`.
- The one-shot filter clones token IDs and strategy keys in `engine.rs:320-340`.

Impact:

The lookup strategy is algorithmically good, but the representation is not latency-optimal. On a scoring update, Rust is formatting strings and hashing variable-length keys before it can know whether to dispatch. On repeated non-action material ticks, it still allocates and mutates state.

Recommendation:

Compile the plan into integer IDs and per-game indexes:

- Intern `provider_game_id` once at startup and map borrowed frame IDs to `GameId(u32)`.
- Store `game_states` and `rows` in `Vec`s indexed by `GameId`, not `HashMap<String, ...>`.
- Replace strategy-key lookups with `StrategyId(u32)` and `TokenId(u32)`.
- Represent totals lines as integer half-runs, for example `line_2x = total * 2 + 1`, avoiding `f64` and `line_key`.
- Store `nrfi_yes`, `nrfi_no`, `moneyline_home`, and `moneyline_away` as direct `Option<StrategyId>` slots.
- Store spread and under targets as compact per-game arrays.
- Keep human-readable strategy keys only for post-dispatch logging or replay output.

Lower-effort interim changes:

- Use `SmallVec<[Intent; 4]>` or an equivalent fixed-capacity buffer for intents.
- Pre-reserve hot maps and sets after loading the plan.
- Consider `hashbrown` plus `ahash` or `rustc_hash` for internal maps if untrusted-input hash collision risk is acceptable.
- Move the one-shot attempted check before cooldown/debounce lookups, or remove cooldown/debounce from the live engine if one-shot remains the invariant.

### P1: Final-Game Cleanup Runs Before Final Orders Are Submitted

Evidence:

- `evaluate_final` builds final intents, then calls `self.cleanup_completed_game(uid)` before returning in `eval.rs:181-182`.
- `cleanup_completed_game` clones all strategy keys for the game and removes rows, states, and per-strategy tracking in `engine.rs:233-247`.

Impact:

For moneyline, spread, and final-under orders, cleanup work happens before dispatch starts. On games with many target keys, this adds avoidable work exactly when final-market orders need to leave immediately.

Recommendation:

- Mark the game final before dispatch, but defer cleanup until after submissions are handed to the submitter.
- Use a post-dispatch cleanup queue or perform cleanup on the next idle housekeeping pass.
- If `final_resolved_games` is enough to suppress duplicate final evaluation, the rest of the cleanup does not need to be on the pre-dispatch path.

### P2: Tick Parsing Allocates for Period and State Normalization

Evidence:

- `parse_period` lowercases the period text on every tick in `parse.rs:102-103`.
- `extract_first_number` collects digits into a `String` in `parse.rs:137-142`.
- `is_completed_state` and `normalize_game_state` allocate uppercase/lowercase strings in `parse.rs:150-219`.

Impact:

This is small compared with network latency, but it is paid on every live frame, including no-action frames. It also adds allocator noise and branch work before evaluation.

Recommendation:

- Replace `to_lowercase`, `to_uppercase`, and digit collection with ASCII byte scanning.
- Parse inning number directly into an integer accumulator.
- Use `eq_ignore_ascii_case` or byte-level state classification.
- Combine completion and normalized state classification so `event_state` is scanned once.
- Replace score `str::parse` with a tiny ASCII integer parser for common one- and two-digit scores.

### P2: Frame Processing Uses Heap-Backed Intermediate Vectors

Evidence:

- `process_kalstrop_frame` always creates a `Vec<TickResult>` in `engine.rs:537-559`.
- Batch frames deserialize into `Vec<KalstropFrame>` in `engine.rs:545-552`.
- Each evaluator returns a separate `Vec<Intent>`, and `process_tick` extends another `Vec` in `engine.rs:289-292`.
- `replay.rs:15-40` allocates `all_intents`, `token_ids`, and `order_outcomes` vectors before dispatch.
- `dispatch/flow.rs:87-138` allocates and moves through `signed_orders`, `early_errors`, `batch_indices`, `batch_orders`, and per-chunk vectors for batch dispatch.

Impact:

Single-frame live updates still pay generic vector setup, and action frames allocate multiple small vectors before dispatch. The new batch path adds more temporary vectors around multi-intent dispatch. For bursts, batch JSON is fully materialized before any contained update can dispatch.

Recommendation:

- Change `process_kalstrop_frame` into a streaming function that takes a callback/sink and emits results immediately.
- For JSON arrays, use a streaming `serde_json::Deserializer` rather than collecting all frames first.
- Replace evaluator-returned `Vec`s with a reusable per-worker `SmallVec` or callback-based intent emission.
- Fill a fixed-capacity `[Option<SignedOrder>; 15]`/`SmallVec` for the common batch case instead of building and draining heap vectors.
- Do not collect order outcomes on the WS thread if dispatch is moved to a submitter.

### P2: Logging Is After Dispatch, but Still on the Worker

Evidence:

- `process_decoded_frame_async` logs tick and order records after awaited single or batch dispatch in `replay.rs:42-61`.
- `LogWriter` uses `format!`, JSON escaping, and `writeln!` in `log_writer.rs`.
- `ws.rs:438` flushes after every frame-drain idle.

Impact:

Logging does not delay the first awaited order's request construction, but it does delay the worker's return to WebSocket reads. It can also delay no-action material ticks, and a full buffer can trigger a write syscall on the worker thread.

Recommendation:

- Move logging to a dedicated non-blocking writer fed by a bounded channel or lock-free ring.
- Log minimal fixed-field structs from the hotpath; format JSON in the writer.
- Flush on a timer or shutdown, not after every idle drain.
- Treat dropped observability logs as acceptable if the alternative is blocking order ingestion.

### P2: Subscription Refresh Can Still Interleave with Frame Readiness

Evidence:

- The event loop drains commands and may refresh/resubscribe before entering the frame drain loop in `ws.rs:323-378`.
- Refresh is mostly local, but active subscription changes perform WebSocket sends before frame reading resumes.

Impact:

The loop is designed to process all pending frames before housekeeping, and that is mostly true. However, if a frame arrives while refresh/resubscribe work is running, the frame waits. This should be rare, but it is another avoidable tail-latency source.

Recommendation:

- Keep subscription scheduling on a separate control task.
- Apply subscription changes only after an explicit idle period or with a biased `select!` that prioritizes inbound frames.
- Avoid any resubscribe send in the same task that owns score frame processing during active game windows unless strictly necessary.

### P2: There Is No Native Hotpath Benchmark Harness

Evidence:

- No `criterion`, `iai-callgrind`, or Rust benchmark target exists under `native/polybot2_native`.
- Existing tests validate behavior, not p50/p99/p999 latency or allocations.

Impact:

Without a stable benchmark, optimizations can regress silently. The current code has several plausible micro-optimizations, but they need measured ranking after the P0 network issue is fixed.

Recommendation:

Add benchmarks for:

- single no-action Kalstrop frame parse/eval,
- scoring frame that crosses one totals line,
- grand-slam multi-line totals crossing,
- NRFI fire,
- final moneyline/spread/under fire,
- presigned pop only,
- presigned submit against a local mock CLOB,
- presigned batch submit for 2, 4, and 15 orders against a local mock CLOB,
- presigned 16-order case to expose chunking cost,
- live-like burst with queued frames while dispatch is in flight.

Track:

- p50/p95/p99/p999 latency,
- allocation count and bytes,
- decision-to-submit enqueue,
- decision-to-first-byte-written for `POST /order` and `POST /orders` where measurable,
- head-of-line delay for second frame/order.

### P3: Time and Hashing Choices Are Conservative, Not Race-Optimized

Evidence:

- `now_unix_ns` uses `SystemTime::now()` per frame in `dispatch/mod.rs`.
- State maps use default `std::collections::HashMap` and `HashSet`.
- The runtime uses string keys for all state and strategy tracking.

Impact:

These are not the top bottlenecks, but they are easy to revisit after the larger architecture fixes. Default hashers and wall-clock calls are reliable, but not the fastest choices for internal hot state.

Recommendation:

- Use `Instant` or a TSC-backed clock for dedup timing; reserve wall time for logs.
- Use faster internal hashers or vector-indexed state after plan compilation.
- Keep externally supplied strings at the boundary and convert to compact IDs immediately.

## Recommended Implementation Order

1. **Remove hot `/time` call.** Set `use_server_time(false)`, add a startup/background clock-skew guard, and verify with packet capture that hot orders emit only `POST /order` or `POST /orders`.
2. **Decouple order submit from WS drain.** Pop presigned orders synchronously, hand prepared submissions to a dedicated submitter, and keep the new batch path off the frame reader's await chain.
3. **Defer final cleanup and logging.** Nothing after intent selection should run before the order has been handed to the submitter.
4. **Add native latency benchmarks.** Measure before changing representation so the remaining work is ranked by actual p99 impact.
5. **Replace string strategy keys in the live engine.** Compile to IDs and per-game arrays; keep strings only for logs and replay compatibility.
6. **Remove parsing allocations.** Byte-scan period/state/score fields and stream JSON arrays.
7. **Tune lower-level runtime choices.** Faster hashers, pre-reserved maps, `SmallVec`, clock source, and CPU/thread pinning once the larger bottlenecks are gone.

## Latency Target Invariants

Keep these invariants while optimizing:

- No Python on the live tick path.
- No live ECDSA signing fallback.
- No broker, telemetry, or logging work before order submission is handed off.
- No `/time`, tick-size, fee-rate, order-book, or other CLOB read before `POST /order` or `POST /orders`.
- One-shot/fail-closed semantics must survive dispatch decoupling.
- Correctness output can remain string-keyed outside the live critical path; the live path itself should not be.
