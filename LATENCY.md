# Rust Native Hotpath Latency Audit

Date: 2026-04-27

Scope: `native/polybot2_native/src`, with spot checks of `polymarket-client-sdk 0.4.4` and Tokio behavior where they directly affect the native hotpath. This is a source-level latency audit, not a measured benchmark report.

Standard: this code is assumed to operate in a competitive order race. The target is not merely "fast"; the target is to make the score-frame-to-order-dispatch path difficult to beat. A 20 us frame-receipt-to-dispatch budget is only an acceptable baseline. The remaining recommendations below are judged against that standard.

## Current Hotpath Shape

HTTP mode currently has the right high-level split:

```text
WS thread:
  ws.next()
  -> process_decoded_frame_sync()
  -> serde_json borrowed Kalstrop frame parse
  -> parse_tick_from_kalstrop_update()
  -> NativeMlbEngine::process_tick()
  -> DispatchHandle::pop_for_target()
  -> DispatchHandle::send_batch()
  -> tick logging after handoff

Submitter thread:
  submit_rx.recv().await
  -> opportunistic cross-frame coalescing
  -> SDK post_order/post_orders
  -> outcome logging
```

Important strengths already present:

- No Python on the live tick path.
- The WS thread does not await CLOB HTTP responses.
- `SdkConfig::builder().use_server_time(false)` is used in `dispatch/sdk_exec.rs:92`, so the prior hot `GET /time` preflight is gone.
- Plan evaluation is index-based (`GameIdx`, `TargetIdx`, `TokenIdx`) rather than live strategy-key formatting.
- `SubmitWork::Batch(SmallVec<...>)` preserves frame-level batching before submitter-side coalescing.
- Tick logging is after HTTP-mode order handoff in `replay.rs:54-75`.
- Dedup/cooldown uses an `Instant`-derived worker clock in `ws.rs:419`.

Validation run:

```text
cargo test --manifest-path native/polybot2_native/Cargo.toml --lib
  50 passed

cargo check --release --manifest-path native/polybot2_native/Cargo.toml
  passed
```

Both commands still warn that `GameTargets::all_target_indices` is dead code.

## Findings

### P0: Successful HTTP-Mode Handoff Allocates Before Every Channel Send

Evidence:

- `replay.rs:36-55` builds a frame batch and calls `dispatch_handle.send_batch(batch, log)`.
- `dispatch/flow.rs:80-84` collects `target_idxs: Vec<TargetIdx>` before `tx.send(...)`.
- That allocation happens on every non-empty successful HTTP-mode batch, before the WS thread hands the order to the submitter.

Impact:

This is the cleanest current violation of the intended "no heap allocation before handoff" invariant. It is especially wasteful because the allocation exists only to log a rare send failure. The common successful path pays for failure diagnostics.

Recommendation:

- Send first.
- If send fails, recover the batch from `SendError<SubmitWork>` and iterate the returned payload for logging.
- This removes the `Vec<TargetIdx>` allocation without weakening diagnostics.

Expected shape:

```rust
match tx.send(SubmitWork::Batch(batch)) {
    Ok(()) => {}
    Err(err) => {
        if let SubmitWork::Batch(batch) = err.0 {
            for (target_idx, _) in batch {
                // resolve and log
            }
        }
    }
}
```

### P0: The Submitter Handoff Uses A General Tokio Unbounded Channel And A Sleeping Receiver

Evidence:

- `runtime.rs:139-140` creates `tokio_mpsc::unbounded_channel::<SubmitWork>()`.
- `dispatch/types.rs:59` stores `UnboundedSender<SubmitWork>`.
- `dispatch/submitter.rs:53-58` waits on `submit_rx.recv().await` for the first batch.
- Tokio's MPSC implementation stores messages in linked-list blocks; Tokio's own source documents block-based storage in `tokio/src/sync/mpsc/mod.rs:92-100`.

Impact:

The channel is non-blocking from the WS thread's perspective, but it is not a race-grade handoff primitive. The end-to-end path from message receipt to actual order POST start includes:

- channel bookkeeping and atomics,
- occasional channel block allocation/reclamation,
- a cross-thread wake,
- OS scheduler latency before the submitter resumes,
- then SDK request construction before bytes are written.

That wake/schedule leg alone can consume a meaningful fraction of a 20 us budget, and its tail can exceed the budget under normal Linux scheduling noise. This is now likely one of the largest remaining local-process costs if "dispatch" means bytes leaving toward the CLOB, not merely enqueueing work.

Recommendation:

- Replace the Tokio unbounded channel in the production submit path with a preallocated bounded SPSC queue or a bounded MPSC queue sized to the maximum acceptable in-flight work.
- Use non-blocking `try_push`/`try_send`; fail closed on saturation.
- Run the submitter on a dedicated pinned thread with an adaptive spin/yield/park loop during active game windows.
- Attach enqueue monotonic time and a hard deadline/max age to each order; drop stale work before posting.
- Keep Tokio for the HTTP client if needed, but do not make the first submitter wake depend on `recv().await` parking latency during active trading windows.

### P1: The Live Tick Representation Still Owns And Clones Fixture IDs

Evidence:

- `Tick` owns `universal_id: String` in `lib.rs:117-128`.
- `parse_tick_from_kalstrop_update` allocates `update.fixture_id.to_owned()` in `parse.rs:24-25`.
- `process_tick` immediately clones that string again in `engine.rs:344-345`.
- The lookup uses `game_id_to_idx.get(&game_id)` in `engine.rs:356`.
- `TickResult` owns `game_id: String` in `lib.rs:171-175`, mainly so post-handoff tick logging can use it.

Impact:

Every live frame with a fixture ID pays at least one heap allocation before evaluation, and the current `process_tick` adds a second string clone before dispatch. This affects both action and no-action material frames. The default `HashMap` lookup also hashes a variable-length string in the pre-dispatch path.

Recommendation:

- Split the live path from the PyO3/replay convenience path.
- For live Kalstrop frames, resolve `fixture_id: &str` directly to `GameIdx` before building any owned `Tick`.
- Keep `fixture_id` borrowed through the decoded-frame function only for tick logging after handoff.
- Change `TickResult` to carry `GameIdx` plus an optional borrowed/log-only fixture ID, or have `process_decoded_frame_sync` log from the original frame after dispatch.
- Consider `hashbrown` plus `ahash`/`rustc_hash` for `game_id_to_idx` if provider IDs are trusted, or another precomputed/runtime-perfect map if the active subscription set is small.

### P1: Intermediate `Vec` Plumbing Still Sits Before Handoff

Evidence:

- `process_kalstrop_frame` always returns `Vec<TickResult>` in `engine.rs:621-643`.
- JSON array frames deserialize into `Vec<KalstropFrame<'_>>` before contained frames are processed in `engine.rs:629-635`.
- `process_tick` allocates `raw_intents: Vec<_>` and `emitted: Vec<_>` in `engine.rs:378-393`.
- Each evaluator returns a fresh `Vec<RawIntent>` in `eval.rs:15-155`; NRFI's one-intent success path returns `vec![...]` at `eval.rs:91-101`.
- `replay.rs:37-55` makes a second pass over `results` to pop presigned orders and send the batch.

Impact:

The common single-frame case still routes through generic heap-backed collections. More importantly, if Kalstrop sends an array frame, no contained update can dispatch until the entire JSON array has been deserialized and collected. That is correct but not optimal for "first actionable update wins" latency.

Recommendation:

- Convert `process_kalstrop_frame` to a sink/callback style API that emits each decoded update immediately.
- For array frames, use `serde_json::Deserializer` streaming rather than collecting `Vec<KalstropFrame>`.
- Have evaluators push into a caller-provided `SmallVec<[RawIntent; 4]>` or fixed worker-local buffer.
- Build and send the `SubmitBatch` during the same streaming pass, then log after handoff.

### P1: Submitter-Side SDK Request Construction Is Still In The Race

Evidence:

- `dispatch/sdk_exec.rs:185-190` calls SDK `post_order`.
- `dispatch/sdk_exec.rs:231-254` calls SDK `post_orders`.
- In `polymarket-client-sdk 0.4.4`, `post_order` formats the URL, serializes JSON via `.json(&order)`, builds the request, creates L2 headers, then sends (`clob/client.rs:1476-1484`).
- `post_orders` does the same for batches (`clob/client.rs:1496-1504`).
- `create_headers` now uses local `Utc::now().timestamp()` when server time is disabled, then computes L2 auth headers (`clob/client.rs:2091-2098`).

Impact:

The WS thread is no longer blocked by SDK work, but actual order dispatch still waits for request body serialization, URL formatting, header creation, HMAC work, and async client machinery after the submitter wakes. If the goal is frame receipt to order bytes leaving the process, this is on the critical path.

Recommendation:

- Benchmark SDK `post_order`/`post_orders` against a prepared raw HTTP path against a local mock CLOB.
- If material, add a narrow prepared-submit layer:
  - pre-serialize signed order bodies at presign time,
  - pre-build stable request pieces,
  - generate only the timestamp-dependent L2 headers at submit time,
  - reuse a warmed HTTP client/connection,
  - avoid `format!("{}order", host)` and serde body construction on each order.
- Keep `use_server_time(false)` and verify with packet capture that no `/time` request occurs before live submissions.
- Maintain a background clock-skew monitor because local timestamp correctness is now an operational dependency.

### P1: The Queue Can Submit Stale Orders After A Submitter Stall

Evidence:

- The submit channel is unbounded (`runtime.rs:139-140`, `dispatch/types.rs:59`).
- `SubmitWork::Batch` carries signed orders but no enqueue timestamp or deadline (`dispatch/types.rs:39-41`).
- `run_submitter_async` drains and posts whatever it receives in order (`dispatch/submitter.rs:52-77`).

Impact:

Unbounded non-blocking enqueue protects the WS thread, but it has the wrong failure mode for a race. If the CLOB client stalls, old orders can accumulate and be posted after the edge is gone. In this domain, dropping stale work is usually safer than submitting late work.

Recommendation:

- Add `enqueue_monotonic_ns` and `deadline_monotonic_ns` or `max_age_ns` to the prepared order/batch.
- Drop stale orders in the submitter before request construction.
- Expose queue depth, dropped-stale count, dropped-full count, and submitter lag in `health_snapshot()`.
- Use bounded capacity so overload fails closed instead of converting latency into unbounded risk.

### P1: Logging Is After Handoff, But Still On The WS Thread

Evidence:

- HTTP-mode order handoff happens before tick logging in `replay.rs:54-75`.
- `LogWriter::log_tick` gets wall time, formats JSON, escapes strings, and writes to a buffered file in `log_writer.rs:54-73`.
- The WS worker flushes the shared writer after each idle frame drain in `ws.rs:445-447`.
- Submitter outcome logging uses the same `Arc<Mutex<LogWriter>>` in `dispatch/submitter.rs:107-121`.

Impact:

This no longer delays the first handoff for an action frame, which is good. It can still delay the next WebSocket read after an action frame, and the shared mutex means submitter outcome logging can contend with WS tick logging. On bursts, this can add avoidable head-of-line delay.

Recommendation:

- Move all JSON formatting and file I/O to a dedicated log writer thread.
- Send compact fixed-field log structs over a bounded lossy queue.
- Flush on a timer or shutdown, not after every idle frame drain.
- Treat dropped observability events as preferable to delaying frame ingestion.

### P2: Submitter Coalescing And Chunking Can Add Schedule-Dependent Tail Latency

Evidence:

- The submitter receives one frame batch, then greedily drains immediately available batches while `combined.len() < MAX_BATCH_SIZE` in `dispatch/submitter.rs:60-75`.
- It extends by whole batches, so `combined` can exceed `MAX_BATCH_SIZE` if the next batch is large (`dispatch/submitter.rs:66-73`).
- Multi-order handling allocates `target_idxs` and `signed` vectors in `dispatch/submitter.rs:115-117`.
- More than 15 signed orders are chunked sequentially in `dispatch/sdk_exec.rs:267-270` and subsequent loop body.

Impact:

Cross-frame coalescing is throughput-friendly and does not intentionally wait, but it is schedule-dependent. If more than 15 orders are combined, later orders wait behind the first chunk response. This is mostly submitter-side latency, not WS handoff latency, but it affects actual CLOB arrival order.

Recommendation:

- Cap cross-frame extension by remaining capacity instead of appending an entire batch past 15.
- Keep the frame-preserving batch as the atomic unit, but avoid merging unrelated frames if doing so pushes any order behind a sequential second chunk.
- Use `SmallVec` or reusable buffers in the submitter for `target_idxs` and `signed`.
- Benchmark three policies against a local mock CLOB: no cross-frame coalescing, immediate coalescing up to 15, and short spin-window coalescing. Optimize for earliest accepted order and p99, not only total batch throughput.

### P2: `SmallVec<[SdkSignedOrder; 4]>` May Be Too Large To Move Around Inline

Evidence:

- `SubmitBatch` is `SmallVec<[(TargetIdx, SdkSignedOrder); 4]>` in `dispatch/types.rs:31-34`.
- SDK `SignedOrder` contains an order, signature, order type, owner, and post-only flag in the dependency source (`clob/types/mod.rs:478-484`).

Impact:

Inline capacity avoids a heap allocation for 1-4 intents, but it may also make each `SubmitBatch` large. Moving a large inline `SmallVec` through the channel can copy more stack/channel memory than a pointer-based representation would. This may still be the right tradeoff, but it should be measured instead of assumed.

Recommendation:

- Add a test/benchmark that records `size_of::<SdkSignedOrder>()`, `size_of::<SubmitBatch>()`, and the p50/p99 cost of moving one, four, and fifteen orders through the handoff path.
- If size is large, store presigned orders as boxed/preallocated prepared payloads and send small handles or `Box<PreparedOrder>` values through the queue.
- Avoid `Arc` unless refcount atomics benchmark acceptably; a moved `Box` gives ownership transfer without hot refcount updates.

### P2: The Presign Pool Uses `VecDeque` For A Depth-1 Common Case

Evidence:

- `DispatchHandle::new` initializes `presign_pool` as one `VecDeque` per token in `dispatch/presign_pool.rs:5-13`.
- The pool depth target defaults to one signed order per token, and `warm_presign_startup_into` fills per-token queues (`dispatch/presign_pool.rs:97-116`).
- `pop_for_target` does `pool.pop_front()` in `dispatch/flow.rs:40-45`.

Impact:

`VecDeque` is general and correct, but a one-shot pool with depth 1 does not need ring-buffer machinery. This is a small cost, but it is paid directly before handoff.

Recommendation:

- If depth remains 1, use `Vec<Option<PreSignedOrderData>>` indexed by `TokenIdx`.
- If depth can be greater than 1, consider `SmallVec<[PreSignedOrderData; 1 or 2]>` with `pop()` or a compact custom cursor.
- Keep the fail-closed miss behavior.

### P2: There Is No Native Latency Benchmark Harness

Evidence:

- `Cargo.toml` has no `criterion`, `iai-callgrind`, bench target, or release profiling setup (`native/polybot2_native/Cargo.toml:1-26`).
- Existing Rust tests validate behavior but do not track p50/p95/p99/p999 latency or allocation counts.

Impact:

The current source contains several plausible microsecond-scale wins. Without a stable benchmark harness, future refactors can regress the hotpath silently, and it is impossible to rank queue, parser, SDK, and representation changes by p99 impact.

Recommendation:

Add native benchmarks for:

- no-action single Kalstrop frame,
- one totals crossing,
- grand-slam multi-line crossing,
- NRFI fire,
- final moneyline/spread/under fire,
- frame array where the first contained update is actionable,
- presign pop only,
- process frame to channel handoff,
- channel handoff to submitter receive,
- SDK `post_order`/`post_orders` against a local mock CLOB,
- prepared raw submit against the same mock CLOB.

Track:

- p50/p95/p99/p999,
- allocations and allocated bytes,
- frame receipt to handoff,
- handoff to submitter receive,
- submitter receive to first byte written,
- stale/drop counts under artificial CLOB stalls.

### P3: Parser CPU Can Still Be Tightened

Evidence:

- Score parsing uses generic `str::parse` in `parse.rs:28-33`.
- `parse_period` scans the same short string multiple times with `contains_ascii_ci` in `parse.rs:102-135`.
- `is_completed_state` and `normalize_game_state` classify the same `event_state` separately (`parse.rs:180+` and the following function).

Impact:

These paths are now allocation-free except for fixture ID ownership, so they are not the first issue to fix. They still burn cycles on every live frame, including no-action frames.

Recommendation:

- Replace score `str::parse` with a tiny ASCII integer parser for one- and two-digit scores.
- Parse period text in one pass, returning inning number and half without repeated substring scans.
- Combine completion and normalized-state classification into one function so `event_state` is scanned once.

### P3: The WS Loop Uses Timer-Based Nonblocking Drains And Awaits Pong Sends Inline

Evidence:

- After the first read, the frame drain loop uses `tokio::time::timeout(Duration::ZERO, ws.next()).await` in `ws.rs:391-397`.
- Ping handling awaits `ws.send(Message::Pong(...))` inline in `ws.rs:426-428`.
- Subscription refresh and resubscribe work can run before entering the frame drain loop (`ws.rs:351-379`).

Impact:

These are tail-latency risks rather than the main action-frame cost. A zero-duration Tokio timeout still constructs/polls timeout machinery, and inline pong/resubscribe sends can delay subsequent frame processing if they coincide with score updates.

Recommendation:

- Replace zero-timeout drains with a direct poll/`now_or_never` style drain if feasible.
- Keep ping/pong and subscription control on a separate task or make inbound score frame processing biased over control traffic.
- During active games, apply subscription changes only after an explicit idle period unless correctness requires immediate resubscribe.

### P3: Release And Host Tuning Are Not Encoded

Evidence:

- `Cargo.toml` has no custom `[profile.release]` settings (`native/polybot2_native/Cargo.toml:1-26`).
- Deployment target is Linux EC2, but target CPU, LTO, allocator, and thread affinity are not encoded in the native crate.

Impact:

Once hot allocations are removed, compiler and host tuning matter. Defaults are reasonable for general services, not for a microsecond-sensitive race path.

Recommendation:

- Evaluate `[profile.release]` settings: `lto = "thin"` or `"fat"`, `codegen-units = 1`, and `panic = "abort"` if acceptable for the extension boundary.
- Build for the actual EC2 CPU class where possible (`-C target-cpu=...`) rather than generic x86_64.
- Pin WS and submitter threads to isolated cores during live windows.
- Consider a tuned allocator only after hotpath allocations are measured and reduced; allocator swaps should not be the primary fix for avoidable allocations.
- Keep chrony/NTP monitoring because `use_server_time(false)` makes local clock correctness part of order submission.

## Recommended Implementation Order

1. Remove the `target_idxs` allocation in `DispatchHandle::send_batch`.
2. Add native benchmarks for frame-to-handoff, handoff-to-submit, and SDK-submit paths before larger rewrites.
3. Replace the Tokio unbounded submit channel with a bounded preallocated queue plus stale-order deadlines.
4. Move the submitter to a pinned adaptive-spin receive loop during active windows.
5. Remove owned fixture IDs from the live parser/engine path and switch evaluator output to reusable `SmallVec`/fixed buffers.
6. Stream JSON array frames and dispatch contained actionable updates immediately.
7. Move logging to a dedicated non-blocking writer.
8. Benchmark a prepared raw HTTP submit path against SDK `post_order`/`post_orders`.
9. Revisit `SubmitBatch` representation, presign pool storage, parser single-pass classification, and release/host tuning with benchmark data.

## Target Invariants

For production HTTP mode, the successful action path should converge on these invariants:

- No Python on the live tick path.
- No network call on the WS thread.
- No `/time` request before `POST /order` or `POST /orders`.
- No log lock, file I/O, or JSON formatting before handoff.
- No heap allocation before handoff in the common one-intent frame.
- No string allocation before handoff.
- No unbounded queue growth.
- Stale work is dropped before request construction.
- Multi-intent provider frames are deterministically batched when `2..=15`.
- The submitter starts request construction without depending on ordinary scheduler wake latency during active game windows.
