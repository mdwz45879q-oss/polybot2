# Pure Latency Audit — polybot2 Rust Hotpath

Source-level audit of `native/polybot2_native/src/`. Every file on the critical path
read line-by-line. Findings organized by severity.

Architecture under audit: two threads, one lock-free SPSC ring between them.

```
WS thread (frame → ring push):
  Message::Text received → Instant::elapsed() monotonic timestamp
  → fast_extract_v1 / fast_extract_boltodds (memchr Finders, borrowed slices)
  → check_duplicate (one FxHashMap lookup + InlineStr comparisons)
  → parse_period / parse_half + fast_parse_score
  → process_tick_live (SmallVec<[Intent; 32]>, stack-allocated)
  → pop_for_target (Vec index + Option::take on Box<PreparedOrderPayload>)
  → send_batch (rtrb::Producer::push, lock-free SPSC, capacity 64)
  → flush_tick_logs (log lock AFTER push — not on critical path)

Submitter thread (ring pop → HTTP):
  spin-loop: submit_rx.pop() (rtrb::Consumer, lock-free)
  → submit_batch_task (inline .await, no tokio::spawn)
  → FastClobSubmitClient: HMAC-SHA256 + pre-serialized body → reqwest → wire
```

---

## Tier 1 — Fix Now (confirmed waste on every tick)

### F-01: Heap allocation in `soccer/parse.rs:19` on every soccer tick

```rust
// soccer/parse.rs:14-31
pub(crate) fn parse_half(text: &str) -> &'static str {
    let s = text.trim();
    if s.is_empty() { return ""; }
    let lower = s.to_ascii_lowercase();  // ← HEAP ALLOC + DEALLOC per call
    if lower == "1st half" { return "1st"; }
    if lower == "halftime" || lower == "half time" || lower == "ht" { return "Halftime"; }
    if lower == "2nd half" { return "2nd"; }
    ""
}
```

`to_ascii_lowercase()` allocates a `String` (heap malloc + byte copy + dealloc) on every
invocation. Called once per material tick per game. With 10-20 concurrent soccer games,
that's 10-20 malloc/free cycles per frame burst — on the WS thread, inside the
zero-alloc pipeline that otherwise does zero heap work.

The function directly below in the same file already does it correctly:

```rust
// soccer/parse.rs:35-42
pub(crate) fn is_completed_free_text(free_text: &str) -> bool {
    let s = free_text.trim();
    s.eq_ignore_ascii_case("Ended")       // ← zero alloc, correct
        || s.eq_ignore_ascii_case("Final")
        || s.eq_ignore_ascii_case("Game Over")
        || s.eq_ignore_ascii_case("Finished")
        || s.eq_ignore_ascii_case("FT")
}
```

**Fix:**
```rust
pub(crate) fn parse_half(text: &str) -> &'static str {
    let s = text.trim();
    if s.is_empty() { return ""; }
    if s.eq_ignore_ascii_case("1st half") { return "1st"; }
    if s.eq_ignore_ascii_case("halftime")
        || s.eq_ignore_ascii_case("half time")
        || s.eq_ignore_ascii_case("ht")
    { return "Halftime"; }
    if s.eq_ignore_ascii_case("2nd half") { return "2nd"; }
    ""
}
```

**Impact:** ~50-200ns eliminated per soccer tick. The only heap allocation on the
WS thread success path.

---

### F-02: Missing `target-cpu=native` for deployment builds

```toml
# Cargo.toml:40-45
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
strip = "symbols"
incremental = false
```

No `target-cpu` specified. The deployment target is x86_64 EC2 (eu-west-1), which
has AVX2, SSE4.2, AES-NI, and BMI2. The default target is generic x86_64 — the
compiler leaves performance on the table for:

- **memchr:** SIMD-accelerated byte scanning (AVX2 processes 32 bytes/cycle vs 16
  with SSE2). `fast_extract_v1` and `fast_extract_boltodds` call memchr Finders
  on every frame. This is the single hottest loop in the WS path.
- **SHA-256:** Hardware AES-NI and SHA extensions accelerate HMAC computation on
  the submitter thread.
- **Branch/loop optimization:** AVX2-aware instruction scheduling, wider register
  allocation.

**Fix:** Set `RUSTFLAGS="-C target-cpu=native"` when building on the deployment
target (or `-C target-cpu=x86-64-v3` for portable AVX2). Do NOT set this in
`Cargo.toml` (it would break cross-compilation to macOS ARM).

```bash
# In EC2 build script:
RUSTFLAGS="-C target-cpu=native" maturin build --release \
    --manifest-path native/polybot2_native/Cargo.toml
```

**Impact:** 5-15% improvement on byte scanning, HMAC, and integer parsing. Free
performance — no code change.

---

### F-03: Redundant UTF-8 validation in `fast_extract.rs`

```rust
// fast_extract.rs:194-220 (four instances)
if let Some(start) = find_key_value_start(&FINDER_FIXTURE_ID, 11, bytes, pos) {
    if let Some((val, end)) = extract_string_value(bytes, start) {
        fixture_id = Some(std::str::from_utf8(val).ok()?);  // ← UTF-8 scan
        pos = end;
    }
}
// ... repeated for free_text, home_score, away_score
```

`std::str::from_utf8()` scans the entire byte slice to validate UTF-8 encoding.
Called 4 times per frame. The input is already validated:

- `Message::Text`: tungstenite validates UTF-8 on the entire message before
  delivering it. The bytes are guaranteed valid UTF-8.
- `Message::Binary`: the caller at `ws.rs:461` already calls
  `std::str::from_utf8(bytes)` on the full message. Subslices of valid UTF-8 are
  always valid UTF-8.

**Fix:**
```rust
// SAFETY: bytes are a subslice of a UTF-8-validated WS text frame.
fixture_id = Some(unsafe { std::str::from_utf8_unchecked(val) });
```

**Impact:** ~20-40ns eliminated per frame (4 validation scans × ~5-10ns each for
short strings: UUID=36 bytes, scores=1-2 bytes, freeText=~15 bytes).

**Risk:** Minimal. The safety invariant is structural — the WS library guarantees
it. Add a `// SAFETY:` comment documenting the invariant.

---

## Tier 2 — Fix Soon (waste on submitter thread per batch)

### F-04: Unnecessary semaphore acquire for len ≤ 15

```rust
// submitter.rs:149-167 (single-item path)
if batch_len == 1 {
    let (target_idx, prepared) = batch.into_iter().next().expect("len==1");
    let permit = match Arc::clone(&semaphore).acquire_owned().await {  // ← waste
        Ok(p) => p,
        Err(_) => return,
    };
    // ... one HTTP call ...
    drop(permit);
}

// submitter.rs:169-226 (2..=15 path)
if batch_len <= MAX_CLOB_BATCH {
    // ... build body ...
    let permit = match Arc::clone(&semaphore).acquire_owned().await {  // ← waste
        Ok(p) => p,
        Err(_) => return,
    };
    // ... one HTTP call ...
    drop(permit);
}
```

The semaphore (`Semaphore::new(3)`) exists to gate concurrent HTTP calls. But
`submit_batch_task` is called inline from the submitter spin-loop — **batches are
processed serially**. For len ≤ 15, there's exactly one HTTP call per batch.
The semaphore is pure overhead:

1. `Arc::clone(&semaphore)` — atomic increment (~5ns)
2. `.acquire_owned().await` — Tokio semaphore internal accounting (~100-500ns)
3. `drop(permit)` — atomic decrement + wake check (~10-50ns)

Total waste: ~115-555ns per batch, on a thread where the HTTP round-trip
takes 1-50ms. Marginal in absolute terms, but it's a free delete.

The semaphore is only load-bearing in the >15 chunk path where `join_all`
runs concurrent chunks.

**Fix:**
```rust
if batch_len == 1 {
    // Single order: direct post, no semaphore needed (serial execution)
    let (target_idx, prepared) = batch.into_iter().next().expect("len==1");
    let payload = *prepared;
    let outcome = match client_ref.post_order_bytes_single(payload.order_json).await {
        // ...
    };
    log_outcome_idx(log, &registry, target_idx, &outcome);
    return;
}

if batch_len <= MAX_CLOB_BATCH {
    // Small batch: single post_orders call, no semaphore needed
    // ...
}
```

**Impact:** ~115-555ns per batch eliminated. These are the two most common paths
(single-intent frames and small game-end frames).

---

### F-05: Per-batch heap allocations in 2..=15 path

```rust
// submitter.rs:171-185
let target_idxs: smallvec::SmallVec<[crate::TargetIdx; 4]> =
    batch.iter().map(|(idx, _)| *idx).collect();       // heap if >4 items
let mut order_jsons: Vec<Vec<u8>> = Vec::with_capacity(batch_len);  // heap
for (_, prepared) in batch {
    order_jsons.push(prepared.order_json);
}
let slices: Vec<&[u8]> = order_jsons.iter().map(|b| b.as_slice()).collect();  // heap
let chunk_body = build_orders_body_from_slices(slices.as_slice());  // heap (concat)

// ... after HTTP call ...
let outcomes: Vec<(crate::TargetIdx, Result<String, String>)> = ...;  // heap
```

Five heap allocations per batch in the 2-15 range:
1. `SmallVec` spills to heap for >4 items
2. `order_jsons: Vec<Vec<u8>>` — outer Vec allocated, inner Vecs moved from payload
3. `slices: Vec<&[u8]>` — pointer array
4. `chunk_body` from `build_orders_body_from_slices` — concatenated JSON
5. `outcomes: Vec<(TargetIdx, Result<...>)>` — result array

Total: ~1-2µs in allocator overhead per batch.

**Fix:** Extend `ChunkScratch` to hold reusable buffers for the 2..=15 path:

```rust
struct BatchScratch {
    target_idxs: Vec<crate::TargetIdx>,
    order_refs: Vec<Vec<u8>>,
    body_buf: Vec<u8>,
    outcomes: Vec<(crate::TargetIdx, Result<String, String>)>,
}
```

Clear and reuse across batches. After the first batch, subsequent batches
reuse the same heap allocations (Vec capacity is preserved across `.clear()` calls).

**Impact:** ~1-2µs per batch eliminated after first batch.

---

### F-06: `Url::clone()` on every HTTP call

```rust
// fast_submit_client.rs:84
.request(Method::POST, self.order_url.clone())  // ← Url::clone() allocates String
// fast_submit_client.rs:105
self.send_json(Method::POST, self.orders_url.clone(), "/orders", body)
```

`reqwest::Url::clone()` clones the internal `String` — heap allocation on every
HTTP call. The URLs are immutable after construction.

**Fix:** Store pre-built URL strings and pass `&str` to reqwest:

```rust
pub(crate) struct FastClobSubmitClient {
    // ...
    order_url_str: String,   // pre-built "https://clob.polymarket.com/order"
    orders_url_str: String,  // pre-built "https://clob.polymarket.com/orders"
}

// Then:
.request(Method::POST, self.order_url_str.as_str())
```

Note: `reqwest` re-parses the URL from `&str`, which is roughly equivalent cost
to cloning `Url`. The real win is switching to `hyper` directly for zero-overhead
URL handling. For now, this is a wash — **deprioritize**.

**Impact:** ~100ns per HTTP call (marginal given ms-scale HTTP round-trip).

---

### F-07: Scratch buffer cloning in >15 chunk path

```rust
// submitter.rs:234-247
while offset < batch_len {
    let chunk_len = (batch_len - offset).min(MAX_CLOB_BATCH);
    scratch.idxs_buf.clear();
    scratch.body_buf.clear();
    // ... fill scratch buffers ...
    chunk_jobs.push((scratch.idxs_buf.clone(), scratch.body_buf.clone()));  // ← clone
    offset += chunk_len;
}
```

Each chunk clones the scratch buffers (`Vec::clone()` = malloc + memcpy). For 30
items across 2 chunks, that's 2 malloc + 2 memcpy operations. The scratch pattern
is intended to reuse buffers, but the clone defeats the purpose.

**Fix:** Build owned buffers directly instead of filling scratch + cloning:

```rust
while offset < batch_len {
    let chunk_len = (batch_len - offset).min(MAX_CLOB_BATCH);
    let mut idxs = Vec::with_capacity(chunk_len);
    let mut body = Vec::with_capacity(estimated_body_size);
    body.push(b'[');
    for i in 0..chunk_len {
        let (tidx, prepared) = items.next().unwrap();
        idxs.push(tidx);
        if i > 0 { body.push(b','); }
        body.extend_from_slice(&prepared.order_json);
    }
    body.push(b']');
    chunk_jobs.push((idxs, body));
    offset += chunk_len;
}
```

Or use `mem::take()` on the scratch buffers and rebuild them for the next
iteration. Either way, avoid the clone.

**Impact:** ~500ns-2µs per oversized batch. Low severity — >15 intents per frame
is extremely rare (only massive game-end frames with many exact scores + spreads).

---

## Tier 3 — Observations (correct design, confirmed)

### O-01: Linear scan of sorted `over_lines` is optimal

Initial automated analysis recommended binary search. This is wrong. With
5-15 elements (typical totals line count per game), linear scan with sorted
early-exit (`if ol.half_int >= now { break; }`) is faster than binary search:

- Linear: sequential memory access, perfect cache prefetch, no branch
  mispredictions (early exit is well-predicted)
- Binary search: random access pattern within array, unpredictable branches
  at each bisection, function call overhead from `slice::binary_search_by_key`

The crossover point where binary search wins is ~30-50 elements for this
data shape. Games never have 30+ totals lines.

**Verdict:** No change needed.

---

### O-02: Exact score evaluation is single-pass

Initial analysis claimed "double iteration" over exact score slots. The actual
code (`soccer/eval.rs:271-280`) is a single pass with per-slot branching:

```rust
for slot in &targets.exact_scores {
    if home == slot.home_pred && away == slot.away_pred {
        push_if_some(slot.yes_idx, out);
        any_exact_matched = true;
    } else {
        push_if_some(slot.no_idx, out);
    }
}
```

One iteration, one branch per slot. Optimal for the data shape (typically 5-10
exact score predictions per game).

**Verdict:** No change needed.

---

### O-03: `SmallVec<[Intent; 32]>` capacity is correct

32 intents inline = 64 bytes on the stack (Intent is 2 bytes, TargetIdx is u16).
Maximum theoretical intents per frame:

- Baseball: ~20 (10 over lines + 2 NRFI + 1 walkoff + 1 moneyline + 6 spreads)
- Soccer: ~30 (10 over lines + 6 moneyline + 2 BTTS + 10 corners + 3 halftime + 10 exact scores + 1 any-other)

Soccer approaches the limit with fully loaded games. Consider increasing to
`SmallVec<[Intent; 48]>` if exact score coverage expands, but for current plan
sizes 32 is sufficient.

**Verdict:** Monitor, no change needed now.

---

### O-04: `FxHashMap::get` is the only string hash on the live path

```rust
// baseball/engine.rs:415
let &gidx = self.game_id_to_idx.get(fixture_id)?;
```

One hash per frame, on the fixture_id (UUID = 36 bytes, or game_label for
BoltOdds = ~40 bytes). FxHashMap uses a non-cryptographic hash — ~10-20ns for
these input sizes. This is irreducible: the game index must be resolved from
the provider's string ID.

Alternative considered: precomputed hash stored per subscription. Would save
~10ns but adds complexity and fragility. Not worth it.

**Verdict:** Optimal.

---

### O-05: `InlineStr<N>` dedup is zero-alloc

```rust
// lib.rs:80-105
pub(crate) struct InlineStr<const N: usize> {
    buf: [u8; N],
    len: u8,
}
```

Stack-allocated, no heap. Used for dedup fields in `StateRow`:
- `InlineStr<4>` for scores (5 bytes each, covers 0-9999)
- `InlineStr<32>` for freeText (33 bytes, covers all observed patterns)

Comparison via `as_str()` returns `&str` from stack memory — one `memcmp` with
good cache locality (the entire StateRow fits in 1-2 cache lines).

**Verdict:** Excellent design. No change.

---

### O-06: Log lock is never on the critical dispatch path

```rust
// baseball/frame_pipeline.rs:63-66
if !batch.is_empty() && !matches!(dispatch_handle.cfg.mode, DispatchMode::Noop) {
    dispatch_handle.send_batch(batch, log);  // ring push — no lock
}
flush_tick_logs(engine, &pending_logs, log);  // lock acquired HERE, after push
```

The `Arc<Mutex<LogWriter>>` lock is acquired after `send_batch` completes.
The ring push (the latency-critical operation) holds zero locks. The log flush
happens at the end of the frame drain loop in `ws.rs:497-499`, batching all
pending tick logs into a single lock acquisition.

On the submitter thread, log locks are acquired after HTTP round-trips complete
(milliseconds have passed — lock contention is negligible).

**Verdict:** Correct design.

---

### O-07: SPSC ring backpressure is fail-safe

```rust
// flow.rs:76-88
match tx.push(SubmitWork::Batch(batch)) {
    Ok(()) => {}
    Err(rtrb::PushError::Full(work)) => {
        // Log error per item, orders are dropped
    }
}
```

Ring capacity is 64. If the submitter is overwhelmed (HTTP calls backing up),
new orders are dropped with error logging. This is correct: stale orders in a
backed-up system are worse than dropped orders. The presign pool's one-shot
semantics mean dropped orders can't be retried anyway.

The per-item log lock in the error path (`flow.rs:84`) is fine — this is a
system failure state where latency doesn't matter.

**Verdict:** Correct design.

---

### O-08: `flume` is still used (not dead dependency)

`flume` is used for the Python→Rust control plane channels:

```rust
// runtime.rs:  flume::unbounded::<LiveWorkerCommand>()  — stop, set_subscriptions
// runtime.rs:  flume::unbounded::<PatchPayload>()       — hot-patch delivery
// ws.rs:       command_rx: flume::Receiver<LiveWorkerCommand>
// ws.rs:       patch_rx: flume::Receiver<PatchPayload>
```

These are drained at quiescent points in the WS event loop (between frame drain
iterations, not during frame processing). `flume::TryRecvError::Empty` is a
non-blocking check — negligible overhead.

**Verdict:** Correctly used, not a dead dependency.

---

### O-09: `serde_json` `preserve_order` feature is harmless

```toml
serde_json = { version = "1", features = ["preserve_order"] }
```

This swaps `BTreeMap` for `IndexMap` in JSON object representation. Only affects
code paths that deserialize JSON objects via serde:

- Batch frame path (`frame_pipeline.rs:31`): rare, cold path
- `merge_plan()`: hot-patch, called once per refresh (every 30min)
- Plan loading at startup: one-time

The fast_extract path bypasses serde entirely (byte-level scanning). No impact
on the hot WS path.

**Verdict:** No change needed.

---

## Tier 4 — Architecture Notes (beyond single-commit scope)

### A-01: reqwest abstraction overhead on submitter thread

`reqwest::Client::request()` builds a `RequestBuilder` internally, which:
1. Clones the URL (heap alloc)
2. Constructs a `HeaderMap` and inserts headers (heap alloc for map + values)
3. Builds the hyper `Request` struct
4. Wraps in middleware layers (redirect, timeout, etc.)

Estimated overhead: ~1-5µs per HTTP call, compared to raw hyper which would
be ~200-500ns for request construction.

Switching to `hyper` directly would:
- Eliminate the reqwest middleware stack
- Allow reusing a pre-built `Request` template with only signature/timestamp
  headers varying per call
- Remove the URL clone (hyper accepts `Uri` by reference)

**Trade-off:** Significant code change (~200-300 lines). The HTTP round-trip
(1-50ms) dominates, so the 1-5µs savings is <0.5% of wall time. Only worth
doing if competing against implementations that have already eliminated all
WS-thread waste and are fighting for submitter-thread microseconds.

---

### A-02: HMAC computation is near-optimal

```rust
// fast_submit_client.rs:180-209
fn hmac_signature_b64_impl(&self, ...) {
    let mut mac = Hmac::<Sha256>::new_from_slice(self.decoded_secret.as_slice())?;
    mac.update(ts_text.as_bytes());
    mac.update(method.as_bytes());
    mac.update(path.as_bytes());
    // body: direct update or quote-normalized update
    let digest = mac.finalize().into_bytes();
    URL_SAFE.encode_slice(digest_bytes, &mut out)?;
}
```

- Secret is decoded once at client construction (cached `Vec<u8>`)
- Timestamp uses `itoa::Buffer` (stack-allocated integer formatting)
- Multiple `mac.update()` calls vs single concatenated update: the SHA-256
  internal block processing doesn't care — it buffers internally until a
  64-byte block boundary. No performance difference.
- Quote normalization uses `memchr(b'\'', body)` — single SIMD scan. For
  pre-serialized JSON without apostrophes (the common case for the single-order
  path), this is one scan + direct `mac.update(body)`.
- Base64 output to stack buffer `[0u8; 44]` — zero alloc.

The `hmac_signature_b64_into_stack_trusted_json` variant skips normalization
entirely for single orders (the order JSON is trusted, generated by serde
at presign time — no apostrophes). Good specialization.

**Verdict:** Near-optimal. Only gains from `target-cpu=native` (SHA-NI
hardware instructions).

---

### A-03: Monotonic clock is correct

```rust
// ws.rs:458
let source_recv_ns = worker_clock_origin.elapsed().as_nanos() as i64;
```

`Instant::elapsed()` uses the platform's monotonic clock (CLOCK_MONOTONIC on
Linux). This is a single `clock_gettime` syscall — ~20-30ns on modern Linux
with vDSO. Not improvable without `rdtsc` (which has calibration issues across
CPU frequency changes and is not portable).

The `.as_nanos() as i64` conversion is a zero-cost integer cast.

**Verdict:** Optimal for portable code.

---

### A-04: Spin-loop on submitter thread

```rust
// submitter.rs:101-103
if !had_work {
    std::hint::spin_loop();
}
```

`spin_loop()` emits a PAUSE instruction on x86_64, reducing CPU power draw
while maintaining immediate wake-up latency. This is the correct choice for
the submitter thread where latency matters more than CPU efficiency.

Alternative: `thread::park()` / `Condvar` would save CPU but add ~1-10µs
wake-up latency. For a trading system, that's unacceptable.

Alternative: `rtrb` doesn't support blocking waits natively. A
`crossbeam::Parker` with timeout could offer a middle ground (spin for N
iterations, then park). But the submitter thread runs on a dedicated core
in production — CPU waste is acceptable.

**Verdict:** Correct for the use case.

---

## Latency Budget (Estimated)

### WS Thread: Frame Receipt → Ring Push

| Stage | Estimated ns | Notes |
|-------|-------------|-------|
| `Instant::elapsed()` | 20-30 | vDSO monotonic clock |
| `fast_extract_v1` (memchr scan) | 200-500 | ~400 byte frame, 4 field extractions |
| `from_utf8` validation (4x) | 20-40 | **eliminable (F-03)** |
| `check_duplicate` (FxHashMap) | 20-50 | single hash + 3 InlineStr comparisons |
| `fast_parse_score` (2x) | 5-10 | 1-digit: single byte op |
| `parse_period` / `parse_half` | 30-60 | byte scanning (baseball); **+50-200ns overhead (F-01) for soccer** |
| `is_completed_free_text` | 5-20 | `eq_ignore_ascii_case` × 5 |
| `process_tick_live` state update | 20-50 | scalar assignments, Option unwraps |
| `evaluate_*_into` (4-6 evals) | 30-100 | array indexing, early exits, SmallVec push |
| `pop_for_target` (per intent) | 50-100 | 2 Vec lookups + Option::take |
| `send_batch` (ring push) | 30-50 | rtrb lock-free SPSC |
| **Total (success path)** | **~430-1010** | **< 1.1µs typical** |
| **With F-01 + F-03 fixes** | **~360-770** | **< 0.8µs typical** |

### Submitter Thread: Ring Pop → Bytes on Wire

| Stage | Estimated ns/µs | Notes |
|-------|-----------------|-------|
| `submit_rx.pop()` | 10-20ns | rtrb lock-free |
| `shared_registry.load()` | 5-10ns | ArcSwap atomic load |
| Semaphore acquire (if kept) | 100-500ns | **eliminable (F-04)** |
| `timestamp_now_seconds()` | 20-50ns | SystemTime |
| `itoa::Buffer::format` | 5-10ns | stack integer formatting |
| HMAC-SHA256 computation | 200-400ns | ~300 byte message |
| Base64 encode to stack | 10-20ns | 32 → 44 byte encode |
| `HeaderValue` construction (5x) | 50-100ns | small byte copies |
| reqwest request build | 1-5µs | builder + headers map |
| TCP/TLS send | 1-50ms | **dominates, not optimizable** |
| Response read + parse | 1-5µs | small JSON response |
| Log outcome | 50-200ns | Mutex lock + buffered write |
| **Total (pre-TCP)** | **~1.5-6.5µs** | setup before send() |
| **With F-04 + F-05 fixes** | **~1-5µs** | |

---

## Priority Ranking

| ID | Tier | Impact | Effort | Recommendation |
|----|------|--------|--------|----------------|
| F-01 | 1 | 50-200ns/tick | 5 min | Fix immediately — 4 line change |
| F-02 | 1 | 5-15% global | 2 min | Add RUSTFLAGS to deploy build |
| F-03 | 1 | 20-40ns/frame | 10 min | unsafe from_utf8_unchecked with SAFETY comment |
| F-04 | 2 | 115-555ns/batch | 15 min | Remove semaphore from ≤15 path |
| F-05 | 2 | 1-2µs/batch | 30 min | Scratch buffer reuse |
| F-06 | 2 | ~100ns/call | — | Deprioritize (reqwest re-parses anyway) |
| F-07 | 2 | 0.5-2µs/rare | 15 min | Fix clone in >15 path |
| A-01 | 4 | 1-5µs/call | 2-3 days | Defer — hyper migration is high-risk |

---

## What's Already Right

The design has been through multiple optimization passes and the fundamentals
are sound. Things that are often done wrong but are correct here:

1. **Zero-alloc WS path** — InlineStr, SmallVec, borrowed slices, no Tick/TickResult intermediate types
2. **Decoupled submitter** — WS thread does zero HTTP, zero signing, zero string formatting
3. **Lock-free SPSC ring** — rtrb with spin-loop consumer, fail-safe backpressure
4. **Pre-serialized orders** — JSON bytes computed once at presign time, submitter sends raw bytes
5. **Frame-preserving batch** — one ring push per material frame, no cross-frame coalescing
6. **Deferred logging** — tick logs flushed after dispatch, not before
7. **One-shot presign pool** — Option::take as the sole duplicate gate, no bitsets or cooldowns
8. **Release profile** — LTO=fat, codegen-units=1, opt-level=3, strip=symbols
9. **Monotonic clock** — Instant-based, no wall-clock on the engine path
10. **Arc<str> registry** — ref-count bumps instead of string copies on registry clone
