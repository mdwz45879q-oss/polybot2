# Rust Hotpath Code Review

Review of `native/polybot2_native/src/` — prioritized findings grouped by severity.

---

## 1. Correctness Risks

### 1.1 `cancel_order_async` ambiguous fallback — `dispatch/sdk_exec.rs:219`

```rust
Ok(!response.canceled.is_empty())
```

If the cancel response contains neither our order ID in `canceled` nor `not_canceled`, this returns `Ok(true)` if ANY order was canceled — potentially a different order. In the cancel-replace path (`flow.rs:230`), this would proceed to submit a replacement while the original may still be live, creating a duplicate position.

**Mitigated today** because cancel-replace is unreachable (one-shot intents). Becomes a real risk if multi-intent support is added.

**Fix:** Return `Ok(false)` or `Err` when the target order isn't explicitly confirmed canceled.

### 1.2 Silent broker query failures mask stale eviction — `dispatch/flow.rs:520`

```rust
Err(_) => {}
```

`refresh_active_state_from_broker_async` silently ignores all `get_order_async` errors. After 60 seconds of silent failures, `evict_stale_active_orders` (line 525) removes the order and emits `order_failed` with reason `stale_active_order_evicted`. The order may still be live on the CLOB.

**Fix:** Track consecutive broker failures per strategy key. Only evict on broker-confirmed terminal states, not on API timeouts.

### 1.3 Silent presign refill errors — `dispatch/presign_pool.rs:266`

```rust
let _ = self.refill_presign_key_async(&key, false).await;
```

Refill errors are silently discarded. If SDK auth expires or signing consistently fails, the pool drains to zero with no telemetry. Operators only learn about it when orders start failing with `submit_presigned_miss`.

**Fix:** Emit a telemetry event on refill failure, or at minimum log the error.

### 1.4 Unbounded HashMap/HashSet growth — `lib.rs:295-313`

The engine accumulates state in 15 collections (`rows`, `game_states`, `last_emit_ns`, `last_signature`, `attempted_strategy_keys`, `totals_final_under_emitted`, `nrfi_resolved_games`, `nrfi_first_inning_observed`, `final_resolved_games`, etc.) with no eviction policy. `reset_runtime_state()` exists but is only called during `load_plan`. Over a long-running session with many games, this is a slow memory leak.

**Fix:** Add game-completion cleanup — when `match_completed` fires for a game, remove that game's entries from all tracking maps/sets. Alternatively, add a TTL-based sweep.

### 1.5 GIL held during presign warmup — `runtime.rs:105-106`

```rust
let warm_result = match TokioBuilder::new_current_thread().enable_all().build() {
    Ok(rt) => rt.block_on(dispatch_runtime.warm_presign_startup_async()),
```

A blocking Tokio runtime runs presign warmup while holding the Python GIL. If warmup takes 5+ seconds (network latency, many keys), all Python threads are frozen.

**Fix:** Wrap in `py.allow_threads(|| { ... })` to release the GIL during warmup.

---

## 2. Simplification Opportunities

### 2.1 Unreachable cancel-replace code — `dispatch/flow.rs:225-371`

~150 lines implementing cancel-then-submit that cannot execute due to one-shot intents (`attempted_strategy_keys` in `engine.rs:559`). CLAUDE.md documents this as "future use." Consider either:
- Deleting it (CLAUDE.md says "prefer deletion over compatibility shims")
- Adding a `#[cfg(test)]` integration test proving it works, so it's verified if/when one-shot is relaxed

### 2.2 `evaluate_final` unused return element — `engine.rs:509`

```rust
let (reason, _) = self.evaluate_final(&delta, &state, &mut intents);
```

The second return value (`"action"` / `"no_action"`) is always discarded. `evaluate_final` could return just `String`.

### 2.3 Redundant `unwrap_or` after None guard — `eval.rs:48-49`

```rust
if total_now.is_none() || prev_total.is_none() {
    return Some("mlb_totals_missing_state".to_string());
}
let total_now_int = total_now.unwrap_or(0);   // can never be None here
let prev_total_int = prev_total.unwrap_or(0); // can never be None here
```

After the None guard at lines 45-46, these can never be None. Use `.unwrap()` or destructure with `if let`.

### 2.4 Redundant message type check — `engine.rs:316`, `engine.rs:787`

```rust
if mtype == "ping" || mtype != "next" {
```

Logically equivalent to `if mtype != "next"`. The `ping` check is subsumed by `!= "next"`. Confusing but not incorrect.

### 2.5 `is_first_inning` allocates HashSet on every call — `eval.rs:302`

```rust
let mut allowed: HashSet<&str> = HashSet::from(["top", "bottom", ""]);
```

Called on every tick during NRFI evaluation. A simple `matches!()` or array `.contains()` avoids the allocation.

### 2.6 Duplicate frame processing logic — `engine.rs:300-363` vs `engine.rs:773-818`

`process_score_frame` (PyDict) and `process_score_frame_value` (serde_json::Value) contain identical orchestration logic. The only difference is the parse function called. Could extract the shared accumulation into a generic helper that takes a tick iterator.

### 2.7 Unnecessary clones in active order refresh — `dispatch/flow.rs:494-498`, `flow.rs:528-533`

```rust
let keys = self.active_orders_by_strategy.iter()
    .map(|(k, v)| (k.clone(), v.clone()))
    .collect::<Vec<_>>();
```

Cloning every key+value to iterate. Standard pattern for borrow-checker workaround when the loop body needs `&mut self`, but could collect only the keys needed (exchange_order_id) to reduce allocation size.

### 2.8 SDK init not retryable — `dispatch/sdk_exec.rs:20-22`

```rust
if self.sdk_runtime.is_some() {
    return Ok(());
}
```

If `authenticate()` fails, `sdk_runtime` stays `None` and subsequent calls retry. But if it partially initializes (signer created, auth fails), `cached_signer` is set without a matching `sdk_runtime`. This isn't currently a problem because `ensure_sdk_runtime` is the only path, but the partial state is fragile.

---

## 3. Minor / Style

### 3.1 Empty `chain_id` in all intents — `eval.rs:24`

`chain_id: String::new()` is always empty. If no consumer needs it, remove the field. If a consumer expects it, populate it from config.

### 3.2 `strategy_key_owned` cloned twice — `engine.rs:554-557`

```rust
let strategy_key_owned = intent.strategy_key.clone();
self.last_emit_ns.insert(strategy_key_owned.clone(), ...);
self.last_signature.insert(strategy_key_owned.clone(), sig);
```

One clone could be avoided by inserting into `last_signature` first (consuming the owned value) and using `entry` API for `last_emit_ns`, or just accepting this as minor.

### 3.3 `get_order_async` price parse fallback — `dispatch/sdk_exec.rs:258`

```rust
limit_price: order.price.to_string().parse::<f64>().unwrap_or(0.0),
```

If the SDK returns a malformed price, this silently becomes 0.0. Only affects telemetry (order is already on the CLOB), but corrupts observability.

### 3.4 Presign warmup timeout is all-or-nothing — `dispatch/presign_pool.rs:146-164`

If any single key's signing times out, the entire startup fails even if other keys completed. The successfully signed orders are lost. Could collect partial results and only fail if all keys fail.

---

## Summary

| # | Finding | Severity | File | Impact |
|---|---------|----------|------|--------|
| 1.1 | Cancel response ambiguity | High (latent) | sdk_exec.rs:219 | Duplicate position risk if cancel-replace activated |
| 1.2 | Silent broker query failures | High | flow.rs:520 | Misleading stale eviction of potentially live orders |
| 1.3 | Silent presign refill errors | High | presign_pool.rs:266 | Undetectable pool drain |
| 1.4 | Unbounded collection growth | Medium | lib.rs:295-313 | Memory leak over long sessions |
| 1.5 | GIL held during warmup | Medium | runtime.rs:105-106 | Python thread starvation during startup |
| 2.1 | Unreachable cancel-replace | Low | flow.rs:225-371 | Dead code / maintenance burden |
| 2.2 | Unused return value | Low | engine.rs:509 | Unnecessary tuple allocation |
| 2.3 | Redundant unwrap_or | Low | eval.rs:48-49 | Misleading safety |
| 2.4 | Redundant message type check | Low | engine.rs:316 | Confusing logic |
| 2.5 | HashSet alloc on hot path | Low | eval.rs:302 | Unnecessary allocation per tick |
| 2.6 | Duplicate frame processing | Low | engine.rs:300/773 | Copy-paste maintenance risk |

**Not bugs (verified):**
- NRFI guard at `eval.rs:157` — `total` is only `Some` when both home/away are present (see `update_game_state:690`)
- Totals over/under crossing logic — correct boundary conditions
- Spread evaluation formula — correct for both home and away margins
- `load_plan` GIL safety — all PyDict data is copied to Rust types, no Python references stored
- Partial score updates in `update_game_state` — `.or(prev.X)` fills from previous state, so state is always consistent
