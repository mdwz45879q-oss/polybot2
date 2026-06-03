# Guardian Correctness Audit

Audit of the Guardian overturn-detection pipeline: log tailing → score tracking → overturn detection → market monitoring → execution.

---

## 1. Critical Issues

### C1. Execution retry is broken — failed overturn response never retries

**File:** `guardian/overturn.py:222-247, 284-299`

After execution fails, the comment says "will retry on next confirmation check" but the code structure prevents this.

```python
# overturn.py:234-246 — check_confirmations()
if not alert.signal1_confirmed:                    # ← GUARDED: only enters on transition
    elapsed_s = (now_ms - alert.reversal_ts) / 1000.0
    if elapsed_s >= self._confirmation_window_s:
        alert.signal1_confirmed = True
        ...
        result = self._check_and_trigger(alert)    # ← only called here
```

```python
# overturn.py:294-299 — _on_execution_done() on failure
alert.signal1_confirmed = True   # already True
alert.signal2_confirmed = True   # already True
# acted remains False — but no code path calls _check_and_trigger again
```

**What's wrong:** `check_confirmations()` only calls `_check_and_trigger()` when `signal1_confirmed` transitions from False to True (line 235: `if not alert.signal1_confirmed`). After execution fails, both signals remain True and `acted` is False. On the next `check_confirmations` cycle, the `if not alert.signal1_confirmed` guard skips the alert entirely. Similarly, `on_best_bid_ask()` guards with `if not alert.signal2_confirmed` (line 191). Neither path re-invokes `_check_and_trigger`.

**Impact:** If the CLOB client fails during overturn execution (network error, timeout, rate limit), the guardian permanently gives up on that overturn. Resting GTC orders stay open and filled positions remain unsold — real money at risk.

**Correct behavior:** `check_confirmations()` should call `_check_and_trigger()` for any alert where both signals are confirmed and `acted` is False, not only on the signal1 transition:

```python
# After the signal1 transition block, add:
if alert.signal1_confirmed and alert.signal2_confirmed and not alert.acted:
    result = self._check_and_trigger(alert)
    if result:
        triggered.append(alert)
```

### C2. `_find_original_goal_event` fails when goals from both sides intervene

**File:** `guardian/overturn.py:301-329`

```python
# overturn.py:318-329
for event in reversed(game.score_timeline):
    if event.prev_home is None or event.prev_away is None:
        continue
    if event.home == prev_home and event.away == prev_away:     # ← requires EXACT score match
        if prev_home > new_home and event.home > event.prev_home:
            return event
        if prev_away > new_away and event.away > event.prev_away:
            return event
return None
```

**Scenario:** 0-0 → 1-0 (T1, home goal) → 1-1 (T2, away goal) → 0-1 (T3, home goal overturned by VAR)

At T3: `prev=(1,1), new=(0,1)`. The function searches for an event with `event.home == 1 AND event.away == 1`:
- Event T3 (0,1): `0 == 1` → False. Skip.
- Event T2 (1,1): `1 == 1 AND 1 == 1` → True. But `prev_home > new_home AND event.home > event.prev_home` → `1 > 0 AND 1 > 1` → **False** (1 > 1 fails). And `prev_away > new_away AND event.away > event.prev_away` → `1 > 1 AND ...` → **False**. Skip.
- Event T1 (1,0): `1 == 1 AND 0 == 1` → **False**. Skip.
- Returns **None**. The overturned goal is undetected.

**What's wrong:** The function requires `event.home == prev_home AND event.away == prev_away` — meaning the event must produce EXACTLY the pre-reversal score. When the other side scores between the original goal and the overturn, no event in the timeline produces that exact score.

**Impact:** Any overturn of a goal that occurred before a subsequent goal by the other team will fail to be detected. This is uncommon but realistic — VAR reviews can take 3-5 minutes, during which play continues and the other team can score.

**Correct behavior:** Search for the most recent event where the overturned side's score increased, regardless of the other side's score:

```python
for event in reversed(game.score_timeline):
    if event.prev_home is None or event.prev_away is None:
        continue
    if prev_home > new_home and event.home > event.prev_home:
        return event  # most recent home goal — the one being overturned
    if prev_away > new_away and event.away > event.prev_away:
        return event  # most recent away goal — the one being overturned
return None
```

### C3. VAR `GoalNotAwarded` signal is logged but not acted upon

**File:** `guardian/overturn.py:210-218`

```python
# overturn.py:210-217
elif var_type == "VarEnded":
    if "NotAwarded" in var_subtype:
        logger.warning(
            "🚨 VAR: GOAL OVERTURNED for %s (%s)", game_id, var_subtype,
        )
        # TODO: this is a definitive overturn signal — can trigger
        # sell/buy immediately without waiting for score reversal.
```

**What's wrong:** `VarEnded:GoalNotAwarded` is a **definitive** signal from the referee — the goal is officially overturned. The system has this data but does not use it to expedite or independently trigger the overturn response. It only logs a warning.

**Impact:** The system relies entirely on the dual-signal (score reversal held 10s + market bid drop). The VAR signal arrives seconds before the score reversal appears in the data feed. Using it would save 5-15 seconds on the response time — material at the prices these assets trade at. Additionally, if the V2 data feed lags or misses the score reversal but does deliver the VAR action, the overturn would go undetected entirely.

---

## 2. Potential Issues

### P1. GTC fill amount overwritten by individual trade, not accumulated

**File:** `guardian/tracker.py:260-264`

```python
# tracker.py:260-264 — _on_ws_trade
if size is not None:
    try:
        order.fill_amount = float(size)    # ← overwrites, not accumulates
    except (TypeError, ValueError):
        pass
```

**What's wrong:** For GTC orders that fill in multiple tranches, each `trade` event overwrites `fill_amount` with the individual trade's `size`. If a 100-share fill is followed by a 50-share fill, `fill_amount` ends up as 50, not 150. The sell order in the executor would sell only 50 shares.

**Mitigation:** `_on_ws_order` (line 299) sets `fill_amount = float(size_matched)` where `size_matched` is cumulative. If the `order` event arrives after the `trade` events, it corrects the value. But timing is not guaranteed — the sell decision might execute before the corrective `order` event arrives.

**Impact:** For GTC orders with multiple partial fills, the sell order during an overturn would be for the wrong (too small) amount. FAK orders fill in one shot and are unaffected.

### P2. WS trade event before log order event silently drops fill data

**File:** `guardian/tracker.py:249-275`

```python
# tracker.py:257-258 — _on_ws_trade
taker_order_id = str(data.get("taker_order_id", ""))
order = self.state.orders_by_eid.get(taker_order_id)   # ← None if order not yet processed
```

**What's wrong:** If the Polymarket user WS delivers a `trade` event before the guardian processes the corresponding `order` event from the JSONL log, `orders_by_eid` does not contain the order yet. The fill data is silently dropped — no retry, no queue.

**Mitigation:** In practice, the CLOB responds to the order submission first (order logged by Rust), then the fill propagates through the matching engine (trade event via WS). The order log entry typically arrives first. But under heavy load or WS connection issues, ordering is not guaranteed.

**Secondary mitigation:** REST fallback via `_query_pending_fak_orders`/`_poll_gtc_orders` would recover the data — but these only run when `not self.ws` (lines 464-466). If the WS is connected, REST fallback is disabled entirely, so a single dropped WS message means permanent data loss for that order.

### P3. Maker-side fills not matched by `taker_order_id`

**File:** `guardian/tracker.py:255-258`

```python
# tracker.py:255-258 — _on_ws_trade
taker_order_id = str(data.get("taker_order_id", ""))
order = self.state.orders_by_eid.get(taker_order_id)
```

**What's wrong:** GTC orders that rest on the orderbook are filled as maker — someone else's market order crosses our limit. In that case, the `trade` event's `taker_order_id` is the other party's order ID, not ours.

**Mitigation:** `_on_ws_order` (line 283-310) matches by `id` (our order ID) and includes `size_matched`. This covers maker fills, though the fill price from `_on_ws_order` may be the limit price (not actual execution price).

**Impact:** Low — maker fills are caught by the `order` event. But the `trade` event's individual execution price is lost.

### P4. `add_game_id_alias` docstring is misleading (code is correct)

**File:** `guardian/manager.py:196-204` and `_cli/commands_hotpath_runtime.py:580-583`

```python
# manager.py:196-204 — docstring says "pre-resolution → fixture ID"
def add_game_id_alias(self, alias_id: str, canonical_id: str) -> None:
    """Register an alternate game ID mapping (e.g., pre-resolution → fixture ID)."""
    self._tracker._game_id_map[alias_id] = canonical_id

# commands_hotpath_runtime.py:580-583 — actual call
guardian.add_game_id_alias(
    resolved.pending.prematch_event_id,   # alias_id = prematch
    resolved.fixture_id,                  # canonical_id = fixture
)
```

**Why this works despite the confusing naming:** After `update_plan` runs `build_game_id_map`, the map has `{prematch→prematch, fixture→prematch}`. Then `add_game_id_alias` overrides to `{prematch→fixture, fixture→prematch}`. Since both tick and order log events use the prematch_event_id as `gid` (Rust's `game_ids[GameIdx]` stores the `provider_game_id`), both are normalized via `game_id_map.get(prematch, prematch)` → `fixture`, resolving to the same key. Not a bug, but the parameter names (`alias_id`, `canonical_id`) and docstring ("pre-resolution → fixture ID") suggest the opposite mapping direction from what `build_game_id_map` uses, making the code hard to reason about.

### P5. Log file rotation/truncation not handled

**File:** `guardian/log_tailer.py:35-50`

```python
# log_tailer.py:39-50 — tail_log_async
with open(log_path, "r") as f:
    while True:
        line = f.readline()
        if line:
            ...
        else:
            await asyncio.sleep(poll_interval)
```

**What's wrong:** If the hotpath restarts and writes to a new log file (different `run_id` path), the guardian continues tailing the old file indefinitely. There is no detection of file rotation, inode change, or file truncation.

**Mitigation:** In practice, the guardian runs in the same process as the hotpath (started by the orchestrator), so a hotpath restart kills the guardian too. The `log_path` is set at startup and doesn't change.

**Impact:** Not a live issue given the current architecture, but a latent risk if the architecture changes.

### P6. Signal 2 confirms immediately for low-value tokens

**File:** `guardian/overturn.py:190-197`

```python
# overturn.py:190-197
if bid_val < self._bid_threshold and token_id in alert.affected_token_ids:
    if not alert.signal2_confirmed:
        alert.signal2_confirmed = True
```

**What's wrong:** If a token's best bid was already below the $0.80 threshold before the overturn (e.g., a TOTAL OVER 4.5 at 2-1 would have a low bid), Signal 2 confirms immediately when the alert is armed. The dual-signal effectively becomes Signal 1 only.

**Mitigation:** Signal 1 (10-second hold) is designed to be the primary gate. Tokens that are "already winning" on a goal (TOTAL OVER near current score, BTTS YES) typically have high bids, so the threshold is meaningful for the common case. Low-value tokens that are already below threshold are generally not worth much — the financial exposure is small.

**Impact:** For edge cases (orders on high-line totals that happen to cross on a goal that's then overturned), the system degrades to single-signal confirmation. Acceptable given the 10s hold window.

---

## 3. Verified Correct

### Log Tailing (`guardian/log_tailer.py`)

- **Async yield:** `tail_log_async` uses `asyncio.sleep(0.1)` allowing the event loop to process WS events and confirmation checks during idle. Verified correct — the 100ms poll interval is appropriate for log events (Rust flushes after each tick batch, orders arrive ~50-200ms after ticks).
- **JSON error handling:** Malformed lines are caught by `json.JSONDecodeError` and skipped. No crash path.
- **Catchup:** On startup, all existing lines are read before blocking. The guardian sees the full history.

### Tick Processing (`guardian/tracker.py:_on_tick`)

- **Score field extraction:** `goals_home`/`goals_away` correctly reads the V2 soccer log format, with `h`/`a` fallback for V1. Verified against `log_writer.rs:209-211` which writes `"goals_home":N,"goals_away":N` for soccer ticks.
- **VAR field extraction:** `var_type`/`var_sub` correctly reads from tick events. Verified against `log_writer.rs:231-235` which writes `"var_type":"...","var_sub":"..."` when non-empty.
- **Game ID normalization:** `gid = self._game_id_map.get(gid, gid)` correctly normalizes alternate provider IDs. Single-hop lookup, identity fallback.
- **Cold-start protection:** First tick for a game has `prev is None`, which skips overturn detection (line 169: `if self.detector and prev is not None`). Prevents false overturns when guardian starts mid-game.
- **Prev-score initialization:** `prev_home, prev_away = prev if prev else (0, 0)` creates a synthetic ScoreEvent for cold-start. This allows subsequent overturns to be detected correctly — orders triggered by the cold-start event are found and cancelled. Verified with trace analysis.

### Order Processing (`guardian/tracker.py:_on_order`)

- **Game ID normalization:** Applied correctly via `self._game_id_map.get(gid, gid)` on line 195. The `gid` field from the Rust log uses `gid_from_sk(sk)` (log_writer.rs:99-100), which extracts the provider_game_id prefix from the strategy key.
- **TIF extraction:** `str(ev.get("tif", "")).upper()` reads directly from the Rust log event. Verified against `log_writer.rs:326` which writes `"tif":"FAK"` or `"tif":"GTC"`.
- **triggered_by matching:** The reverse search `for score_ev in reversed(game.score_timeline): if score_ev.ts <= ts` correctly finds the latest score event before the order. Since Rust writes the order event (`log_order_ok`) AFTER CLOB HTTP completion, and writes the tick event BEFORE the channel send, the tick timestamp always precedes the order timestamp for the same frame. Verified against Rust frame pipeline flow.

### Wobble Disarm (`guardian/overturn.py:129-143`)

- **Correct condition:** `is_restored = (new_home >= alert.original_score_event.home and new_away >= alert.original_score_event.away)`. If the score returns to or exceeds the original goal's result, the alert is disarmed.
- **Cleanup is complete:** `_cleanup_alert` removes the alert from `self.alerts` AND removes token-to-game mappings from `_token_to_game`.
- **Timing:** Wobble within 10s prevents Signal 1 confirmation. Wobble after Signal 1 but before Signal 2 still disarms (checked: `not alert.acted` guard passes). Wobble after both signals + acted=True correctly does NOT disarm (damage already done, but acted flag prevents re-entry).

### Dual-Signal Design (`guardian/overturn.py`)

- **Signal 1 (score held):** `check_confirmations()` compares `now_ms` (Python wall clock) with `reversal_ts` (Rust wall clock `now_unix_ms()`). Both are system wall clock milliseconds — no clock domain mismatch.
- **Signal 2 (market bid):** `on_best_bid_ask()` checks bid < threshold for affected tokens only (`token_id in alert.affected_token_ids`). Non-affected tokens are ignored.
- **And-gate:** `_check_and_trigger()` requires `signal1_confirmed AND signal2_confirmed AND NOT acted`. Correct boolean logic.

### Market WS (`guardian/market_ws.py`)

- **Wait-for-tokens:** `_run_session()` loops `while not self._token_ids and not self._stop` before connecting. Prevents Polymarket's auto-disconnect on empty subscription.
- **Reconnection with backoff:** Exponential backoff from 2s to 30s on failure. Resets on clean session. Re-subscribes all tokens on reconnect (line 120-121: sends full `_token_ids` set).
- **Subscription key:** `"assets_ids"` is correct per Polymarket's market WS API.
- **Dynamic subscription:** `subscribe()` adds to `_token_ids` and sends immediately if connected, or queues for next connection.

### User WS (`guardian/polymarket_ws.py`)

- **Wait-for-conditions:** Same pattern as market WS — waits for at least one condition ID before connecting.
- **Auth:** Sends `apiKey`, `secret`, `passphrase` in every subscription message. Correct per Polymarket's user WS API.
- **Event dispatch:** `event_type == "trade"` → `on_trade`, `event_type == "order"` → `on_order`. Correct event types.
- **Reconnection:** Re-subscribes all condition IDs on reconnect. Connection state (`_connected`, `_ws`) correctly reset in outer loop.

### Executor (`guardian/executor.py`)

- **GTC cancellation gate:** `order.time_in_force == "GTC" and order.order_status in ("OPEN", "PLACEMENT", "")`. Empty string catches orders that haven't been queried yet. Correct.
- **Sell price:** Uses `best_bids.get(order.token_id, 0.0)` from the detector's market data. `MIN_SELL_BID = 0.01` prevents selling at zero.
- **Dry-run isolation:** All actions gated by `self.dry_run`. In dry-run, only logging occurs — no CLOB calls.
- **Fill check for sell:** `if order.fill_amount and order.fill_amount > 0` correctly skips unfilled orders. `None` and `0.0` both evaluate to falsy.

### Guardian Logger (`guardian/guardian_logger.py`)

- **Self-contained logs:** Both event and price logs include `session_start` header. Correct.
- **Token-to-SK mapping:** Built from `session_header["tokens"]` in `OrderStateTracker.__init__` (tracker.py:76-79). Verified: `_build_session_header` correctly maps every target's `token_id → strategy_key`.
- **Price log isolation:** Price snapshots go to a separate file (`guardian_prices_{ts}.jsonl`), keeping the event log human-scannable.

### Manager Lifecycle (`guardian/manager.py`)

- **Daemon thread:** `daemon=True` ensures the thread dies with the parent process.
- **Credential validation:** Raises `RuntimeError` on missing `POLY_EXEC_*` credentials (lines 115, 127). The orchestrator catches this with a single log line and continues without guardian.
- **Stop cleanup:** `stop()` calls `request_stop()`, closes the logger, and joins the thread with 5s timeout.
- **update_plan threading:** `update_mappings()` does `dict.update()` which is atomic under CPython GIL for individual key writes. The tracker runs in a separate thread but GIL serialization prevents data corruption.

### CLOB Client (`guardian/clob_client.py`)

- **HMAC auth:** Same scheme as Rust `FastClobSubmitClient` — `HMAC-SHA256(secret, timestamp+method+path+body)`. Verified against `dispatch/fast_submit_client.rs`.
- **Sell order signing:** Uses `py_clob_client_v2` SDK via `asyncio.to_thread()` for EIP-712 signing. Non-blocking. Falls back to REST DELETE for cancellation if SDK fails.
- **Error handling:** All CLOB calls return `None`/`False` on failure with logging. No exceptions propagate.
- **Signature type mapping:** `from_env()` correctly derives `signature_type` from `POLY_EXEC_SIGNATURE_TYPE` or infers from `POLY_EXEC_FUNDER` presence.

---

## 4. Race Condition Analysis

### R1. Hotpath fires orders before guardian WS connects

**Scenario:** Hotpath starts and fires a FAK order within the first second. Guardian is still connecting its user/market WebSockets.

**Analysis:** The order is logged to JSONL. The guardian processes it via log tailing (no WS needed). The fill comes via user WS — but the WS isn't connected yet. The REST fallback could recover the fill, but it's disabled when `self.ws` exists (even if not yet connected — `self.ws` is the `PolymarketUserWS` instance, which exists from construction).

**Risk:** Fill data for early orders may be permanently missed if the WS connects after the trade event was already delivered. In practice, the WS connects within 1-2 seconds (handshake + subscribe), and CLOB fill propagation takes a similar time. The window is narrow.

**Assessment:** Low risk — startup orders are rare (subscriptions haven't been sent yet), and Signal 1's 10s window provides time for the WS to connect.

### R2. Two score changes in the same tick batch

**Scenario:** V2 delivers `1-0` then immediately `0-0` in the same frame burst.

**Analysis:** `tail_log_async` processes events sequentially. First tick: score changes to 1-0, ScoreEvent added, no reversal (first increase). Second tick: score changes to 0-0, ScoreEvent added, reversal detected. `_find_original_goal_event` searches backward and finds the 1-0 event. Alert armed correctly.

**Assessment:** Correct — sequential processing handles burst frames.

### R3. `update_plan()` during tick processing

**Scenario:** Orchestrator calls `update_plan()` on the main thread while the guardian thread is processing a tick.

**Analysis:** `update_plan` calls `self._tracker.update_mappings()` which does `dict.update()`. Under CPython GIL, `dict.update()` is atomic at the C level — the guardian thread either sees the old dict state or the new one, never a partial update. The `_on_tick` handler reads `self._game_id_map` via `.get()` which is also GIL-atomic.

**Assessment:** Safe under CPython GIL. Would be unsafe under a no-GIL Python implementation.

### R4. Cold-start at score 2-1, then overturn to 1-1

**Scenario:** Guardian starts mid-game at score 2-1. A VAR review overturns the last home goal: 2-1 → 1-1.

**Analysis:** 
1. First tick at 2-1: `prev` is None. Creates ScoreEvent(home=2, away=1, prev_home=0, prev_away=0). Overturn detection skipped (`prev is not None` fails).
2. Order event logged for goal at 2-1: matched to the cold-start ScoreEvent via timestamp.
3. Score change to 1-1: `prev=(2,1)`, `new=(1,1)`. Reversal detected. `_find_original_goal_event(game, 2, 1, 1, 1)`: finds cold-start event (home=2, away=1, prev_home=0, prev_away=0). Check: `event.home==2==prev_home AND event.away==1==prev_away` → True. `prev_home>new_home AND event.home>event.prev_home` → `2>1 AND 2>0` → True. Returns cold-start event.
4. `_find_affected_orders`: finds orders triggered by cold-start event. Correct — those orders were triggered by the same goal now being overturned.

**Assessment:** Correct for the common case. The synthetic prev=(0,0) in the cold-start ScoreEvent doesn't affect matching because the function only uses `event.home`/`event.away` (the result score), not `event.prev_home`/`event.prev_away`.

### R5. Same game patched multiple times via V2 resolution

**Scenario:** V2 resolution fails on first attempt, succeeds on second with a different fixture_id.

**Analysis:** Each `update_plan()` call merges new mappings via `dict.update()`. If the fixture_id changes between attempts, both old and new fixture_ids would map to the prematch_event_id in `_game_id_map`. The old mapping is harmless (orphaned but never matched). The new mapping correctly resolves ticks with the new fixture_id.

`add_game_id_alias` is called after each `update_plan`, adding `prematch→fixture` mapping. If called twice with different fixture_ids, the second call overwrites the first. Only the latest fixture_id → prematch mapping matters (from `build_game_id_map`); the prematch → latest_fixture direction is set by the second `add_game_id_alias`.

**Assessment:** Safe — dict updates are idempotent for the latest value.

---

## Summary

| # | Severity | Component | Issue |
|---|----------|-----------|-------|
| C1 | Critical | overturn.py | Execution retry never fires — CLOB failure = permanent loss |
| C2 | Critical | overturn.py | `_find_original_goal_event` fails when opposing side scores between goal and VAR |
| C3 | Critical | overturn.py | VAR `GoalNotAwarded` definitive signal unused (TODO acknowledged) |
| P1 | Potential | tracker.py | GTC partial fills overwritten (not accumulated) in `_on_ws_trade` |
| P2 | Potential | tracker.py | WS trade before log order = silent fill data loss |
| P3 | Potential | tracker.py | Maker-side fills not matched by `taker_order_id` |
| P4 | Cosmetic | manager.py | `add_game_id_alias` docstring misleading (code correct) |
| P5 | Latent | log_tailer.py | No file rotation detection (safe given current architecture) |
| P6 | Design | overturn.py | Signal 2 instant-confirms for pre-existing low bids |
