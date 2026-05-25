# Multiplexed Baseball (V1 + BoltOdds) Correctness Audit

**Date:** 2026-05-25
**Scope:** New BoltOdds baseball integration in the multiplexed hotpath pipeline
**Verdict:** PASS with 1 HIGH **verified bug** (subscription routing), 1 MODERATE gap (walkoff coverage), and minor observations

---

## Summary

The core engine logic, evaluator correctness, cross-provider dedup, and resolution flag sharing are all sound. The BoltOdds baseball frame pipeline is a clean, well-tested addition that correctly extends the existing V1 evaluation with outs-based NRFI and game-end detection.

Two structural concerns warrant attention before live deployment:

1. **HIGH — VERIFIED BUG: BoltOdds WS subscription uses `game_ids()` (primary provider IDs), not BoltOdds game labels.** V1 is the primary provider for baseball, so the BoltOdds subscription sends V1 UUIDs instead of BoltOdds game labels, and BoltOdds delivers zero frames. Traced end-to-end from `config/mappings.py` through the compiler, engine, runtime, and multiplexed worker.

2. **MODERATE: Walkoff games are undetectable when V1 is disconnected.** BoltOdds alone cannot resolve walkoff moneyline/spreads/unders because it doesn't run `evaluate_walkoff_into` and doesn't extract `matchCompleted`.

---

## 1. `boltodds_baseball_types.rs` — Byte-Level Extractor

**Verdict: CORRECT**

- All field extraction follows the established `find_key_*` pattern from the soccer BoltOdds extractor. Key lengths match finder patterns (e.g., `FINDER_OUT` len=5 matches `"out"` + quotes).
- Boolean extraction (`find_key_bool`) correctly handles `true`/`false` by checking the first byte (`b't'` vs `b'f'`) and advancing past the full token. Edge case: truncated JSON where `"topOfInning":tru` reaches EOF — the function returns `None` because the `if p < bytes.len()` guard fails. This is fail-safe (frame rejected, not mis-parsed).
- Integer extraction handles negative values and empty digit runs (returns `None` if no digits follow the colon). The `wrapping_mul`/`wrapping_add` arithmetic is safe for baseball scores (never exceeds `i64` range).
- `find_key_array_second_string` correctly extracts the second element of `matchPeriod: ["BaseballMatchPeriod", "AT_TOP_1ST_INNING"]`. Skips the first string, finds the second quote-delimited value.
- The `"match_update"` guard at line 214 correctly rejects non-game frames (ping, socket_connected, etc.). Test coverage confirms this.
- **Field ordering assumption:** The extractor scans forward from the last found field (`pos = end`). This requires `game` to appear before `out`, which appears before `inning`, etc. in the JSON. The BoltOdds frame format has a consistent field order (`game` → `state:{out, inning, ...}` → `totalRunsForTeamA/B`), so this is safe. A reordered frame would return `None` (fail-safe).

### Home/Away Score Mapping

The extractor hardcodes `totalRunsForTeamA → home_score` and `totalRunsForTeamB → away_score`. It does NOT parse the `designation` field dynamically. This matches the soccer BoltOdds extractor (`boltodds_types.rs`), which also hardcodes `goalsA → home`, `goalsB → away`.

**Risk assessment:** All observed BoltOdds frames show `"designation":{"A":"home","B":"away"}`. The CLAUDE.md documents that BoltOdds can publish duplicate entries with flipped home/away, but these have *different game labels* (e.g., "Sunderland vs Man Utd" vs "Man Utd vs Sunderland"). The linker's home/away ordering check (soccer only) rejects flipped duplicates. For baseball, if the linker selects the correctly-ordered game label, the `A=home` invariant holds.

**Residual risk:** If BoltOdds ever sends a frame with `"designation":{"A":"away","B":"home"}` for the *correctly-linked* game label, home/away scores would be swapped silently. Moneyline would fire for the wrong team. This is LOW probability but HIGH impact. Consider adding a runtime assertion or logging check on the `designation` field.

---

## 2. `boltodds_baseball_frame_pipeline.rs` — Frame Pipeline

**Verdict: CORRECT**

The pipeline is a clean 4-step chain: `fast_extract → check_boltodds_game → process_boltodds_tick_live → dispatch_intents`. Each step returns `Option` and short-circuits on failure.

- `BoltOddsBaseballPendingLog` carries `GameIdx` + `GameState` (both `Copy`). No heap allocation on the pipeline path.
- `dispatch_intents` is called only when intents are non-empty (line 41). This avoids entering the dispatch path for deduped or no-op frames.
- The return type (`Option<BoltOddsBaseballPendingLog>`) correctly carries the deferred log data for the caller to flush after the drain loop.
- Uses the shared `dispatch_intents` from `dispatch/flow.rs` — same path as soccer BoltOdds. Noop-mode logging and presign pool pops work identically.

---

## 3. `baseball/engine.rs` — Engine Methods

### 3a. `check_boltodds_game(game_label)` — Line 429

**Verdict: CORRECT**

Simple delegation to `game_id_to_idx.get(game_label).copied()`. BoltOdds game labels must be present in `game_id_to_idx` (via `alternate_provider_game_ids` in `load_plan_from_json` at line 135 or `merge_plan` at line 662). Verified: both `load_plan_from_json` and `merge_plan` insert alternate IDs into `game_id_to_idx`.

No separate `boltodds_label_to_idx` map — baseball uses the unified `game_id_to_idx` for all providers. This is simpler than the soccer engine's approach but functionally equivalent since the map accepts any string key.

### 3b. `process_boltodds_tick_live(...)` — Lines 576-635

**Verdict: CORRECT — the critical method is sound.**

**Dedup (lines 592-603):** Integer-based `BoltOddsBaseballRow` comparison via `PartialEq`. Includes `outs`, `strikes`, `inning`, `top_of_inning`, `home_score`, `away_score`. Notably excludes `ball` count — this is correct because ball-count-only changes are irrelevant to all evaluators. Strike changes pass through (needed for strikeout pre-fire). Uses `bo_rows[gi]` (separate from V1's `rows[gi]`) — no cross-provider dedup interference.

**GameState construction (lines 606-621):**
- `home`, `away`, `total`: computed directly from BoltOdds frame data (not merged with V1's value). This is correct — BoltOdds provides authoritative score data.
- `prev_total`: reads from `prev.total` (shared `game_states[gi]`). This is critical for cross-provider dedup: if V1 already updated the total, BoltOdds's `prev_total` reflects V1's value, preventing duplicate over-crossings.
- `inning_number`, `inning_half`: set from BoltOdds data. `top_of_inning: true → "top"`, `false → "bottom"`.
- `match_completed`: preserves V1's value (`prev.match_completed`). BoltOdds doesn't signal match completion — this is intentional and documented.
- `game_state`: preserves V1's value (`prev.game_state`). Same rationale.
- `outs`, `strikes`: set from BoltOdds data (`Some(outs)`, `Some(strikes)`).

**Evaluators called (lines 624-627):**
1. `evaluate_totals_into` — shared with V1, works via `prev_total` delta detection.
2. `evaluate_nrfi_from_outs_into` — BoltOdds-specific, fires on `outs=3` bottom of 1st.
3. `evaluate_game_end_from_outs_into` — BoltOdds-specific, fires on `outs=3` in 9th+ inning.

NOT called: `evaluate_walkoff_into`, `evaluate_final_into`, `evaluate_nrfi_into`. This is by design — walkoffs are handled by V1 (faster for run deltas), and V1's `evaluate_final` triggers on `match_completed`.

### 3c. `load_plan` / `merge_plan` — Alternate ID insertion

**Verdict: CORRECT**

Both `load_plan_from_json` (line 135) and `merge_plan` (line 662) insert `alternate_provider_game_ids` into `game_id_to_idx`. The `contains_key` guard prevents overwriting existing entries. Both also initialize `bo_rows` alongside `rows` (line 333 and no-op for merge since game count doesn't change).

`reset_runtime_state` (line 78) clears `bo_rows` alongside all other state vectors.

---

## 4. `baseball/eval.rs` — Evaluation Functions

### 4a. `evaluate_nrfi_from_outs_into` — Lines 224-279

**Verdict: CORRECT**

**First-inning observation gate (lines 240-253):** Identical logic to V1's `evaluate_nrfi_into`. Late subscriptions (inning > 1) permanently set `nrfi_resolved_games[gi] = true`. First observation of inning 1 sets `nrfi_first_inning_observed[gi] = true`.

**Bottom-of-1st guard (lines 257-259):** Only fires when `inning_number == 1 AND inning_half == "bottom"`. Top of 1st is correctly blocked (away team's half isn't the end of the first inning). Inning > 1 returns early (NRFI would already be resolved by the gate above).

**Outs signal (lines 263-264):** Two conditions:
- `outs == 3`: half-inning definitively over.
- `outs == 2 AND strikes == 3`: strikeout pre-fire (~2% of cases, +4-6s edge). Documented tradeoff: if the strikeout is reversed (extremely rare), the pre-fired intent is irrevocable. Fail-safe: presign pool prevents double-fire from the later correct frame.

**Resolution (lines 269-278):**
- `total > 0 → nrfi_yes` (runs scored in first inning — bet fires)
- `total == 0 → nrfi_no` (clean first inning — bet fires)
- Consistent with V1's NRFI semantics. Sets `nrfi_resolved_games[gi] = true` to block both V1 and future BoltOdds evaluations.

**Edge cases verified:**
- `outs=3, inning=1, top=true, score=0-0`: returns early at line 257 (not bottom). Correct — top of 1st, away team done, but home team hasn't batted yet.
- `outs=3, inning=1, bottom=false (top), score=0-0`: same as above. Correct.
- `outs=3, inning=1, bottom=true, score=0-0`: fires `nrfi_no`. Correct — clean first inning.
- `outs=3, inning=1, bottom=true, score=1-0`: fires `nrfi_yes`. Correct — run scored.
- Sac fly on third out (run scores on the out): BoltOdds frame has `outs=3` and the updated score simultaneously. `total > 0` fires `nrfi_yes`. Correct.
- Late subscription (inning=3 on first frame): gate sets `nrfi_resolved_games = true`, blocks forever. Correct.

### 4b. `evaluate_game_end_from_outs_into` — Lines 286-370

**Verdict: CORRECT**

**Pre-conditions (lines 293-309):**
- `has_final[gi]` check prevents evaluation when no moneyline/spread targets exist.
- `final_resolved_games[gi]` check prevents double-fire (shared with V1's `evaluate_final`).
- `inning < 9`: returns early — game-end impossible before 9th.
- Outs signal: same `outs=3 OR (outs=2 AND strikes=3)` logic as NRFI.

**Winner determination (lines 312-329):**
- `top AND home > away`: Home wins. Correct — away team batted in top of 9th+, couldn't overcome home's lead. Home doesn't bat (skips bottom).
- `bottom AND away > home`: Away wins. Correct — home team batted in bottom of 9th+, couldn't catch up. Game over.
- All other cases (tied, or leading team's half): returns early. Correct — game continues.

**Key edge case: bottom of 9th, home leads, outs=3.**
This case returns early (line 329: "tied or game continues"). At first glance this looks like a bug (home won, game should be over), but it's actually unreachable in valid baseball: if home leads going into the bottom of the 9th, they don't bat — the game ends after the top. If home takes the lead DURING the bottom (walkoff), the game ends on the run, not on 3 outs. So `bottom + home > away + outs=3` is an impossible state in real baseball. The test `game_end_from_outs_bottom9_home_ahead_no_fire` (line 2263) confirms this is intentional.

**Spread evaluation (lines 342-354):** Same margin logic as `evaluate_final_into`. `margin_home = home - away`. For `SpreadSide::Away`, margin is negated. `(margin as f64) + slot.line > 0.0` → covers.

**Under emission (lines 357-367):** Checks `totals_final_under_emitted[gi]` to prevent double-fire. Sets it to `true` after emitting. Shared with `evaluate_totals_into`'s under emission at game completion.

**Edge cases verified:**
- `top 9, home=5, away=2, outs=3`: home wins, fires moneyline_home + spreads + under. Test at line 2062.
- `bottom 9, home=1, away=4, outs=3`: away wins, fires moneyline_away. Test at line 2108.
- `top 9, home=3, away=3, outs=3`: tied, no fire (extras). Test at line 2163.
- `inning=8, outs=3`: no fire (too early). Test at line 2188.
- `bottom 10, home=4, away=5, outs=3`: extras, away wins. Test at line 2211.
- Already resolved: no fire. Test at line 2239.

### 4c. Shared Resolution Flags

**Verdict: CORRECT — Cross-provider blocking works.**

All resolution flags are in `NativeMlbEngine` and shared between V1 and BoltOdds:
- `nrfi_resolved_games[gi]`: checked by both `evaluate_nrfi_into` (V1) and `evaluate_nrfi_from_outs_into` (BoltOdds). Whichever fires first sets it.
- `final_resolved_games[gi]`: checked by `evaluate_final_into` (V1), `evaluate_walkoff_into` (V1), and `evaluate_game_end_from_outs_into` (BoltOdds).
- `totals_final_under_emitted[gi]`: checked by `evaluate_totals_into` (shared) and `evaluate_game_end_from_outs_into` (BoltOdds).
- `nrfi_first_inning_observed[gi]`: set by either evaluator on first inning-1 observation.

Tests at lines 2436-2509 explicitly verify cross-provider blocking:
- `boltodds_nrfi_blocks_v1_nrfi`: BoltOdds resolves NRFI → V1 evaluator returns empty.
- `boltodds_game_end_blocks_v1_final`: BoltOdds resolves game end → V1 `evaluate_final` returns empty.

---

## 5. `baseball/types.rs` — GameState and BoltOddsBaseballRow

**Verdict: CORRECT**

- `GameState` includes `outs: Option<u8>` and `strikes: Option<u8>` (lines 72-74), documented as BoltOdds-specific fields. `None` when last tick was V1 — verified by V1's `process_tick_live` which sets `outs: None, strikes: None` (line 538-539 of engine.rs).
- `BoltOddsBaseballRow` (lines 80-88): `PartialEq + Eq` for dedup comparison. All fields are `Copy`. Correct.
- `NativeMlbEngine` includes `bo_rows: Vec<Option<BoltOddsBaseballRow>>` (line 127). Initialized in `load_plan_from_json` (line 333) and `reset_runtime_state` (line 79).

---

## 6. `ws_boltodds.rs` — Standalone BoltOdds WS Worker

**Verdict: CORRECT**

**SportEngine dispatch (lines 226-249, 251-276):** Both `Message::Text` and `Message::Binary` arms have `match engine` with:
- `SportEngine::Soccer` → `process_boltodds_frame_sync` (soccer pipeline)
- `SportEngine::Baseball` → `process_boltodds_baseball_frame_sync` (baseball pipeline)
- `_ => {}` — catches Tennis (BoltOdds doesn't support tennis). Silent drop is correct.

**Pending log flush (lines 292-353):** Separate handling for `pending_soccer_logs` and `pending_baseball_logs`:
- Soccer logs use `TickPayload::Soccer` with `src: "boltodds"`.
- Baseball logs use `TickPayload::Baseball` with `runs_home`/`runs_away`, `inn`/`inn_half`, `gs`. No `src` field in baseball TickPayload (unlike soccer) — minor inconsistency but not a bug.

**Monotonic clock (line 224):** `worker_clock_origin.elapsed().as_nanos() as i64`. Set at worker startup (line 50). All frame timestamps use this origin. No wall-clock leakage.

---

## 7. `ws_multiplexed.rs` — Multiplexed Worker

**Verdict: CORRECT (dispatch logic), with HIGH concern on subscription routing**

### BoltOdds Dispatch Sites — All 6 verified

| Location | Frame type | Soccer arm | Baseball arm | Catch-all |
|----------|-----------|------------|-------------|-----------|
| Burst drain, line 445 | Text | `process_boltodds_frame_sync` | `process_boltodds_baseball_frame_sync` | `_ => {}` |
| Burst drain, line 465 | Binary | `process_boltodds_frame_sync` | `process_boltodds_baseball_frame_sync` | `_ => {}` |
| Select!, line 617 | Text | `process_boltodds_frame_sync` | `process_boltodds_baseball_frame_sync` | `_ => {}` |
| Select!, line 638 | Binary | `process_boltodds_frame_sync` | `process_boltodds_baseball_frame_sync` | `_ => {}` |

All 4 BoltOdds dispatch sites correctly match on both `SportEngine::Soccer` and `SportEngine::Baseball`.

### V1 Dispatch Sites — Both verified

| Location | Frame type | Soccer arm | Baseball arm |
|----------|-----------|------------|-------------|
| Burst drain, line 411 | Text | `soccer::frame_pipeline::process_decoded_frame_sync` | `baseball::frame_pipeline::process_decoded_frame_sync` |
| Select!, line 569 | Text | `soccer::frame_pipeline::process_decoded_frame_sync` | `baseball::frame_pipeline::process_decoded_frame_sync` |

V1 only handles Text (no Binary arm needed — Kalstrop V1 GraphQL WS always sends Text).

### V2 Dispatch Sites

V2 only processes `SportEngine::Soccer`. This is correct — V2 doesn't support baseball (documented in CLAUDE.md). V2 frames arriving when `engine` is `SportEngine::Baseball` are silently ignored.

### Pending Baseball BoltOdds Logs

`pending_baseball_bo_logs` (line 350-351): correctly declared, populated from all 4 BoltOdds dispatch sites, flushed at lines 728-748 inside `if let SportEngine::Baseball(ref e) = engine`. The `.clear()` at line 747 is technically redundant (the vector is scoped to the event loop iteration) but harmless.

### HIGH — VERIFIED: BoltOdds Subscription Routing Is Broken

**File:** `runtime.rs:336`
**Status:** Confirmed bug. Traced end-to-end on 2026-05-25.

```rust
let game_labels: Vec<String> = worker_engine.game_ids().to_vec();
```

**Full trace:**

1. `config/mappings.py:20` — MLB config: `"provider": ["kalstrop_v1", "boltodds"]`. V1 is first.
2. `commands_hotpath_runtime.py:42` — `_primary_provider_for_league` takes `providers[0]` → `"kalstrop_v1"`.
3. `compiler.py:817` — compiled plan: `provider_game_id` = V1 UUID (e.g. `"d3f41158-..."`). BoltOdds game labels (e.g. `"ATL Braves vs NY Mets"`) go into `alternate_provider_game_ids`.
4. `baseball/engine.rs:130` — `game_ids.push(uid)` stores V1 UUIDs. Alternates go only into `game_id_to_idx` (line 139), not `game_ids`.
5. `lib.rs:208` — `game_ids()` returns `&e.game_ids` → V1 UUIDs only.
6. `runtime.rs:336` — `worker_engine.game_ids().to_vec()` → V1 UUIDs passed as `game_labels`.
7. `ws_multiplexed.rs:117` — subscribes: `{"action":"subscribe","filters":{"games":["d3f41158-..."]}}`.
8. BoltOdds server does not recognize V1 UUIDs → silently returns zero frames.

**Note:** The engine-side resolution works correctly — `game_id_to_idx` contains BoltOdds labels (via `alternate_provider_game_ids`), so `check_boltodds_game()` would resolve frames fine. The bug is purely in subscription: wrong IDs are sent to the BoltOdds WS server.

**Impact:** BoltOdds provides no frames, all BoltOdds evaluators (NRFI-from-outs, game-end-from-outs) never execute. The system silently degrades to V1-only operation. No error is logged — the BoltOdds connection sits idle for the entire session.

**Fix:** `runtime.rs:336` must extract BoltOdds game labels from the plan's `alternate_provider_game_ids` (where `provider == "boltodds"`) rather than from `game_ids()`. The engine already has these labels in `game_id_to_idx` — only the subscription routing is wrong.

---

## 8. `dispatch/flow.rs` — `dispatch_intents`

**Verdict: CORRECT**

BoltOdds baseball uses `dispatch_intents` from `flow.rs:92` (called at `boltodds_baseball_frame_pipeline.rs:42`). This is the same function used by soccer BoltOdds. Both noop and http modes work correctly:
- Noop: logs `"noop"` per intent.
- Http: pops from presign pool → builds batch → sends batch. Presign miss → logs error (fail-closed).

V1 baseball (`baseball/frame_pipeline.rs`) uses inline dispatch (lines 189-212) rather than `dispatch_intents`. This is because V1 batches intents across multiple frames (batch KalstropFrame arrays), while BoltOdds processes one frame at a time. Both approaches are correct for their use cases.

---

## 9. Cross-Provider Race Condition Analysis

### Scenario A: Both providers deliver the same score change

**V1 first, BoltOdds second:**
1. V1 frame: `game_states[gi].total` updated from `Some(2)` to `Some(3)`. Over 2.5 fires.
2. BoltOdds frame: `prev = game_states[gi]` where `total = Some(3)`. `state.total = Some(3)`, `prev_total = Some(3)`. `evaluate_totals_into`: `3 > 3` → false → no crossing. Correct.

**BoltOdds first, V1 second:**
1. BoltOdds frame: `game_states[gi].total` updated from `Some(2)` to `Some(3)`. Over 2.5 fires.
2. V1 frame: `process_tick_live` reads `prev = game_states[gi]` where `total = Some(3)`. `state.total = Some(3)`, `prev_total = Some(3)`. No crossing. Correct.

**Mechanism:** The shared `game_states[gi]` acts as the cross-provider dedup layer. Whichever provider updates the total first prevents the other from seeing a delta.

### Scenario B: BoltOdds delivers NRFI signal, V1 delivers inning transition

**BoltOdds first:** `evaluate_nrfi_from_outs_into` fires NRFI NO at `outs=3, bottom 1, total=0`. Sets `nrfi_resolved_games[gi] = true`.
**V1 later:** `evaluate_nrfi_into` checks `nrfi_resolved_games[gi]` → `true` → returns early. No duplicate.

**V1 first:** `evaluate_nrfi_into` fires NRFI NO at inning transition to 2nd. Sets `nrfi_resolved_games[gi] = true`.
**BoltOdds later:** `evaluate_nrfi_from_outs_into` checks `nrfi_resolved_games[gi]` → `true` → returns early. No duplicate.

### Scenario C: BoltOdds delivers game-end, V1 delivers "Ended"

**BoltOdds first:** `evaluate_game_end_from_outs_into` fires at `outs=3, top 9, home > away`. Sets `final_resolved_games[gi] = true` and `totals_final_under_emitted[gi] = true`.
**V1 later:** `evaluate_final_into` checks `final_resolved_games[gi]` → `true` → returns early. `evaluate_totals_into` checks `totals_final_under_emitted[gi]` → `true` → no under emission. No duplicates.

### Scenario D: Presign pool as final dedup gate

Even if both providers somehow produce the same `TargetIdx` intent (e.g., walkoff from V1 + game-end-from-outs — though this can't happen due to the walkoff exclusion), the presign pool's `Option::take()` ensures only the first `pop_for_target` succeeds. The second returns `Err("submit_presigned_miss")`. This is the ultimate fail-safe.

### Dedup Mechanism Independence

- V1 dedup: `rows[gi]` (string-based: `home_score_raw`, `away_score_raw`, `free_text_raw`)
- BoltOdds dedup: `bo_rows[gi]` (integer-based: `outs`, `strikes`, `inning`, `top_of_inning`, `home_score`, `away_score`)
- Engine-level dedup: `game_states[gi]` (shared `prev_total` for totals; resolution flags for NRFI/final)

Three independent layers. V1 and BoltOdds dedup cannot interfere with each other (different vectors). The engine-level dedup catches any frames that pass provider-level dedup but represent no state change.

---

## 10. Monotonic Clock Consistency

**Verdict: CORRECT**

Both `ws_boltodds.rs` (line 50) and `ws_multiplexed.rs` (line 157) create `worker_clock_origin = Instant::now()` at worker startup. All frame timestamps use `worker_clock_origin.elapsed().as_nanos() as i64`.

In the multiplexed worker, V1, V2, and BoltOdds frames all use the same `worker_clock_origin` (shared across the `select!` arms). No wall-clock timestamps in engine math.

---

## 11. MODERATE Concern: Walkoff Coverage Gap

When V1 is disconnected and only BoltOdds is streaming:

1. **Walkoff detection:** `evaluate_walkoff_into` is V1-only (not called from `process_boltodds_tick_live`). BoltOdds delivers the score change (home takes lead in bottom 9+) but doesn't fire moneyline_home via walkoff logic.

2. **Game completion:** BoltOdds doesn't extract `matchCompleted` from frames. The engine's `match_completed` stays `None` (or whatever V1 last set). `evaluate_final_into` requires `match_completed == true` to fire.

3. **BoltOdds game-end evaluator:** `evaluate_game_end_from_outs_into` explicitly skips `bottom + home > away` (the walkoff scenario, line 328). This is correct for the normal case (V1 handles walkoffs faster), but means BoltOdds alone cannot detect walkoffs at all.

**Impact:** If V1 disconnects during a walkoff, moneyline/spreads/unders for that game are never resolved. Presign pool orders expire unfilled. No incorrect bets are placed (fail-safe), but winning opportunities are missed.

**Mitigation options:**
1. Add `evaluate_walkoff_into` to the BoltOdds evaluation chain (as a slower fallback). Since V1 is faster for run deltas, V1's walkoff intent would always fire first in the normal case, and the BoltOdds walkoff intent would find the presign pool empty. Only when V1 is disconnected would BoltOdds's walkoff fire.
2. Extract `matchCompleted` from BoltOdds frames (it's present in the frame but currently not parsed by the extractor) and use it to set `match_completed` on the engine state. This would allow `evaluate_final_into` to fire as a fallback.
3. Use `period_detail == "MATCH_COMPLETED"` (already extracted but unused) as a game-completion signal.

---

## 12. Minor Observations

### 12a. `period_detail` extracted but unused

`BoltOddsBaseballExtract.period_detail` is extracted (e.g., `"AT_TOP_1ST_INNING"`, `"MATCH_COMPLETED"`) but never consumed by the engine. Currently for logging/debugging only. Could be used for game-completion detection (see concern #11).

### 12b. V1 clears outs/strikes on GameState

When a V1 frame arrives after a BoltOdds frame, `process_tick_live` sets `outs: None, strikes: None` (line 538-539 of engine.rs). This is correct — V1 doesn't provide outs data, so the state should reflect "unknown outs" after a V1 tick. The BoltOdds evaluators check `state.outs == Some(3)` which requires a BoltOdds-originated state.

Test `v1_tick_clears_outs_and_strikes` (line 2405) verifies this.

### 12c. BoltOdds `matchCompleted` frames are processed

When BoltOdds sends a `MATCH_COMPLETED` frame (test at line 322), the extractor successfully parses it (`outs=0, inning=9, period_detail="MATCH_COMPLETED"`). The engine processes this as a normal tick with `outs=0` — no evaluator fires (outs < 3, not a strikeout pre-fire). The frame is effectively a no-op. This is safe but represents a missed opportunity for fallback game-completion detection.

### 12d. `bo_rows` cleanup in `cleanup_completed_game_idx`

Line 407 of engine.rs: `self.bo_rows[gi] = None`. This correctly resets the BoltOdds dedup row alongside the V1 dedup row when a game is cleaned up. Post-cleanup BoltOdds frames for the same game would pass dedup but find the game marked as final-resolved (tombstone preserved). Correct.

### 12e. No `src` field in baseball tick logs

Soccer tick logs include `src: "boltodds"` to identify the frame source. Baseball tick logs (`TickPayload::Baseball`) don't have an `src` field. This means the log observer (`live_observer.py`) cannot distinguish V1 vs BoltOdds baseball ticks. Not a correctness issue, but reduces observability. Consider adding `src` to `TickPayload::Baseball`.

---

## Test Coverage Assessment

The engine tests are thorough:
- 13 `evaluate_nrfi_from_outs` tests covering all edge cases (lines 1823-2055)
- 9 `evaluate_game_end_from_outs` tests covering all scenarios (lines 2060-2327)
- 6 `process_boltodds_tick_live` tests covering dedup, state, and totals (lines 2333-2402)
- 3 cross-provider blocking tests (lines 2436-2509)
- 7 extractor tests with real BoltOdds frame data (boltodds_baseball_types.rs, lines 262-350)

Missing test coverage:
- No integration test combining V1 + BoltOdds ticks in sequence through `process_tick_live` + `process_boltodds_tick_live` on the same engine (the cross-provider tests call evaluators directly, not through the full tick methods).
- No test for `process_boltodds_tick_live` when `prev_total` is `None` (cold start).
- No test for BoltOdds tick arriving after V1 has set `match_completed = true`.
