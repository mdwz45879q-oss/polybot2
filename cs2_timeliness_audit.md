# CS2 Timeliness Audit

Audit of the CS2 evaluation pipeline for earliest-possible bet firing. All code references are to `native/polybot2_native/src/cs2/`.

---

## 1. Timeliness Gaps Found

### GAP-1: Match-deciding map without child_moneyline market (30–100s delay)

**Affects:** Moneyline, totals under, map handicap covers — all match-end evaluators.

**Scenario:** BO3 map 3, and potentially BO1 map 1 or any deciding map where Polymarket doesn't list a per-map winner market. The design doc confirms: *"Polymarket does NOT list a separate map 3 winner market for BO3 matches."*

**Delay:** 30–100 seconds (empirically measured gap between V1 round-13 frame and maps-won counter update).

**Root cause:** The effective_state optimization at `engine.rs:468-494` is gated on `map_winner_resolved[gi][map_idx]` (line 470-471). This flag is only set by `evaluate_child_moneyline_into` at `eval.rs:84`, which requires `map_idx < targets.map_moneyline.len()` (eval.rs:73). When no child_moneyline target exists for the deciding map, Signal 1 never runs for that map, `map_winner_resolved` stays `false`, and effective_state is never computed.

**Walkthrough (BO3 map 3, home wins map 3 at round 13-5):**

```
Tick N: maps=1-1, rounds=13-5, current_map=3
  → child_moneyline: map_idx=2, targets.map_moneyline.len()=2
    → 2 < 2 = false → Signal 1 SKIPPED
  → effective_state: signal1_fired = map_winner_resolved[0][2] = false → None
  → eval_state = raw state (maps 1-1)
  → moneyline: 1 < 2 (mtw) → NO FIRE                  ← MISSED
  → totals under: not match end → NO FIRE              ← MISSED
  → map handicap covers: not decided → NO FIRE         ← MISSED

Tick N+X (30-100s later): maps=2-1, freeText="Closed"
  → moneyline: 2 >= 2 → FIRES                          ← LATE
  → totals under: match_completed → FIRES               ← LATE
  → map handicap covers: match_decided → FIRES          ← LATE
```

**Financial impact:** Every BO3 that goes to map 3 (estimated ~40-50% of BO3 matches based on typical CS2 statistics) delays moneyline/under/handicap-covers by 30-100s. At current market depths, this delay means competing bots fill the best prices, costing an estimated $50-200+ per occurrence in slippage or missed fills.

**Fix:** Decouple map-winner detection from child_moneyline market existence. In `process_tick_live`, check `map_winner()` for the current map unconditionally and set `map_winner_resolved` based on the result:

```rust
// In process_tick_live, BEFORE evaluate_child_moneyline_into:
if state.current_map > 0 {
    let map_idx = (state.current_map - 1) as usize;
    if map_idx < self.map_winner_resolved[gi].len()
        && !self.map_winner_resolved[gi][map_idx]
    {
        if map_winner(state.rounds_home, state.rounds_away).is_some() {
            self.map_winner_resolved[gi][map_idx] = true;
        }
    }
}
// Then evaluate_child_moneyline_into still runs normally,
// reading map_winner_resolved to skip already-resolved maps.
// The effective_state check at line 470 now sees the flag even
// when no child_moneyline market exists.
```

This adds one `map_winner()` call per non-deduped tick (pure arithmetic, <10ns) and eliminates the 30-100s delay for all BO3 map-3 and BO1 scenarios.

**Code locations:**
- Gate: `engine.rs:470-471` (`signal1_fired` check)
- Flag set: `eval.rs:84` (only inside child_moneyline evaluator)
- Fix target: `engine.rs:461-462` (insert unconditional map_winner check before evaluators)

---

## 2. Verified Timely

### 2.1 Child Moneyline — Signal 1 (round-level detection)

**Fires at earliest possible moment.** `map_winner()` at `eval.rs:30-47` is a pure function of `(rounds_home, rounds_away)` that fires on the exact frame where the map is decided — no buffering, no waiting for a subsequent signal.

**Regulation proof (12-7 → 13-7):**
```
map_winner(13, 7): big=13, small=7
  Regulation: big == 13 && small <= 11 → true
  Returns Some("home") → fires immediately
```

**Overtime proof (15-14 → 16-14, OT1 win):**
```
map_winner(16, 14): big=16, small=14, d=2
  Overtime: big >= 16 && big % 3 == 1 && d >= 2 && d <= 4
          → true     && true           && true   && true → true
  Returns Some("home") → fires immediately
```

**Overtime non-fire proof (15-15 → 16-15, OT2 at 1-0):**
```
map_winner(16, 15): big=16, small=15, d=1
  d >= 2 → false → returns None. Correctly does NOT fire.
```

**Trap case proof (17-15, OT2 at 2-0):**
```
map_winner(17, 15): big=17, d=2
  big % 3 == 1 → 17 % 3 = 2 ≠ 1 → false → returns None.
  Correctly does NOT fire (this is mid-OT2, not decided).
```

### 2.2 Child Moneyline — Signal 2 (maps-won fallback)

**Fires at earliest possible moment for Behavior B.** Signal 2 at `eval.rs:90-118` detects `maps_home > prev_home` or `maps_away > prev_away` and fires on the exact tick that V1 increments the maps-won counter.

**Double-fire prevention:** When Signal 1 fires first, it sets `map_winner_resolved[gi][map_idx] = true` (eval.rs:84). Signal 2 checks `!self.map_winner_resolved[gi][map_idx]` (eval.rs:76/113) and skips. Verified by test `child_ml_no_double_fire` (eval.rs:537-554).

### 2.3 Effective Maps Optimization (when child_moneyline market exists)

**Fires at earliest possible moment for maps 1 and 2 in BO3 (and maps 1-4 in BO5).** When Signal 1 detects a map winner and the maps counter hasn't caught up (`map_number > maps_already_counted`), the effective_state projects the corrected maps count. All match-end evaluators evaluate against this projection and fire on the same tick as the child_moneyline intent.

**Proof (BO3 sweep, round-13 on map 2, maps still 1-0):**
```
engine.rs:468-494:
  current_map=2, map_idx=1
  signal1_fired = map_winner_resolved[0][1] = true (just set by child_moneyline)
  maps_already_counted = 1+0 = 1
  map_number = 2, 2 > 1 → compute effective
  map_winner(13, 7) = "home" → eff_home=2, eff_away=0
  eval_state = {maps_home=2, maps_away=0, total_maps=2, ...}

  → moneyline: 2 >= 2 (mtw) → FIRES on same tick ✓
  → totals under: is_match_end=true, total=2 → FIRES on same tick ✓
  → map_handicap: match_decided=true → FIRES on same tick ✓
```

Verified by test `match_end_fires_on_round13_signal` (eval.rs:843-876).

### 2.4 Totals Over — Guaranteed Certainty

**Fires at earliest possible moment.** The guaranteed-certainty formula at `eval.rs:183-198` uses `min(maps_home, maps_away) >= N + 1 - maps_to_win` instead of waiting for the Nth map to complete.

**BO3 over 2.5 proof (fires at 1-1):**
```
half_int = 2, mtw = 2
min_needed = max(2 + 1 - 2, 0) = 1
At maps 1-1: min(1, 1) = 1 >= 1 → FIRES ✓
At maps 1-0: min(1, 0) = 0 < 1 → correctly does NOT fire
```

**BO5 over 3.5 proof (fires at 1-1):**
```
half_int = 3, mtw = 3
min_needed = max(3 + 1 - 3, 0) = 1
At 1-1: min(1,1) = 1 >= 1 → FIRES ✓
```

**BO5 over 4.5 proof (fires at 2-2):**
```
half_int = 4, mtw = 3
min_needed = max(4 + 1 - 3, 0) = 2
At 2-2: min(2,2) = 2 >= 2 → FIRES ✓
At 2-1: min(2,1) = 1 < 2 → correctly does NOT fire
```

**With effective maps:** When Signal 1 detects away wins map 2 at maps=1-0, effective_state has maps=1-1 and total_maps=2. The totals evaluator sees `total_now=2 > prev_total=1` and checks `min(1,1)=1 >= 1` → fires over 2.5 on the same tick as the child_moneyline intent.

### 2.5 Totals Under

**Fires at earliest possible moment (with effective maps, when child_moneyline market exists).** Under fires when `match_completed || maps_home >= mtw || maps_away >= mtw` (eval.rs:203-204). With effective maps on the round-13 tick of the deciding map, this condition is true immediately.

### 2.6 Map Handicap Covers

**Fires at earliest possible moment (with effective maps, when child_moneyline market exists).** Same match-decided gate as moneyline. With effective maps, fires on the round-13 tick.

### 2.7 Map Handicap Not-Covers (Early Fire)

**Fires at earliest possible moment.** The early fire at `eval.rs:263-274` uses `max_margin = mtw - opponent_maps` to detect mathematical elimination. With effective maps, this uses the projected opponent_maps count.

**BO3 home -1.5 proof (fires at effective maps 0-1):**
```
After Signal 1 detects away wins map 1 → effective maps 0-1:
  opponent_maps (for Home) = maps_away = 1
  max_margin = 2 - 1 = 1
  1 + (-1.5) = -0.5 ≤ 0 → FIRES not_covers ✓
```

Fires on the same tick as the child_moneyline intent for map 1.

### 2.8 Moneyline

**Fires at earliest possible moment (with effective maps, when child_moneyline market exists).** Checks `maps_home >= mtw || maps_away >= mtw || match_completed` (eval.rs:144). With effective maps on the round-13 tick of the deciding map, the first condition is true immediately.

**`match_completed` fallback:** If `freeText="Closed"` arrives with maps already at or past mtw, this fires correctly. If `maps < mtw` at match_completed (shouldn't happen), `maps_home > maps_away` determines the winner; tied maps fire nothing (fail-closed at eval.rs:155).

### 2.9 Cold-Start Protection

**Correct.** On first observation (engine.rs:447-458):
- `match_completed` → marks `final_resolved_games` and `totals_under_emitted`, returns None.
- Already-completed maps: tombstones `map_winner_resolved[gi][0..maps_done]` to prevent fallback firing for maps that finished before subscription.
- `prev_total_maps = None` → progressive over evaluator skips entirely (`if let Some(prev_total)` at eval.rs:187 fails).
- First tick does NOT fire any progressive intents. Subsequent ticks with real deltas fire correctly.

---

## 3. Edge Case Analysis

### 3.1 Cold-start at score 1-1 with round data 5-3 on map 3

**Tick 1 (first observation):** maps=1-1, rounds=5-3, current_map=3.
- `is_first_observation = true`.
- Not `match_completed` → continue.
- Tombstone maps 0 and 1: `map_winner_resolved[0][0] = true`, `[0][1] = true`.
- `prev_total_maps = None` → totals over skipped.
- Signal 1: map_idx=2, `map_winner(5, 3)` = None → no fire.
- Signal 2: `prev_maps_home = None` → `prev_home = 0`, `maps_home = 1` → `1 > 0` → completed_map = 0+0+1 = 1, map_idx=0 → tombstoned. Skip.
  - Similarly for away: completed_map = 0+0+1 = 1 → tombstoned. Skip.

Wait — Signal 2's `prev_maps_home` is `None` on first observation (set at engine.rs:433). `prev_home = state.prev_maps_home.unwrap_or(0) = 0`. `maps_home = 1 > 0` → would fire for map 1. But `map_winner_resolved[0][0]` is already tombstoned → skip. Correct.

Similarly, `prev_away = 0`, `maps_away = 1 > 0` → completed_map for away = 0+0+1 = 1 → tombstoned → skip. Correct.

**Result:** No intents fire on first tick. Maps 1 and 2 are tombstoned. Map 3 child_moneyline (if it exists) is available for future firing. Progressive evaluators are blocked by `prev_total_maps = None`. Handicap early-fire is available.

**Tick 2:** If maps unchanged (1-1) with different rounds (e.g., 6-3):
- `prev_total_maps = Some(2)`, `total_now = 2`, `2 > 2` → false → no progressive over fire (correct, nothing changed).
- If rounds reach 13: map_winner fires for map 3. If child_moneyline exists, fires. Effective state computed. Moneyline fires.

### 3.2 V1 delivers maps-won increment AND round-13 on the SAME tick

**Scenario:** maps=2-0 AND rounds=13-7 simultaneously (current_map=2).

```
  Signal 1: map_winner(13, 7) = "home" → fires map 2 home.
            map_winner_resolved[0][1] = true.
  
  Signal 2: maps_home=2 > prev_home=1 → completed_map = 1+0+1 = 2.
            map_idx=1. map_winner_resolved[0][1] = true → SKIP. ✓
  
  effective_state: signal1_fired = true.
    maps_already_counted = 2+0 = 2.
    map_number = 2. 2 > 2 → FALSE. No effective state.
  
  Raw state: maps_home=2 >= mtw=2 → moneyline FIRES directly. ✓
  totals under: maps_home=2 >= mtw=2 → FIRES directly. ✓
  map handicap: match_decided from raw maps → FIRES directly. ✓
```

**Result:** All evaluators fire correctly. The `map_number > maps_already_counted` guard correctly detects that V1 has already counted this map, so no effective state is needed — the raw state suffices.

### 3.3 Overtime: 15-15 (OT1 tied) → 16-15 (OT2 at 1-0)

```
Tick A: rounds=15-15
  map_winner(15, 15): big=15
    Regulation: 15 != 13 → false
    Overtime: 15 >= 16 → false → None ✓

Tick B: rounds=16-15
  map_winner(16, 15): big=16, small=15, d=1
    Overtime: 16 >= 16 → true
             16 % 3 == 1 → true
             d >= 2 → FALSE → None ✓
  Correctly does NOT fire. This is OT2 at 1-0, not decided.
```

### 3.4 BO3 sweep — complete tick-by-tick

All markets: map 1 & 2 child_moneyline (home/away), moneyline (home/away), totals over 2.5, totals under 2.5, map_handicap home -1.5 (covers/not_covers).

```
Tick 0: maps=0-0, rounds=0-0, map=1. First observation.
  → No intents (cold-start, prev_total_maps=None).

Tick 1-19: Map 1 rounds progress. e.g., maps=0-0, rounds=7-5, map=1.
  → map_winner(7,5) = None. No intents.

Tick 20: maps=0-0, rounds=13-7, map=1. ← ROUND-13 MAP 1
  → Signal 1: map_winner(13,7) = "home" → FIRES map 1 home. ✓
  → effective maps: 1-0. moneyline: 1 < 2 → no fire.
    totals: prev=0, now=1. min(1,0)=0 < 1 → no over fire.
    handicap: max_margin=2-0=2, 2+(-1.5)=0.5>0 → no early fire.
  ● Intents: [child_moneyline MAP1 HOME]

Tick 21: maps=1-0, rounds=0-0, map=2. Maps counter updates.
  → Signal 2: maps_home=1>prev=0 → map 1 → tombstoned. Skip. ✓
  → No intents.

Tick 40: maps=1-0, rounds=13-5, map=2. ← ROUND-13 MAP 2
  → Signal 1: map_winner(13,5) = "home" → FIRES map 2 home. ✓
  → effective maps: 2-0.
    moneyline: 2 >= 2 → FIRES home. ✓
    totals under: match_end, total=2, half_int=2, 2>=2 → FIRES. ✓
    handicap: match_decided, margin=2, 2+(-1.5)=0.5>0 → FIRES covers. ✓
  ● Intents: [child_ML MAP2 HOME, moneyline HOME, under 2.5, handicap covers]
  ● final_resolved_games = true.

All subsequent ticks: blocked by final_resolved_games. ✓
```

**Analysis:** Every intent fires at the earliest possible moment. No gaps.

### 3.5 BO3 full distance (home, away, home) — tick-by-tick

```
Tick 0: maps=0-0, rounds=0-0, map=1. First observation. No intents.

Tick 20: maps=0-0, rounds=13-7, map=1. ← HOME WINS MAP 1
  → Signal 1: FIRES map 1 home.
  → effective maps 1-0. Not decided. No other intents.
  ● Intents: [child_ML MAP1 HOME]

Tick 21: maps=1-0, rounds=0-0, map=2. Maps counter update. No intents.

Tick 50: maps=1-0, rounds=9-13, map=2. ← AWAY WINS MAP 2
  → Signal 1: map_winner(9,13) = "away" → FIRES map 2 away.
  → effective maps: 1-1.
    moneyline: 1-1, not decided. No fire. ✓
    totals over 2.5: prev=1, now=2. min(1,1)=1>=1 → FIRES. ✓
    handicap not_covers: opponent=1, max_margin=2-1=1, 1+(-1.5)=-0.5≤0 → FIRES. ✓
  ● Intents: [child_ML MAP2 AWAY, over 2.5, handicap not_covers]

Tick 51: maps=1-1, rounds=0-0, map=3. Maps counter update. No intents.

Tick 80: maps=1-1, rounds=13-5, map=3. ← HOME WINS MAP 3
  → Signal 1: map_idx=2. targets.map_moneyline.len()=2. 2<2=false → SKIPPED.
  → effective_state: signal1_fired = map_winner_resolved[0][2] = false → None.
  → Raw state: maps 1-1, not decided.
  → moneyline: NO FIRE. ← GAP-1 (30-100s delay)
  → totals under: NO FIRE. ← GAP-1
  ● Intents: [] ← EMPTY, should have fired moneyline+under

Tick 81 (30-100s later): maps=2-1, freeText="Closed"
  → moneyline: 2 >= 2 → FIRES home. ← LATE by 30-100s
  → totals under: match_completed → FIRES. ← LATE by 30-100s
  → handicap: match_decided. margin=2-1=1, 1+(-1.5)=-0.5≤0 → not_covers.
    But map_handicap_early_emitted[0] = true (set on tick 50) → enters
    match_decided branch → FIRES not_covers. ✓ (correct outcome, correct timing —
    handicap was already early-fired on tick 50)
  ● Intents: [moneyline HOME, under 2.5]
  ● final_resolved_games = true.
```

**Analysis:** Tick 80 is the critical miss. The moneyline and totals under should fire here but don't, because effective_state can't be computed without a child_moneyline market for map 3.

### 3.6 BO3 with Behavior B on map 2 (Signal 1 skipped)

```
Tick 0: maps=0-0, rounds=0-0, map=1. First observation. No intents.

Tick 20: maps=0-0, rounds=13-7, map=1. Signal 1 fires map 1 home.
  ● Intents: [child_ML MAP1 HOME]

Tick 21: maps=1-0, rounds=0-0, map=2. No intents.

Tick 49: maps=1-0, rounds=12-4, map=2. map_winner(12,4)=None. No intents.

Tick 50: maps=1-1, rounds=0-0, map=3. ← V1 SKIPS ROUND-13 (Behavior B)
  → Signal 1: map=3, map_idx=2, 2 < map_moneyline.len()=2 → false. Skip.
  → Signal 2: maps_away=1 > prev_away=0 →
    completed_map = 1+0+1 = 2, map_idx=1.
    map_winner_resolved[0][1] = false → FIRES map 2 away. ✓
  → effective_state: signal1_fired=false (map 3 not resolved) → None.
  → Raw state: maps 1-1, not decided.
    totals over 2.5: prev=1, now=2. min(1,1)=1>=1 → FIRES. ✓
    handicap not_covers: opponent=1, 1+(-1.5)=-0.5≤0 → FIRES. ✓
  ● Intents: [child_ML MAP2 AWAY, over 2.5, handicap not_covers]

Tick 80: maps=1-1, rounds=13-5, map=3. ← HOME WINS MAP 3
  → Same as Section 3.5 Tick 80: NO FIRE due to GAP-1.
  ● Intents: [] ← EMPTY

Tick 81: maps=2-1, freeText="Closed". LATE fire.
  ● Intents: [moneyline HOME, under 2.5]
```

**Analysis:** Signal 2 correctly catches the Behavior B skip for map 2. Totals over and handicap not_covers fire correctly on the maps counter update (the earliest possible given Behavior B). GAP-1 affects map 3 identically to Section 3.5.

---

## 4. Summary

| Market Type | Evaluation Path | Fires at Earliest? | Notes |
|---|---|---|---|
| **Child moneyline** | Signal 1 (round-13) | ✅ Yes | `map_winner()` fires on exact deciding frame |
| **Child moneyline** | Signal 2 (maps-won) | ✅ Yes | Fires on exact tick of maps counter increment |
| **Moneyline** | Via effective maps (maps 1-2 in BO3) | ✅ Yes | Fires on round-13 tick of child_moneyline |
| **Moneyline** | Deciding map without child_moneyline | ❌ **30-100s late** | **GAP-1** — effective state not computed |
| **Totals over** | Guaranteed certainty | ✅ Yes | `min(h,a) >= N+1-mtw` fires at earliest safe point |
| **Totals over** | With effective maps | ✅ Yes | Projects maps count, fires on same tick as child_ML |
| **Totals under** | Via effective maps (maps 1-2 deciding) | ✅ Yes | Fires on round-13 tick |
| **Totals under** | Deciding map without child_moneyline | ❌ **30-100s late** | **GAP-1** |
| **Map handicap covers** | Via effective maps | ✅ Yes | Fires on round-13 tick |
| **Map handicap covers** | Deciding map without child_moneyline | ❌ **30-100s late** | **GAP-1** |
| **Map handicap not_covers** | Early fire | ✅ Yes | Mathematical elimination fires immediately |
| **Cold-start** | All evaluators | ✅ Correct | Tombstones + prev=None prevent stale firing |
| **Double-fire prevention** | Signal 1 → Signal 2 | ✅ Correct | `map_winner_resolved` flag gates both |

**Total gaps found: 1 (GAP-1), affecting 3 market types in the BO3 map-3 scenario.**
