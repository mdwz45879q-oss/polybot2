# Multiplexed Baseball Evaluation Audit

Audited files: `baseball/eval.rs`, `baseball/engine.rs`, `baseball/types.rs`,
`boltodds_baseball_frame_pipeline.rs`, `boltodds_baseball_types.rs`,
`baseball/frame_pipeline.rs`.

Design doc: `multiplexed_baseball_design.md`.

Capture data: 7 MLB games from `captures/2026_05_21/`, 5 complete
(all away-team wins in bottom 9th).

---

## 1. Critical Issues

**None found.**

---

## 2. Potential Issues

### 2.1 Design doc / code discrepancy: NRFI outs signal

**File:** `multiplexed_baseball_design.md:262`

**Design doc says:**
```
Outs signal: `outs == 3` OR (`outs == 2 AND strikes == 3`)
```

**Code says** (`eval.rs:370`):
```rust
if state.outs != Some(3) {
    return;
}
```

The **code is correct** — section 2 of the same design doc explicitly explains
`strike=3` has a 68% false positive rate and should not be used. The NRFI
outs-based section at line 262 was not updated to match. This is a
documentation-only issue — no code change needed. The design doc should be
updated to remove the `OR (outs == 2 AND strikes == 3)` clause from the NRFI
section to match the code and the empirical findings in section 2.

**Risk:** A future developer reading only section "Detailed Evaluator
Conditions" could re-add the `strike=3` pre-fire logic, believing the code
diverged from design intent.

---

## 3. Real-Data Trace-Throughs

### 3.1 Game-ending sequence: DET 1, CLE 3 (away wins, bottom 9th)

**Source:** `captures/2026_05_21/det_tigers_vs_cle_guardians/boltodds_raw.jsonl`

The game-ending `out=3` frame (line 357):
```
inn=9 top=false out=3 strike=3 home=1 away=3 b1=true b2=false b3=false
period=AT_BOT_9TH_INNING matchCompleted=false
ts_ns=1779393060865120868
```

**Evaluator trace through `evaluate_game_end_from_outs_into`:**

1. `final_resolved_games[gi]` — false (no prior resolution). Pass.
2. `has_final[gi]` — true (game has moneyline/spread targets). Pass.
3. `inning = state.inning_number.unwrap_or(0)` → `9`. `inning < 9` → false. Pass.
4. `state.outs != Some(3)` → `Some(3) != Some(3)` → false. Pass.
5. `home = 1, away = 3`.
6. Branch: `state.inning_half == "top" && home > away` → `"bottom" == "top"` → false.
7. Branch: `state.inning_half == "bottom" && away > home` → `"bottom" == "bottom" && 3 > 1` → **true**. `home_wins = false`.

**Intents fired:**
- **Moneyline:** `winner_slot = targets.moneyline_away`. **Correct** — CLE (away) won.
- **Spreads (hypothetical Away -1.5):** `margin_home = 1 - 3 = -2`.
  For `SpreadSide::Away`: `margin = -(-2) = 2`. `(2.0) + (-1.5) = 0.5 > 0.0` → `covers_idx` fires. **Correct** — away won by 2, covers -1.5.
- **Spreads (hypothetical Home +1.5):** For `SpreadSide::Home`: `margin = -2`. `(-2.0) + 1.5 = -0.5 < 0.0` → `not_covers_idx` fires. **Correct** — home lost by 2, does not cover +1.5.
- **Under lines:** `total = 1 + 3 = 4`. Under 8.5 (`half_int=8`): `8 >= 4` → fires. **Correct**.
- `final_resolved_games[gi] = true`. `totals_final_under_emitted[gi] = true`.

**V1 "Ended" frame arrives 15.378 seconds later** (ts=1779393076243081768).
`evaluate_final_into` checks `final_resolved_games[gi]` → true → early return.
`evaluate_totals_into` checks `totals_final_under_emitted[gi]` → true → skips
under emission. **Correct** — no double-fire.

**Note on base runner state:** The `out=3` frame has `base1=true`. This does
NOT affect `evaluate_game_end_from_outs_into` — base runner state is only
used by the walkoff evaluator's Tier 2 logic. The game-end evaluator fires
all spreads and unders regardless of base state. **Correct**.

**Result: CORRECT.** Away-team win correctly detected ~15s before V1.

---

### 3.2 NRFI sequence: DET vs CLE (0-0 through bottom of 1st)

**Source:** `captures/2026_05_21/det_tigers_vs_cle_guardians/boltodds_raw.jsonl`

Relevant frames:

| Line | Inning | Half | Outs | Strikes | Home | Away | Period |
|------|--------|------|------|---------|------|------|--------|
| 1 | 1 | top | 0 | 0 | 0 | 0 | AT_TOP_1ST_INNING |
| 13 | 1 | top | 2 | 3 | 0 | 0 | AT_TOP_1ST_INNING |
| 14 | 1 | bottom | 0 | 0 | 0 | 0 | AT_MID_1ST_INNING |
| 43 | 1 | bottom | 3 | 0 | 0 | 0 | AT_BOT_1ST_INNING |
| 44 | 1 | bottom | 0 | 0 | 0 | 0 | AT_END_1ST_INNING |

**Evaluator trace through `evaluate_nrfi_from_outs_into` on line 43:**

1. `nrfi_first_inning_observed[gi]` — set to true on first `inning=1` tick (line 1). Pass.
2. `nrfi_resolved_games[gi]` — false (no prior resolution). Pass.
3. Early YES check: `is_first_inning(state)` → true. `total=0, prev_total=0` → `total > prev` → false. Skip early YES.
4. `state.inning_number != Some(1)` → false. `state.inning_half != "bottom"` → false. Continue.
5. `state.outs != Some(3)` → `Some(3) != Some(3)` → false. Continue.
6. `total = state.total.unwrap_or(0) = 0`.
7. `total > 0` → false. Fire `nrfi_no`. `nrfi_resolved_games[gi] = true`.

**V1 `"Break top 2 bottom 1"` arrives 5.484 seconds later** (ts=1779384575202012443).
V1's `evaluate_nrfi_into` checks `nrfi_resolved_games[gi]` → true → early return. **Correct** — no double-fire.

**Strike wobble on line 13:** `out=2, strike=3` in top of 1st. Under the old
`out=2, strike=3` pre-fire logic, this would have been a false fire point.
The code correctly ignores it because `inning_half == "top"` (NRFI NO only
fires in bottom of 1st). Even if this had been bottom of 1st, the code only
checks `outs == 3`, not `strike == 3`. **Correct** — wobble is harmless.

**Strike wobble on line 20:** `out=1, strike=3` in bottom of 1st. Strike
flashes to 3, reverts to 0 on line 21 without outs incrementing. Under the
old `out=2, strike=3` pre-fire logic this would NOT have triggered (only
`out=1`), but it demonstrates the wobble pattern the design doc describes.

**Result: CORRECT.** NRFI NO fires ~5.5s before V1.

---

### 3.3 Break frame guard verification

**Source:** All 7 games, all `AT_MID_*` and `AT_END_*` frames.

**Finding:** Every break frame across all 7 games (97 break frames observed)
has `outs=0, strikes=0`. The break frame guard at `engine.rs:641-642`:

```rust
let is_break = period_detail.starts_with("AT_MID_")
    || period_detail.starts_with("AT_END_");
```

correctly identifies these frames and skips all evaluators. Confirmed from captures:

- `AT_MID_1ST_INNING` through `AT_MID_9TH_INNING`: all have `out=0, strike=0`
- `AT_END_1ST_INNING` through `AT_END_8TH_INNING`: all have `out=0, strike=0`
- `MATCH_COMPLETED`: has `out=0, strike=0` (also NOT a break frame — correctly not matched by the guard)

**State update before guard:** The `GameState` is updated at `engine.rs:616-631`
**before** the `is_break` check at line 641. This means `prev_total` tracks
correctly across break frames, and the dedup row is updated so the next
active-play frame is not erroneously deduplicated. The tick IS still returned
(line 652-658) so it gets logged.

**Guard scope:** The guard only skips evaluators, not state updates or return
values. Specifically:
- `self.game_states[gi] = state` at line 632 → state is updated. ✓
- `self.bo_rows[gi] = Some(new_row)` at line 611 → dedup row is updated. ✓
- `Some(LiveTickResult { ... })` at line 652 → tick is returned for logging. ✓

**Result: CORRECT.** Break frames are defensively guarded, and all 97
observed break frames have `outs=0, strikes=0` as claimed in the design doc.

---

### 3.4 Strike wobble analysis

**Source:** All 5 complete games.

**Finding:** 38 strike-3 wobble instances observed across 5 games. In every
case, `strike=3` appears for one frame, then `strike` reverts to 0 on the
next frame without a corresponding outs increase. Key examples:

**DET vs CLE, line 329-330 (top of 9th, game-ending inning):**
```
line=329: inn=9 top=True out=1 s=3 → would have been o=1, not o=2, so no pre-fire risk
line=330: inn=9 top=True out=1 s=0 → reverts without out change
```

**DET vs CLE, line 350-351 (bottom of 9th, game-ending at-bat):**
```
line=350: inn=9 top=False out=2 s=3 → OUT=2, STRIKE=3 in bottom 9th!
line=351: inn=9 top=False out=2 s=0 → reverts without out change
```

This is the exact scenario described in the design doc: `out=2, strike=3` in
the bottom of the 9th inning of a game ending. If the old pre-fire logic had
been used:
- `evaluate_game_end_from_outs_into`: would check `outs == 3` → `Some(2) != Some(3)` → does not fire. **Safe.**
- `evaluate_nrfi_from_outs_into`: irrelevant (inning 9).

The old `out=2, strike=3` pre-fire was removed precisely because of this
pattern. The code's `outs == 3` check is correct.

**Out-decrease anomalies:** 3 instances found where outs decreased without
a break frame:
- STL vs PIT line 38: `out 3→0` at `inn=2, period=AT_BOT_1ST_INNING` — transition frame with stale period (out=3 was delivered, then outs reset to 0 but period hasn't updated yet). Harmless: `out=0` does not trigger any evaluator.
- STL vs PIT line 147: `out 2→0` at `inn=4, period=AT_TOP_4TH_INNING` — same pattern.
- WAS vs NYM line 148: `out 3→2` at `inn=4, period=AT_BOT_4TH_INNING` — brief revert. Harmless: `out=2` does not trigger any evaluator.

None of these anomalies would cause incorrect evaluation. The code's strict
`outs == 3` check prevents mis-fires from all observed data quality issues.

**Result: CORRECT.** The decision to remove `strike=3` pre-fire is validated
by empirical data showing 38 wobble instances across 5 games.

---

### 3.5 V1 tick clears BoltOdds fields

**Trace through `process_tick_live` (`engine.rs:529-543`):**

```rust
let mut state = GameState {
    // ... score, inning, etc from V1 ...
    outs: None,
    strikes: None,
    base1: None,
    base2: None,
    base3: None,
};
```

All BoltOdds-only fields are explicitly set to `None` on every V1 tick.
This prevents the stale-state scenario described in the design doc (section
"Why clearing BoltOdds fields on V1 ticks matters"):

1. BoltOdds: `inning=9, top=true, outs=3` → could trigger game-end.
2. V1: `inning=9, half=bottom` → without clearing, stale `outs=3` +
   new `half=bottom` could erroneously trigger `evaluate_game_end_from_outs_into`.
3. With clearing: `outs=None` → `state.outs != Some(3)` → evaluator
   does not fire. **Correct**.

**Result: CORRECT.** V1 ticks properly clear BoltOdds-specific state fields.

---

### 3.6 BoltOdds tick preserves V1 `match_completed`

**Trace through `process_boltodds_tick_live` (`engine.rs:616-631`):**

```rust
let state = GameState {
    // ...
    match_completed: prev.match_completed,  // preserve V1's value
    game_state: prev.game_state,            // preserve V1's value
    outs: Some(outs),
    // ...
};
```

BoltOdds ticks preserve `match_completed` and `game_state` from the previous
state (which may have been set by V1). BoltOdds does not independently
determine game completion. **Correct**.

---

## 4. Detailed Evaluator Verification

### 4.1 Walkoff evaluator (`evaluate_walkoff_into`) — eval.rs:155-251

**Guard conditions (lines 162-176):**
```rust
if self.final_resolved_games[gi] { return; }
if !self.has_final[gi] { return; }
let inning = state.inning_number.unwrap_or(0);
if inning < 9 || state.inning_half != "bottom" { return; }
let home = state.home.unwrap_or(0);
let away = state.away.unwrap_or(0);
if home <= away { return; }
```

- `home <= away` uses `<=`, meaning walkoff does NOT fire when tied. **Correct** — a walkoff requires home to be strictly ahead.
- `inning < 9` uses `<`, so `inning >= 9` passes. Extra innings (10, 11, ...) fire. **Correct**.
- `inning_half != "bottom"` blocks top-of-inning. **Correct**.
- `unwrap_or(0)` on home/away means missing scores default to 0 (tied) → `home <= away` → no fire. **Correct** fail-safe.

**Tier 1 — Partial resolution (lines 178-205):**

Moneyline: `push_if_some(self.game_targets[gi].moneyline_home, out)` — fires home. **Correct**.

Spread logic:
```rust
for slot in &targets.spreads {
    if slot.side == SpreadSide::Home {
        if (margin_home as f64) + slot.line > 0.0 {
            push_if_some(slot.covers_idx, out);
        }
        // Home not covering: not fired in Tier 1.
    } else {
        let away_margin = -margin_home;
        if (away_margin as f64) + slot.line <= 0.0 {
            push_if_some(slot.not_covers_idx, out);
        }
        // Away covering: not fired in Tier 1.
    }
}
```

Concrete scenarios:

| Scenario | margin_home | Slot | Calc | Result | Correct? |
|----------|------------|------|------|--------|----------|
| Home -1.5, margin=1 | 1 | Home | `1+(-1.5)=-0.5 ≤ 0` | No fire | ✓ (margin could grow) |
| Home -1.5, margin=2 | 2 | Home | `2+(-1.5)=0.5 > 0` | `covers` fires | ✓ (locked) |
| Away -1.5, margin=1 | 1 | Away | `(-1)+(-1.5)=-2.5 ≤ 0` | `not_covers` fires | ✓ (away lost, gets worse) |
| Away +1.5, margin=1 | 1 | Away | `(-1)+1.5=0.5 > 0` | No fire (covers true but unsafe) | ✓ (margin could worsen) |
| Away +0.5, margin=2 | 2 | Away | `(-2)+0.5=-1.5 ≤ 0` | `not_covers` fires | ✓ (not covering, gets worse) |
| Home +0.5, margin=1 | 1 | Home | `1+0.5=1.5 > 0` | `covers` fires | ✓ (locked) |
| Home -2.5, margin=1 | 1 | Home | `1+(-2.5)=-1.5 ≤ 0` | No fire | ✓ (could grow to cover) |

The Tier 1 logic correctly fires only the mathematically locked sides:
- **Home covers** when margin already satisfies the line (margin only grows).
- **Away not_covers** when away margin is insufficient (gets worse).
- **Home not_covers** and **away covers** are NEVER fired in Tier 1. **Correct**.

**Tier 2 — Full resolution (lines 210-250):**

```rust
let bases_empty = state.base1 == Some(false)
    && state.base2 == Some(false)
    && state.base3 == Some(false);
```

- `Some(false)` required for each base. `None` (V1 path) → not empty. **Correct**.
- `Some(true)` (runner on base) → not empty. **Correct**.

When empty:
- Re-iterates ALL spreads (both covers and not_covers). **Correct** — presign pool prevents double-fire on targets already popped by Tier 1.
- Fires under lines: `half_int >= total`. Sets `totals_final_under_emitted[gi] = true`. **Correct**.
- Sets `final_resolved_games[gi] = true`. **Correct**.

When NOT empty (or `None`):
- Only Tier 1 fires. `final_resolved_games` NOT set. **Correct** — game-end evaluators fire remaining targets later.

**Presign pool safety for Tier 2 re-iteration:** Tier 2 calls `push_if_some`
on spread sides that Tier 1 already pushed. Both intents enter the `SmallVec`.
At dispatch time, `DispatchHandle::pop_for_target(target_idx)` uses
`std::mem::take()` on the presign pool slot. The first pop returns the
signed order. The second pop for the same `TargetIdx` maps to the same
`TokenIdx`, finds an empty `SmallVec` (already taken), and returns
`Err("presign_pool_miss")`. This is a logged error, not a panic. **Correct** — harmless no-op.

**Result: VERIFIED CORRECT.** All branches handle the correct cases.

---

### 4.2 Spread evaluation math — all evaluators

The spread margin formula is used identically in three evaluators:

```rust
let margin_home = home - away;
for slot in &targets.spreads {
    let margin = if slot.side == SpreadSide::Home {
        margin_home
    } else {
        -margin_home
    };
    if (margin as f64) + slot.line > 0.0 {
        push_if_some(slot.covers_idx, out);
    } else {
        push_if_some(slot.not_covers_idx, out);
    }
}
```

Locations: `eval.rs:218-229` (walkoff Tier 2), `eval.rs:289-300`
(evaluate_final_into), `eval.rs:445-457` (evaluate_game_end_from_outs_into).

**Ground-truth against DET 1, CLE 3:**

| Spread | Side | margin_home | margin | Calc | Fires | Correct? |
|--------|------|-------------|--------|------|-------|----------|
| Away -1.5 | Away | -2 | 2 | `2+(-1.5)=0.5 > 0` | covers | ✓ (won by 2) |
| Home +1.5 | Home | -2 | -2 | `-2+1.5=-0.5 ≤ 0` | not_covers | ✓ (lost by 2) |
| Away -0.5 | Away | -2 | 2 | `2+(-0.5)=1.5 > 0` | covers | ✓ |
| Home +0.5 | Home | -2 | -2 | `-2+0.5=-1.5 ≤ 0` | not_covers | ✓ |
| Home -1.5 | Home | -2 | -2 | `-2+(-1.5)=-3.5 ≤ 0` | not_covers | ✓ |
| Away +1.5 | Away | -2 | 2 | `2+1.5=3.5 > 0` | covers | ✓ |

The sign flip (`-margin_home` for Away side) is consistent across all three
evaluators. The `> 0.0` threshold (not `>= 0.0`) means a push (exact tie)
resolves to `not_covers`, which matches Polymarket's convention for half-point
lines (the 0.5 ensures no pushes in practice, but the boundary is consistent).

**Result: VERIFIED CORRECT.** Formula is identical and correct in all three evaluators.

---

### 4.3 Game-end evaluator (`evaluate_game_end_from_outs_into`) — eval.rs:391-473

**Guard conditions (lines 397-413):**
```rust
if !self.has_final[gi] { return; }
if self.final_resolved_games[gi] { return; }
let inning = state.inning_number.unwrap_or(0);
if inning < 9 { return; }
if state.outs != Some(3) { return; }
```

- Only fires on `outs == 3`. No `strike=3` pre-fire. **Correct**.
- `inning >= 9` allows extras. **Correct**.

**Winner determination (lines 425-432):**
```rust
if state.inning_half == "top" && home > away {
    home_wins = true;
} else if state.inning_half == "bottom" && away > home {
    home_wins = false;
} else {
    return; // tied or game continues
}
```

Case analysis:
| Half | Leader | Action | Correct? |
|------|--------|--------|----------|
| top | home > away | home_wins=true | ✓ Away batted, can't catch up, bottom skipped |
| top | away > home | return | ✓ Game continues (home bats bottom) |
| top | tied | return | ✓ Game continues (extras) |
| bottom | away > home | home_wins=false | ✓ Home batted and failed |
| bottom | home > away | return | ✓ This is a WALKOFF (handled by walkoff evaluator) |
| bottom | tied | return | ✓ Game continues (extras) |

**Critical case — bottom 9th, home leading, outs=3:** This is the scenario
where the home team scored a walkoff hit, the inning continued (e.g., runner
on base), and then outs accumulated to 3. The walkoff evaluator should have
already fired on the score change. This evaluator correctly returns without
firing because `home > away` in bottom doesn't match either branch.

BUT: what if the walkoff Tier 1 already fired moneyline, and then the
remaining at-bat produces 3 outs? `evaluate_walkoff_into` does NOT set
`final_resolved_games` when bases are not empty (Tier 1 only). So
`final_resolved_games[gi]` is still false. The `evaluate_game_end_from_outs_into`
enters and checks: `state.inning_half == "bottom" && away > home` → false
(home is leading). Returns without firing. **Correct** — the remaining
spread sides and unders will be resolved by V1's `evaluate_final_into` when
the "Ended" frame arrives.

**Under lines (lines 460-470):**
```rust
if self.has_totals[gi] && !self.totals_final_under_emitted[gi] {
    let total = state.total.unwrap_or(0) as u16;
    for ol in &targets.under_lines {
        if ol.half_int >= total {
            out.push(Intent { target_idx: ol.target_idx });
        }
    }
    self.totals_final_under_emitted[gi] = true;
}
```

Checks `totals_final_under_emitted` before firing. Sets it after.
Prevents double-fire from `evaluate_totals_into` (which fires unders on
`match_completed`). **Correct**.

**Result: VERIFIED CORRECT.**

---

### 4.4 NRFI evaluator (`evaluate_nrfi_from_outs_into`) — eval.rs:315-384

**First-inning observation gate (lines 331-344):**
```rust
if !self.nrfi_first_inning_observed[gi] {
    match state.inning_number {
        Some(1) => { self.nrfi_first_inning_observed[gi] = true; }
        Some(_) => { self.nrfi_resolved_games[gi] = true; return; }
        None => { return; }
    }
}
```

Identical logic to V1's `evaluate_nrfi_into` (lines 101-114). Late
subscriptions permanently blocked. No inning data defers. **Correct**.

**Early YES path (lines 349-356):**
```rust
if is_first_inning(state) {
    if let (Some(total), Some(prev)) = (state.total, state.prev_total) {
        if total > prev {
            self.nrfi_resolved_games[gi] = true;
            push_if_some(self.game_targets[gi].nrfi_yes, out);
            return;
        }
    }
}
```

- `prev_total` is `None` on cold start → `let` pattern fails → skip. **Correct** cold-start protection.
- `total > prev` checks delta correctly. **Correct**.
- Sets `nrfi_resolved_games` before returning. **Correct**.

**Outs-based NO path (lines 361-383):**
```rust
if state.inning_number != Some(1) || state.inning_half != "bottom" { return; }
if state.outs != Some(3) { return; }

let total = state.total.unwrap_or(0);
if total > 0 {
    push_if_some(targets.nrfi_yes, out);
} else {
    push_if_some(targets.nrfi_no, out);
}
self.nrfi_resolved_games[gi] = true;
```

- Only fires in bottom of 1st. **Correct** (top of 1st outs=3 means only half the 1st inning is done).
- Only fires on `outs == 3`. **Correct**.
- `total > 0` → YES (runs scored). `total == 0` → NO (clean inning). **Correct**.
- `nrfi_resolved_games` set in both branches. **Correct**.

**Result: VERIFIED CORRECT.**

---

### 4.5 Cross-evaluator interactions

**Scenario A: Walkoff Tier 1 (partial) → V1 "Ended"**

1. BoltOdds reports walkoff (bottom 9th, home leads, bases occupied).
2. `evaluate_walkoff_into` fires moneyline + partial spreads (Tier 1).
   `final_resolved_games` NOT set. `totals_final_under_emitted` NOT set.
3. V1 "Ended" frame arrives. `evaluate_totals_into` fires unders (sets
   `totals_final_under_emitted`). `evaluate_final_into` checks
   `final_resolved_games` → false → fires moneyline (presign pool
   rejects duplicate) + ALL spreads (presign pool rejects already-popped
   Tier 1 targets, accepts remaining). Sets `final_resolved_games`.

**Verified correct.** Remaining spreads and unders fire exactly once.

**Scenario B: Walkoff Tier 2 (empty bases) → V1 "Ended"**

1. BoltOdds reports walkoff with empty bases.
2. `evaluate_walkoff_into` fires moneyline + all spreads + unders.
   Sets `final_resolved_games = true`. Sets `totals_final_under_emitted = true`.
3. V1 "Ended" frame arrives. `evaluate_totals_into` checks
   `totals_final_under_emitted` → true → skips unders. `evaluate_final_into`
   checks `final_resolved_games` → true → early return.

**Verified correct.** No double-fire.

**Scenario C: BoltOdds game-end → V1 "Ended"**

1. BoltOdds `out=3` in bottom 9th, away leading. `evaluate_game_end_from_outs_into`
   fires moneyline + all spreads + unders. Sets `final_resolved_games = true`.
   Sets `totals_final_under_emitted = true`.
2. V1 "Ended" arrives. Both `evaluate_totals_into` (unders) and
   `evaluate_final_into` (moneyline/spreads) find flags set and skip.

**Verified correct.** No double-fire.

**Scenario D: Under line triple-fire prevention**

Three places can fire under lines:
1. `evaluate_totals_into` (lines 74-84): fires on `match_completed`, checks
   `!self.totals_final_under_emitted[gi]`, sets flag.
2. `evaluate_walkoff_into` Tier 2 (lines 232-239): fires when bases empty,
   checks `!self.totals_final_under_emitted[gi]`, sets flag.
3. `evaluate_game_end_from_outs_into` (lines 460-470): fires on game end,
   checks `!self.totals_final_under_emitted[gi]`, sets flag.

All three check the same flag before firing and set it after. **Verified
correct** — at most one path fires under lines per game.

**Scenario E: Walkoff Tier 1 → BoltOdds game-end outs=3**

This is the scenario where a walkoff hit is followed by at-bat completion
(e.g., runner caught stealing, double play, subsequent outs).

1. Walkoff fires Tier 1: moneyline + partial spreads. `final_resolved_games`
   NOT set.
2. Later, `outs=3` in bottom 9th with home > away.
3. `evaluate_game_end_from_outs_into`: checks
   `state.inning_half == "bottom" && away > home` → false (home leads).
   Returns without firing.
4. Remaining spreads/unders resolved later by V1 `evaluate_final_into`.

**Verified correct.** The game-end evaluator's condition explicitly excludes
walkoff scenarios (bottom + home leading).

---

### 4.6 `evaluate_final_into` does not fire under lines

`evaluate_final_into` (lines 253-303) fires moneyline and spreads but does
NOT fire under lines. Under lines are fired by `evaluate_totals_into`
(lines 74-84) which runs BEFORE `evaluate_final_into` in the V1 path:

```
evaluate_totals_into   ← fires unders on match_completed
evaluate_nrfi_into
evaluate_walkoff_into
evaluate_final_into    ← fires moneyline + spreads only
```

This is by design: `evaluate_totals_into` handles both over crossings
(progressive) and final-time unders. The separation is correct because
the totals evaluator owns the `totals_final_under_emitted` flag.

**Result: VERIFIED CORRECT.**

---

### 4.7 GameState field clearing

**V1 tick → BoltOdds fields cleared (engine.rs:538-543):**
```rust
outs: None,
strikes: None,
base1: None,
base2: None,
base3: None,
```

**BoltOdds tick → V1 fields preserved (engine.rs:623-624):**
```rust
match_completed: prev.match_completed,
game_state: prev.game_state,
```

Both directions are correct. V1 clears BoltOdds state to prevent stale
data. BoltOdds preserves V1 completion state because only V1 (via freeText)
determines game completion.

**Result: VERIFIED CORRECT.**

---

## 5. Untested Scenarios

### 5.1 No walkoff test coverage in captures

All 5 complete games in the capture data were away-team wins (bottom 9th,
away leading). No walkoff occurred. This means:

- **Walkoff Tier 1 with real frame data:** NOT tested against captures.
  The evaluator logic is tested in unit tests (`walkoff_fires_moneyline_home_bottom_9th_home_leads`, etc.) but not traced through actual BoltOdds frames.
- **Walkoff Tier 2 (empty bases) with real frame data:** NOT tested against captures.
  Unit tests cover this (`walkoff_empty_bases_fires_all_spreads`, etc.).
- **Walkoff with occupied bases then V1 "Ended"** with real data: NOT tested.
  Unit test `walkoff_partial_then_game_end_fires_remaining` covers this.

**Recommendation:** Capture games that include home-team walkoff wins to
validate Tier 1 and Tier 2 with real BoltOdds frame sequences.

### 5.2 No top-of-9th home-wins test in captures

All 5 complete games ended in bottom 9th with away winning. No game ended
with 3 outs in the top of the 9th with home ahead (causing bottom to be
skipped). Unit test `game_end_from_outs_top9_home_ahead_fires` covers this,
but no real frame trace-through exists.

**Recommendation:** Capture games where home wins without batting in the 9th.

### 5.3 No extra-innings test in captures

No games went to extra innings. Unit tests cover extras
(`walkoff_fires_in_extra_innings`, `game_end_from_outs_extras_bottom_away_ahead`)
but no real data validation.

### 5.4 No NRFI YES via run delta from BoltOdds in captures

The 3 games with clean first innings (ARI vs COL, DET vs CLE, WAS vs NYM)
all had 0-0 first innings, testing NRFI NO. The games with first-inning runs
(STL vs PIT, MIA vs ATL, NYY vs TOR) would test NRFI YES, but the early-YES
path fires on V1 score delta OR BoltOdds score delta — whichever is first.
The capture data doesn't show which provider was faster for first-inning runs
in these games.

Unit test `nrfi_from_outs_early_yes_on_run_delta` and
`boltodds_tick_nrfi_early_yes_integration` cover this path.

### 5.5 Missing test: `evaluate_walkoff_into` Tier 1 away_not_covers with positive line

The test `walkoff_fires_away_not_covers_positive_line` covers Away +0.5
with margin=2 (not covering). But there's no test for the boundary case:
Away +0.5 with margin=1 (away_margin=-1, `-1+0.5=-0.5 ≤ 0` → fires
not_covers). This is covered by the existing test, but the case where the
line is exactly equal to the margin (Away +1.0, margin=1: `-1+1.0=0.0 ≤ 0`
→ fires not_covers) is NOT explicitly tested.

### 5.6 Missing test: break frame with `MATCH_COMPLETED` period

The `MATCH_COMPLETED` period does NOT start with `AT_MID_` or `AT_END_`,
so it passes the break frame guard and evaluators run. This is correct (the
game IS completed), but there's no unit test verifying that `MATCH_COMPLETED`
frames are NOT treated as break frames. The extractor test
`test_match_completed` in `boltodds_baseball_types.rs` verifies extraction
but not the evaluator dispatch path.

---

## 6. Summary

| Section | Verdict |
|---------|---------|
| Walkoff evaluator (Tier 1 partial spreads) | **Verified correct** — fires only mathematically locked sides |
| Walkoff evaluator (Tier 2 empty bases) | **Verified correct** — full resolution with proper flag setting |
| Walkoff guard (home <= away) | **Verified correct** — strict comparison excludes ties |
| Walkoff in extras (inning >= 9) | **Verified correct** — < 9 check allows 10, 11, ... |
| Spread math (all 3 evaluators) | **Verified correct** — formula and sign flip consistent |
| Game-end evaluator (outs == 3 only) | **Verified correct** — no strike=3 pre-fire |
| Game-end winner determination | **Verified correct** — all 6 half/leader cases handled |
| Game-end excludes walkoff scenario | **Verified correct** — bottom+home leading returns |
| NRFI early YES (run delta) | **Verified correct** — cold-start protection, prev_total guard |
| NRFI outs-based NO (bottom 1st, outs=3) | **Verified correct** — only fires in bottom, only on outs=3 |
| First-inning observation gate | **Verified correct** — shared between V1 and BoltOdds |
| Break frame guard | **Verified correct** — 97/97 break frames have outs=0, strikes=0 |
| V1 field clearing | **Verified correct** — prevents stale BoltOdds state |
| BoltOdds V1 field preservation | **Verified correct** — match_completed preserved |
| Cross-evaluator flag interactions | **Verified correct** — no double-fire possible |
| Under line triple-fire prevention | **Verified correct** — single flag gates all 3 paths |
| Design doc accuracy | **Potential issue** — line 262 claims strike=3 pre-fire but code correctly omits it |
| Real-data game-end trace (DET vs CLE) | **Correct** — away_wins, moneyline_away, 15.4s advantage |
| Real-data NRFI trace (DET vs CLE) | **Correct** — NRFI NO fires, 5.5s advantage |
| Strike wobble safety | **Correct** — 38 wobbles observed, none would cause mis-fire |
