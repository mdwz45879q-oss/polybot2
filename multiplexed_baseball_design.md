# Multiplexed Baseball Pipeline Design

## Motivation

The baseball hotpath runs Kalstrop V1 and BoltOdds concurrently in a multiplexed
configuration. BoltOdds reports pitch-by-pitch data — **outs**, **strikes**, and
**base runner state** (`base1`, `base2`, `base3`) — that V1 does not provide. This
enables three categories of advantage over a V1-only setup:

1. **Earlier half-inning-end detection** via `out=3` — fires NRFI, game-end
   moneyline/spreads/unders seconds before V1's freeText transition.
2. **Walkoff spread/under resolution** — when a walkoff occurs, partial spreads
   can be resolved immediately (Tier 1), and when bases are empty the full game
   can be resolved without waiting for the game-end signal (Tier 2).
3. **NRFI YES on run delta** — fires immediately when total increases in the
   first inning, matching V1 timing (whichever provider reports the run first wins).

V1 remains faster for **run scoring** (total overs) and is the only source for
**freeText-based inning parsing** (used by the existing V1 evaluators). The
multiplexed pipeline preserves V1 as the primary evaluation path and adds BoltOdds
as a supplementary signal.

---

## Empirical Findings

All measurements come from dual-source captures of 7 MLB games on 2026-05-21, stored
in `captures/2026_05_21/`. The analysis script is `scripts/analyze_out3_advantage.py`.

### 1. BoltOdds `out=3` arrives before V1 inning transitions

When a half-inning ends (3 outs recorded), BoltOdds reports `out=3` in the current
half-inning's frame **before** V1 updates its `freeText` to reflect the transition.

**NRFI (3 games where 1st inning ended 0-0):**

| Game | BO `out=3` lead over V1 `"Break top 2 bottom 1"` |
|------|:---:|
| ari_diamondbacks_vs_col_rockies | +7,252 ms |
| det_tigers_vs_cle_guardians | +5,484 ms |
| was_nationals_vs_ny_mets | +2,761 ms |
| **Mean** | **+5,166 ms** |

**Game end — away team wins, bottom 9th (4 complete games):**

| Game | BO `out=3` lead over V1 `"Ended"` |
|------|:---:|
| det_tigers_vs_cle_guardians (1-3) | +15,378 ms |
| stl_cardinals_vs_pit_pirates (2-6) | +16,284 ms |
| was_nationals_vs_ny_mets (1-2) | +4,145 ms |
| mia_marlins_vs_atl_braves (3-9) | +599 ms |
| **Mean** | **+9,101 ms** |

No "home wins via top-9th outs" cases occurred in this dataset (all 5 complete games
were away-team wins).

BoltOdds was faster in **100% of measurements**. The advantage is 0.6-16.3 seconds,
with an overall mean of +7.4 seconds.

### 2. `strike=3` is NOT used as a trigger (unreliable signal)

When the 3rd out is a strikeout, BoltOdds sometimes delivers `strike=3` before
`out=3`. Initial analysis suggested this could provide an additional edge. However,
deeper analysis of all `out=2, strike=3` frames across 7 games revealed a **68% false
positive rate**: strikes frequently flash to 3 then revert to 0 (dropped 3rd strikes,
scorer corrections, data wobbles). 3 of these false positives occurred in game-ending
9th-inning situations where premature evaluation would have fired irrecoverable
moneyline/spread intents on incorrect game-end signals.

The `strike=3` signal violates the fail-closed principle: the presign pool's one-shot
gate (`Option::take()`) cannot un-fire a premature evaluation. Given the 68% false
positive rate, **only `outs == 3` is used as the half-inning-end trigger**.

### 3. BoltOdds `out=3` delivery reliability

BoltOdds does not always deliver a frame with `out=3` before resetting to `out=0` for
the next half-inning. Out of 106 half-inning transitions observed:

| Half type | `out=3` delivered | Rate |
|-----------|:-:|:-:|
| Top of inning | 45 / 52 | 87% |
| Bottom of inning | 46 / 48 | 96% |
| Game-ending bottom | 5 / 6 | 83% |
| **Overall** | **96 / 106** | **91%** |

For the **NRFI-relevant case** (bottom of 1st), `out=3` was delivered in all 7 games
(100% in this sample).

When `out=3` is not delivered, the optimization simply does not fire and the system
falls back to V1's inning-transition signal — no harm, just no early edge for that
particular half-inning.

### 4. Inning wobble

BoltOdds occasionally produces brief wobbles in inning state (e.g., `5 -> 6 -> 5 -> 6`)
before settling. This is not a correctness concern because the presign pool's one-shot
gate (`Option::take()`) prevents any target from firing twice. If a wobble causes an
evaluator to emit the same intent on two ticks, the second pop from the presign pool
returns `None` and the duplicate is dropped at dispatch time.

---

## Evaluation Logic

### V1 evaluators (run on V1 ticks only)

Dispatched by `process_tick_live` in `baseball/engine.rs`:

| Evaluator | Trigger | Markets |
|-----------|---------|---------|
| `evaluate_totals_into` | Score change from `prev_total` to `total` (progressive over crossing). Under lines fire on `match_completed`. | Total overs, total unders |
| `evaluate_nrfi_into` | Run delta in 1st inning fires YES immediately. `freeText = "Break top 2 bottom 1"` with total=0 fires NO. | NRFI |
| `evaluate_walkoff_into` | Bottom 9th+, home > away. Fires moneyline + partial spreads (Tier 1). Full resolution when base data available (Tier 2). See walkoff section. | Moneyline, spreads, unders |
| `evaluate_final_into` | `match_completed = true` (freeText = `"Ended"`). Fires moneyline, all spreads, under lines. | Moneyline, spreads, unders |

V1 `process_tick_live` sets `outs: None, strikes: None, base1: None, base2: None,
base3: None` in GameState. V1 ticks cannot trigger outs-based or base-runner-based
evaluators.

### BoltOdds evaluators (run on BoltOdds ticks only)

Dispatched by `process_boltodds_tick_live` in `baseball/engine.rs`:

| Evaluator | Trigger | Markets |
|-----------|---------|---------|
| `evaluate_totals_into` | Same shared evaluator as V1. BoltOdds score changes fire total overs too. | Total overs |
| `evaluate_nrfi_from_outs_into` | Two paths: (1) Early YES on run delta in 1st inning. (2) Bottom of 1st, `outs==3`, total==0 fires NO. | NRFI |
| `evaluate_walkoff_into` | Same shared evaluator as V1. BoltOdds provides base runner state for Tier 2 resolution. | Moneyline, spreads, unders |
| `evaluate_game_end_from_outs_into` | Inning 9+, `outs==3`, non-walkoff game end. | Moneyline, spreads, unders |

**Break frame guard:** Before running any evaluator, `process_boltodds_tick_live`
checks `period_detail` for break periods (`AT_MID_*` = mid-inning break between top
and bottom, `AT_END_*` = end-of-inning break between bottom and next top). Break
frames skip all evaluators entirely. State is still updated (so dedup and `prev_total`
track correctly) and the tick is still returned (so it gets logged with the break
period), but no intents are emitted. This is a defensive guard — analysis of 97 break
frames across 7 games confirmed they always have `outs=0, strikes=0`, so outs-based
evaluators would not mis-fire, but `evaluate_walkoff_into` and `evaluate_totals_into`
do not require outs signals and could theoretically fire if BoltOdds data quality
regressed. The guard makes evaluation explicitly safe regardless of future data changes.

Both paths emit `Intent { target_idx }` into the same `SmallVec`. The presign pool
one-shot gate handles the race: whichever provider fires an intent first consumes the
presign order. The slower provider's identical intent finds an empty pool slot and is
silently dropped.

### Evaluator dispatch order

Both V1 and BoltOdds paths run evaluators in a fixed order. The order matters because
`evaluate_walkoff_into` must run before `evaluate_final_into` / `evaluate_game_end_from_outs_into`
so that walkoff resolution flags block redundant game-end evaluation.

**V1 path** (`process_tick_live`):
1. `evaluate_totals_into`
2. `evaluate_nrfi_into`
3. `evaluate_walkoff_into`
4. `evaluate_final_into`

**BoltOdds path** (`process_boltodds_tick_live`):
1. `evaluate_totals_into`
2. `evaluate_nrfi_from_outs_into`
3. `evaluate_walkoff_into`
4. `evaluate_game_end_from_outs_into`

### Resolution flag interactions

The following per-game flags gate evaluation across both providers:

| Flag | Set by | Checked by |
|------|--------|------------|
| `final_resolved_games[gi]` | `evaluate_walkoff_into` (Tier 2 only), `evaluate_final_into`, `evaluate_game_end_from_outs_into` | All final/walkoff/game-end evaluators (early return if set) |
| `totals_final_under_emitted[gi]` | `evaluate_totals_into` (on `match_completed`), `evaluate_walkoff_into` (Tier 2), `evaluate_game_end_from_outs_into` | All under-line firing paths (prevents double under emission) |
| `nrfi_resolved_games[gi]` | `evaluate_nrfi_into`, `evaluate_nrfi_from_outs_into` | Both NRFI evaluators (early return if set) |
| `nrfi_first_inning_observed[gi]` | Both NRFI evaluators (on first inning=1 observation) | Both NRFI evaluators (late subs with inning>1 permanently skipped) |

---

## Walkoff Resolution

Walkoff: bottom of 9th or later, home scores the go-ahead run. The game ends
immediately. `evaluate_walkoff_into` in `baseball/eval.rs` implements three-tier
resolution:

### Tier 1: Partial resolution (V1 and BoltOdds)

Always fires when walkoff conditions are met (`inning >= 9, inning_half == "bottom",
home > away`). No base runner data needed.

**Intents fired:**
- **Moneyline home:** Home wins.
- **Home covers** (when `margin_home + line > 0`): Home margin can only stay same
  or grow (runners on base can score additional runs). If margin already satisfies
  the line, it will still satisfy it after any further scoring. Safe to fire.
- **Away not_covers** (when `away_margin + line <= 0`): Away margin (= -home_margin)
  only gets more negative as home scores. If away does not cover now, it will never
  cover. Safe to fire.

**NOT fired in Tier 1:**
- **Home not_covers:** Margin could grow and flip to covering. Not safe.
- **Away covers:** Away margin gets worse, could stop covering. Not safe.

### Tier 2: Full resolution with empty bases (BoltOdds only)

When `base1 == Some(false) && base2 == Some(false) && base3 == Some(false)`, the
walkoff hit scored exactly +1 run and no additional runners can score. The current
score IS the final score.

**Additional intents fired:**
- All remaining spread sides (covers and not_covers) evaluated at the final margin.
  Tier 2 re-iterates all spreads including those already fired by Tier 1; the presign
  pool's `Option::take()` one-shot gate prevents double-fire.
- All under lines where `half_int >= total`. Sets `totals_final_under_emitted[gi] = true`.
- Sets `final_resolved_games[gi] = true`, blocking `evaluate_final_into` and
  `evaluate_game_end_from_outs_into` from running on subsequent ticks.

**Guard:** `bases_empty` requires all three to be `Some(false)`. When any base field
is `None` (V1 path) or `Some(true)` (runners on base), only Tier 1 fires. The
remaining spreads/unders are resolved later by `evaluate_final_into` (V1 "Ended")
or `evaluate_game_end_from_outs_into` (BoltOdds outs=3).

### Walkoff spread logic derivation

On a walkoff, `margin_home >= 1` and can only increase (never decrease):

| Spread side | Condition to fire | Reasoning |
|---|---|---|
| Home covers | `margin_home + home_line > 0` | Margin grows. Covers now implies covers at final score. |
| Home not_covers | NEVER in Tier 1 | Margin could grow to satisfy line. |
| Away not_covers | `(-margin_home) + away_line <= 0` | Away margin worsens. Not covering now implies not covering at final. |
| Away covers | NEVER in Tier 1 | Away margin worsens. Could stop covering. |

Example: Score 3-2 (margin=1), Home -0.5 spread.
`1 + (-0.5) = 0.5 > 0` — home covers. Even if runners score (margin grows to 2, 3, ...),
`margin + (-0.5) > 0` still holds. Safe.

Example: Score 3-2 (margin=1), Away -1.5 spread.
Away margin = -1. `(-1) + (-1.5) = -2.5 <= 0` — away does not cover. If more runners
score (away margin = -2, -3, ...), it only gets worse. Safe.

Counter-example: Score 3-2 (margin=1), Home -2.5 spread.
`1 + (-2.5) = -1.5 <= 0` — home does NOT cover now. But if a runner on base scores
(margin=2), `2 + (-2.5) = -0.5 <= 0` still not covering. If two runners score
(margin=3), `3 + (-2.5) = 0.5 > 0` now covers. Cannot fire either side in Tier 1.

---

## Detailed Evaluator Conditions

### `evaluate_nrfi_from_outs_into` (BoltOdds NRFI)

Two resolution paths within one evaluator:

**Path 1 — Early YES on run delta:**
- `inning == 1` (any half)
- `total > prev_total` (run scored)
- Fires `nrfi_yes`, sets `nrfi_resolved_games[gi] = true`
- Same timing as V1's `evaluate_nrfi_into` run-delta path. Whichever provider
  reports the run first wins (presign pool gate).

**Path 2 — Outs-based NO:**
- `inning == 1`, `inning_half == "bottom"` (bottom of 1st)
- Outs signal: `outs == 3` (confirmed half-inning end)
- If `total == 0`: fires `nrfi_no` (clean first inning, no runs scored)
- If `total > 0`: fires `nrfi_yes` (runs scored, outs-based confirmation)
- Sets `nrfi_resolved_games[gi] = true`

The first-inning observation gate is shared with V1: `nrfi_first_inning_observed[gi]`
must be set (inning==1 seen) before evaluation proceeds. Late subscriptions (first
tick has inning > 1) permanently skip NRFI.

### `evaluate_game_end_from_outs_into` (BoltOdds game end)

Fires moneyline, all spreads, and all under lines when the final out is detected.
Only fires for **non-walkoff** game endings:

**Home team wins (top of 9th+ ends, home ahead):**
- `inning >= 9`
- `inning_half == "top"` (away batting — their at-bat just ended)
- `home > away`
- Outs signal: `outs == 3`

**Away team wins (bottom of 9th+ ends, away ahead):**
- `inning >= 9`
- `inning_half == "bottom"` (home batting — their at-bat just ended)
- `away > home`
- Outs signal: `outs == 3`

**Excluded cases:** Bottom of inning + home leads = walkoff (handled by
`evaluate_walkoff_into`). Top of inning + away leads = game continues (bottom
still to play). Tied = game continues (extra innings).

---

## Architecture

### Game State: Unified with Optional BoltOdds-Only Fields

A single `GameState` struct (`baseball/types.rs`) is shared across both providers.
All fields that describe the same physical reality (score, inning, half) are stored
once, not duplicated per provider. The BoltOdds-only fields are `Option` types:

```
GameState {
    // --- Shared fields (set by whichever provider's frame arrives) ---
    home: Option<i64>,
    away: Option<i64>,
    total: Option<i64>,
    prev_total: Option<i64>,
    inning_number: Option<i64>,
    inning_half: &'static str,
    match_completed: Option<bool>,
    game_state: &'static str,

    // --- BoltOdds-only fields (None when last tick was V1) ---
    outs: Option<u8>,       // 0-3
    strikes: Option<u8>,    // 0-3
    base1: Option<bool>,    // runner on 1st
    base2: Option<bool>,    // runner on 2nd
    base3: Option<bool>,    // runner on 3rd
}
```

On a **V1 frame**: update score, inning, half, match_completed, game_state from V1
data. Set `outs = None`, `strikes = None`, `base1 = None`, `base2 = None`,
`base3 = None`. This prevents stale BoltOdds state from leaking into V1
evaluations.

On a **BoltOdds frame**: update score, inning, half from BoltOdds data. Set
`outs = Some(n)`, `strikes = Some(n)`, `base1 = Some(b)`, `base2 = Some(b)`,
`base3 = Some(b)`. Preserve V1's `match_completed` and `game_state` (BoltOdds
doesn't determine completion — V1 does via freeText).

**Why clearing BoltOdds fields on V1 ticks matters:**
1. BoltOdds frame arrives: `inning=9, top=true, outs=3, strikes=0`. Would trigger
   game-end-from-outs if home is ahead.
2. V1 frame arrives: `inning=9, half=bottom`. Without clearing outs, the stale
   `outs=3` + new `half=bottom` could erroneously trigger the away-wins branch of
   `evaluate_game_end_from_outs_into`.
3. With `outs = None` on V1 tick: outs-based evaluators see `None` and do not fire.

### Dedup: Per-Provider Rows

V1 and BoltOdds dedup independently.

**V1:** String-based dedup via `StateRow` with `InlineStr` fields: `home_score_raw`,
`away_score_raw`, `free_text_raw`. Stored in `engine.rows[GameIdx]`.

**BoltOdds:** Integer-based dedup via `BoltOddsBaseballRow`:
```
BoltOddsBaseballRow {
    outs: u8,
    strikes: u8,
    inning: i64,
    top_of_inning: bool,
    home_score: i64,
    away_score: i64,
}
```
Stored in `engine.bo_rows[GameIdx]`. A BoltOdds frame that only changes `ball` count
(no change to outs, strikes, score, or inning) is deduplicated out — no evaluation
runs. Base runner changes without score/outs/strikes changes are also filtered
(base state is not in the dedup row). This is correct because base state only matters
for evaluation when score, outs, or strikes also changed.

### Frame Pipelines

**V1 pipeline** (`baseball/frame_pipeline.rs`):
```
fast_extract_v1(frame_text)
  -> check_duplicate(fixture_id, home_str, away_str, free_text)
  -> parse_period(free_text), fast_parse_score(home_str), fast_parse_score(away_str)
  -> process_tick_live(gidx, ...)
  -> dispatch_intents / pop_for_target + send_batch
  -> flush_tick_logs (src: "kalstrop_v1")
```

**BoltOdds pipeline** (`boltodds_baseball_frame_pipeline.rs`):
```
fast_extract_boltodds_baseball(frame_text)
  -> check_boltodds_game(game_label)
  -> process_boltodds_tick_live(gidx, outs, strikes, inning, top_of_inning,
       home_score, away_score, base1, base2, base3, period_detail, recv_monotonic_ns)
  -> dispatch_intents(intents, dispatch_handle, log)
  -> return BoltOddsBaseballPendingLog (src: "boltodds")
```

### Worker: Multiplexed V1 + BoltOdds

The existing `ws_multiplexed.rs` manages V1 + V2 + BoltOdds connections for both
soccer and baseball. The sport engine is determined at startup via `detect_sport_from_plan()`.
For baseball, the worker runs with `SportEngine::Baseball(NativeMlbEngine)`.

Frame dispatch is sport-aware:
- V1 frames are dispatched to `baseball::frame_pipeline::process_decoded_frame_sync`
  (for `SportEngine::Baseball`) or the soccer equivalent.
- BoltOdds frames are dispatched to `process_boltodds_baseball_frame_sync`
  (for `SportEngine::Baseball`) or the soccer equivalent.

V2 is not used for baseball (V2 does not support baseball). The multiplexed worker
handles this gracefully — V2 connection is simply not established when there are no
V2 subscriptions.

### BoltOdds Baseball Frame Extraction

`boltodds_baseball_types.rs` contains a byte-level extractor
(`fast_extract_boltodds_baseball`) using prebuilt `LazyLock<Finder>` statics.
Fields are extracted via sequential byte scanning following JSON field order:

| Field | JSON key | Finder | Type | Notes |
|-------|----------|--------|------|-------|
| `game_label` | `"game"` | `FINDER_GAME` | `&str` | Game label for lookup |
| `outs` | `"out"` | `FINDER_OUT` | `u8` | 0-3 |
| `inning` | `"inning"` | `FINDER_INNING` | `i64` | 1-based |
| `strikes` | `"strike"` | `FINDER_STRIKE` | `u8` | 0-3 |
| `base1` | `"base1"` | `FINDER_BASE1` | `bool` | Runner on 1st |
| `base2` | `"base2"` | `FINDER_BASE2` | `bool` | Runner on 2nd |
| `base3` | `"base3"` | `FINDER_BASE3` | `bool` | Runner on 3rd |
| `top_of_inning` | `"topOfInning"` | `FINDER_TOP_OF_INNING` | `bool` | true=top, false=bottom |
| `period_detail` | `"matchPeriod"[1]` | `FINDER_MATCH_PERIOD` | `&str` | e.g. `"AT_TOP_4TH_INNING"` |
| `home_score` | `"totalRunsForTeamA"` | `FINDER_TOTAL_RUNS_A` | `i64` | BoltOdds A=home |
| `away_score` | `"totalRunsForTeamB"` | `FINDER_TOTAL_RUNS_B` | `i64` | BoltOdds B=away |

JSON field order in BoltOdds baseball frames:
```
..."out":N,"inning":N,...,"strike":N,"ball":N,"base1":bool,"base2":bool,"base3":bool,
"topOfInning":bool,...,"matchPeriod":["BaseballMatchPeriod","AT_TOP_1ST_INNING"],...,
"totalRunsForTeamA":N,"totalRunsForTeamB":N,...
```

The extractor returns `None` for non-`match_update` frames or any missing field.

### Tick Logging

Baseball tick events include provider-specific fields:

| Field | V1 value | BoltOdds value |
|-------|----------|----------------|
| `src` | `"kalstrop_v1"` | `"boltodds"` |
| `outs` | `null` | `0`-`3` |
| `strikes` | `null` | `0`-`3` |
| `base1` | `null` | `true`/`false` |
| `base2` | `null` | `true`/`false` |
| `base3` | `null` | `true`/`false` |
| `period_raw` | freeText (e.g. `"Break top 2 bottom 1"`) | matchPeriod[1] (e.g. `"AT_TOP_1ST_INNING"`) |

`runs_home`, `runs_away`, `inn`, `inn_half`, `gs`, and `lg` are common to both.

### Configuration

```python
# config/mappings.py
LEAGUES = {
    "mlb": {
        "provider": ["kalstrop_v1", "boltodds"],
        ...
    }
}
```

The orchestrator handles list-valued `provider` fields and spawns the multiplexed
worker. Per-provider subscription routing (`HashMap<String, Vec<String>>`) works for
BoltOdds (subscribes by game labels from the plan's `alternate_provider_game_ids`).

### Linking

BoltOdds baseball games are synced by `provider sync`. `link build` produces BoltOdds
game labels in `alternate_provider_game_ids`. The BoltOdds game label format is a
string like `"Arizona Diamondbacks vs Colorado Rockies, 2026-05-21, 09"`. The linker
matches via team name aliases (`config/baseball_mappings.py`) and date/time proximity.

---

## Summary of Expected Gains

| Signal | Current (V1 only) | With `out=3` (BoltOdds) |
|--------|:--:|:--:|
| NRFI NO | Baseline | +5.2s mean (100% of games) |
| NRFI YES | Baseline (run delta) | Same timing (whichever provider first) |
| Game end (non-walkoff) | Baseline | +9.1s mean (~83-91% of games) |
| Total overs | Baseline (V1 faster) | Same timing (whichever provider first) |
| Walkoff moneyline | Baseline (V1 faster) | Same timing (whichever provider first) |
| Walkoff partial spreads | Not available | **New**: immediate fire on walkoff detection |
| Walkoff full resolution | Not available | **New**: when bases empty, full resolution |

The system is fail-safe: when BoltOdds does not deliver `out=3` (9% of half-innings),
V1 evaluators fire at their normal timing. No bet is missed — the optimization is
purely additive. The `out=2, strike=3` signal is intentionally not used due to its
68% false positive rate (see section 2 under Empirical Findings).

---

## Files

| File | Role |
|------|------|
| `baseball/types.rs` | `GameState` (shared, with `outs`, `strikes`, `base1`, `base2`, `base3`), `BoltOddsBaseballRow` (dedup), `GameTargets`, `SpreadSlot` |
| `boltodds_baseball_types.rs` | `BoltOddsBaseballExtract`, `fast_extract_boltodds_baseball` (byte-level extractor with `LazyLock<Finder>` statics) |
| `boltodds_baseball_frame_pipeline.rs` | `process_boltodds_baseball_frame_sync` (extract, engine tick, dispatch) |
| `baseball/engine.rs` | `process_tick_live` (V1 entry, sets base fields to None), `process_boltodds_tick_live` (BoltOdds entry, sets base fields from extract), `check_boltodds_game` (game_label lookup) |
| `baseball/eval.rs` | `evaluate_walkoff_into` (Tier 1 + Tier 2), `evaluate_nrfi_from_outs_into` (early YES + outs-based NO), `evaluate_game_end_from_outs_into` (non-walkoff game end) |
| `baseball/frame_pipeline.rs` | V1 baseball frame pipeline (unchanged, logs `src: "kalstrop_v1"`) |
| `ws_multiplexed.rs` | Sport-aware frame dispatch (V1 frames to sport pipeline, BoltOdds frames to sport-specific BoltOdds pipeline) |
| `log_writer.rs` | Baseball `TickPayload` variant with `src`, `outs`, `strikes`, `period_raw` fields |
