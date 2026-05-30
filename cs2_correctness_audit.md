# CS2 Hotpath Correctness Audit

Auditor: Claude Opus 4.6  
Date: 2026-05-29  
Scope: Full CS2 pipeline — Rust evaluation, Python compilation, linking, frame extraction, cross-sport isolation

---

## 1. Critical Issues (would cause incorrect bets)

### CRITICAL-1: `final_resolved_games` blocks map_handicap from ever firing

**Files:** `cs2/eval.rs:125-158` (moneyline), `cs2/eval.rs:219-257` (map_handicap)

`evaluate_moneyline_into` sets `self.final_resolved_games[gi] = true` on line 157. `evaluate_map_handicap_into` checks this flag on line 226 and returns early if set. Both evaluators trigger at match end (`maps_home >= mtw || maps_away >= mtw || match_completed`), and the call order in `engine.rs:437-440` is:

```rust
self.evaluate_child_moneyline_into(gidx, &state, &mut intents);  // 1st
self.evaluate_moneyline_into(gidx, &state, &mut intents);        // 2nd — sets flag
self.evaluate_totals_into(gidx, &state, &mut intents);           // 3rd — no flag check
self.evaluate_map_handicap_into(gidx, &state, &mut intents);     // 4th — BLOCKED
```

**Impact:** For any CS2 game with both moneyline and map_handicap markets (the common case), map_handicap intents are silently suppressed. The bot never places handicap bets despite having presigned orders ready. Every map_handicap market is a missed trade.

**Comparison with other sports:** Tennis's `evaluate_moneyline_into` (`tennis/engine.rs:743-772`) does NOT set `final_resolved_games`. It relies on the presign pool's one-shot gate for double-fire prevention. The CS2 code is inconsistent with this pattern.

**Correct behavior:** Remove `self.final_resolved_games[gi] = true` from inside `evaluate_moneyline_into` and set it in `process_tick_live` after all evaluators return, or remove the `final_resolved_games` check from `evaluate_map_handicap_into`:

```rust
// In process_tick_live, after all evaluators:
let mtw = self.maps_to_win[gi];
if match_completed || maps_home >= mtw || maps_away >= mtw {
    self.final_resolved_games[gi] = true;
}
```

---

### CRITICAL-2: Map handicap compiler produces wrong SpreadSide — both outcomes fire simultaneously

**File:** `compiler.py:235-278`

The `_parse_outcome_semantic` function for `map_handicap` determines `SpreadSide` from each outcome's label independently. The two outcomes of the same market have different team labels, producing different sides:

```python
# Outcome index 0 (favored team, e.g. NIP/away):
label_side = "away"  # NIP matched as away
return "away_covers"  # idx == 0

# Outcome index 1 (underdog, e.g. magic/home):
label_side = "home"  # magic matched as home
return "home_not_covers"  # idx == 1
```

In the Rust engine (`engine.rs:273-308`), these create two separate `SpreadSlot`s because the slot lookup key is `(side, line)`:

```rust
// Slot A: SpreadSlot { side: Away, line: -1.5, covers_idx: NIP_token, not_covers_idx: None }
// Slot B: SpreadSlot { side: Home, line: -1.5, covers_idx: None, not_covers_idx: MGC_token }
```

**Concrete walkthrough — NIP sweeps 2-0 (home=MGC=0, away=NIP=2):**

```
margin_home = 0 - 2 = -2

Slot A (Away): margin = -margin_home = 2
  2 + (-1.5) = 0.5 > 0 → covers → fires NIP_token ✓

Slot B (Home): margin = margin_home = -2
  -2 + (-1.5) = -3.5 ≤ 0 → not_covers → fires MGC_token ✗
```

Both tokens fire. NIP_token (outcome 0, "NIP covers -1.5") correctly resolves YES. MGC_token (outcome 1, "magic gets +1.5") resolves NO because NIP swept — but the bot bought YES. **One correct bet and one wrong bet on every map handicap resolution where the favored team covers.**

**Impact:** Every CS2 game with map_handicap markets fires an extra incorrect bet when the favored team covers the spread. This is real money lost. Note that CRITICAL-1 currently prevents this from firing at all (map_handicap is blocked by moneyline). But fixing CRITICAL-1 without fixing CRITICAL-2 would activate this bug.

**Correct behavior:** Both outcomes must share the same `SpreadSide` — the side of the FAVORED team (idx=0). The fix is to determine the side once from the idx=0 label and apply it to both outcomes:

```python
if sports_type == "map_handicap":
    # Determine the favored team's side from the idx=0 label
    label_side = ""
    if home_norm and (...):
        label_side = "home"
    elif away_norm and (...):
        label_side = "away"
    ...
    if not label_side:
        return "unknown"

    # Both outcomes use the FAVORED team's side
    # idx=0 (favored) → covers; idx=1 (underdog) → not_covers
    # But the side is always the favored team's side
    if idx == 0:
        return f"{label_side}_covers"
    else:
        return f"{label_side}_not_covers"
```

**Comparison:** Tennis set handicap (`compiler.py:209-216`) uses the slug to determine the side — both outcomes share the same side. CS2 map handicap should follow the same pattern.

---

## 2. Potential Issues (edge cases under specific conditions)

### POTENTIAL-1: No cold-start protection for totals progressive evaluation

**File:** `cs2/engine.rs:421-433`

On the first tick, `prev.total_maps` is 0 (from `Default`), and `process_tick_live` wraps it in `Some(0)`:

```rust
prev_total_maps: Some(prev.total_maps),  // Some(0) on first tick, NOT None
```

In `evaluate_totals_into` (`eval.rs:178`), the `if let Some(prev_total)` guard passes with `prev_total = 0`. If the first tick shows `total_maps > 0` (mid-match subscription), the progressive over check fires for all lines in `[0, total_maps)`.

**Comparison:** Tennis uses `rows[gi].is_none()` to detect first observation and skips progressive evaluation when prev is `None` (CLAUDE.md: "Progressive evaluators skip entirely when prev is `None`"). CS2 lacks this guard.

**Real-world impact:** Low. V1 prematch games produce no frames, so the first tick should show 0-0 maps. Only manifests on mid-match subscription (restart scenario), where firing on current state may be desirable anyway. But the inconsistency with tennis's design is concerning.

---

### POTENTIAL-2: Serde fallback path missing round-score extraction

**File:** `cs2/frame_pipeline.rs:41-48`

The serde path (for frames starting with `[`) passes empty strings for round scores:

```rust
if let Some(tl) = process_extracted_fields(
    engine, u.fixture_id, home_str, away_str, free_text,
    "", "", None, // rounds and phase from serde path not available
    ...
```

This disables Signal 1 (round-13 map winner detection) for array-formatted frames. Signal 2 (maps-won fallback) still works, but fires 30-100 seconds later.

**Real-world impact:** Low. V1 live update frames are typically single JSON objects (non-array), handled by `fast_extract_cs2_v1`. The serde path handles initial subscription/protocol frames. But if V1 ever delivers live data as arrays, the round-13 edge is lost.

---

### POTENTIAL-3: Simultaneous maps-won double-increment produces wrong map attribution

**File:** `cs2/eval.rs:90-118`

If V1 drops frames and both `maps_home` and `maps_away` increment in a single tick (e.g., from 0-0 to 1-1), the fallback signal computes the same `completed_map` for both:

```rust
// Home wins a map:
let completed_map = prev_home + prev_away + 1;  // 0 + 0 + 1 = 1

// Away also wins a map (same tick):
let completed_map = prev_home + prev_away + 1;  // 0 + 0 + 1 = 1  ← SAME!
```

Both signals target map 1. The home signal fires first (line 95 before line 108), and `map_winner_resolved[gi][0]` blocks the away signal. Result: home incorrectly attributed as map 1 winner (maybe away won map 1 and home won map 2), and map 2 winner is never fired.

**Real-world impact:** Very low. V1 never delivers simultaneous double-increments in observed data (maps are sequential; one completes before the next starts). But the formula is fragile under frame loss.

---

### POTENTIAL-4: "OG" team fails label matching (2-character label)

**File:** `compiler.py:219-228`

The child_moneyline label matching has a `len(label) >= 3` guard:

```python
if home_norm and (home_norm in label or (len(label) >= 3 and label in home_norm)):
    return "home"
```

Team "OG" has V1 name "Team OG" (normalized: "team og"), PM code "og1", PM alias "OG" (normalized: "og", length 2):

- `"team og" in "og"` → FALSE
- `len("og") >= 3` → FALSE → skip
- `"og1" in "og"` → FALSE

All matching paths fail. Returns "unknown". Same for moneyline (guard is `len(label) >= 4`).

**Real-world impact:** Medium. OG games would have their moneyline and child_moneyline markets silently skipped. The `hotpath compile --league cs2` dry-run flags this as `⚠️ UNKNOWN SEMANTIC`, so it wouldn't go live unnoticed. Fix: add "og" as a pm_alias that matches the V1 name substring, or adjust the guard.

---

### POTENTIAL-5: Cross-validation skipped when question text uses unrecognized abbreviation

**File:** `compiler.py:250-274`

The map_handicap cross-validation parses the favored team name from the question text:

```python
favored_match = _re.search(r':\s*(.+?)\s*\(-', _norm(question))
```

If the extracted `favored_label` doesn't match any of `home_norm`, `away_norm`, `home_code`, `away_code`, then `question_side` stays empty and the cross-validation is silently skipped:

```python
if question_side:  # empty → entire block skipped
    if idx == 0 and label_side != question_side: return "unknown"
    if idx == 1 and label_side == question_side: return "unknown"
```

The semantic is derived solely from label matching, with no second verification. This weakens the dual-method validation the design doc mandates.

**Real-world impact:** Low. The label matching itself is usually correct. But if a team name appears in an unexpected form in the question, the safety net is absent.

---

### POTENTIAL-6: Error message cosmetic bug

**File:** `compiler.py:1015`

```python
if sport not in {"baseball", "soccer", "tennis", "cs2"}:
    raise HotPathPlanError("invalid_sport", f"sport must be baseball/soccer/tennis, got: {sport!r}")
```

The error message says "must be baseball/soccer/tennis" but the validation includes "cs2". Should say "baseball/soccer/tennis/cs2".

---

## 3. Verified Correct

### 3.1 Map winner detection (`map_winner` function)

**File:** `cs2/eval.rs:30-47`

The closed-form condition:

```rust
let decided =
    (big == 13 && small <= 11)                            // regulation
    || (big >= 16 && big % 3 == 1 && d >= 2 && d <= 4);  // overtime
```

**Verified correct.** Matches the derivation in `docs/cs_map_win_condition.md` exactly. Each clause is load-bearing:

- `big % 3 == 1`: kills the catastrophic trap case 17-15 (OT2 at 2-0, undecided despite margin 2). `17 % 3 = 2 ≠ 1`.
- `d >= 2`: kills 13-12 and 16-15 (1-0 in OT set).
- `d <= 4`: OT margin caps at 4 (set is 6 rounds, max lead is 4-0).
- `big == 13` / `small <= 11`: separates regulation from OT branches (13-12 is OT territory).

Winner determination `rounds_home > rounds_away → "home"` is correct — in a decided game, margin is always ≥ 2, so the comparison is never ambiguous.

Tests cover regulation (13-0 through 13-11), OT1 (16-12 through 16-14), OT2 (19-15 through 19-17), OT3 (22-18 through 22-20), OT4 (25-21 through 25-23), and all critical non-decided scores (12-12, 13-12, 15-15, 16-15, 17-15 trap, 18-18, 21-21).

---

### 3.2 Child moneyline Signal 1 — round-13 primary detection

**File:** `cs2/eval.rs:69-87`

- Correctly guards on `state.current_map > 0` (skips when `currentPhase` is null/absent).
- Correctly converts 1-based `current_map` to 0-based `map_idx = (state.current_map - 1) as usize`.
- Bounds-checks both `targets.map_moneyline.len()` and `self.map_winner_resolved[gi].len()` before access.
- Uses `map_winner()` for the detection — inherits its correctness for all score scenarios.
- Sets `map_winner_resolved[gi][map_idx] = true` after firing, preventing double-fire from Signal 2.

---

### 3.3 Child moneyline Signal 2 — maps-won fallback

**File:** `cs2/eval.rs:89-118`

- `completed_map = prev_home + prev_away + 1` correctly identifies which map just completed:
  - Map 1 at (0,0): `0+0+1=1` ✓
  - Map 2 at (1,0): `1+0+1=2` ✓
  - Map 2 at (0,1): `0+1+1=2` ✓
  - Map 3 at (1,1): `1+1+1=3` ✓
- `map_winner_resolved` flag prevents double-fire when Signal 1 fires first (test `child_ml_no_double_fire` at eval.rs:519-536 verifies this).
- Correctly handles BO1 (`map_moneyline` has 1 entry, `map_winner_resolved` length 1).

---

### 3.4 Moneyline evaluator

**File:** `cs2/eval.rs:125-158`

- Fires on `maps_home >= mtw || maps_away >= mtw || match_completed`. Both the threshold check and the `freeText = "Closed"` fallback trigger resolution.
- Winner determination: `maps_home > maps_away` / `maps_away > maps_home` with fail-closed on ties (no intent fired).
- Tests cover BO3 home win (2-0), BO3 away win (1-2), 1-1 mid-match (no fire), and `match_completed` fallback.

---

### 3.5 Totals evaluator

**File:** `cs2/eval.rs:164-213`

- Progressive over: iterates sorted `over_lines`, fires when `ol.half_int >= prev && ol.half_int < now`. The `ol.half_int >= now` early break is correct for sorted arrays.
- Under fires only at match end (`match_completed || maps >= mtw`).
- `totals_under_emitted[gi]` prevents double-fire on under (presign pool also guards this, but the flag avoids unnecessary evaluation).
- First-tick guard: `if let Some(prev_total) = state.prev_total_maps` — `Some(0)` on first tick means progressive check runs (see POTENTIAL-1), but the `total_now > prev_total` condition prevents false-fire if total is 0.

---

### 3.6 Map handicap evaluator (logic only — semantics bug is CRITICAL-2)

**File:** `cs2/eval.rs:219-257`

- Margin formula: `margin_home = maps_home - maps_away`, sign-flipped for Away side. Correct for the standard spread evaluation pattern.
- Fires only at match end (same condition as moneyline/totals under).
- Walk-through examples (ASSUMING correct single-slot creation):
  - Home -1.5, final 2-0: `margin=2, 2+(-1.5)=0.5 > 0` → covers ✓
  - Home -1.5, final 2-1: `margin=1, 1+(-1.5)=-0.5 ≤ 0` → not_covers ✓
  - Away -1.5, final 0-2: `margin=2 (negated), 2+(-1.5)=0.5 > 0` → covers ✓
  - Away -1.5, final 1-2: `margin=1 (negated), 1+(-1.5)=-0.5 ≤ 0` → not_covers ✓

---

### 3.7 `fast_extract_cs2_v1`

**File:** `fast_extract.rs:488-588`

- Correctly extracts all 7 fields: `fixture_id`, `maps_home` (top-level homeScore), `maps_away` (top-level awayScore), `rounds_home` (currentPhase.homeScore), `rounds_away` (currentPhase.awayScore), `free_text` (freeText), `current_phase` (currentPhase.phase number).
- Handles `currentPhase: null` by checking `bytes[p] != b'n'` at the value start, leaving rounds/phase as default empty/None.
- Uses the same prebuilt finders (`FINDER_FIXTURE_ID`, `FINDER_HOME_SCORE`, etc.) as other extractors — no shared mutable state between extractors.
- Quick-reject on `"type":"next"` avoids processing non-update frames.

---

### 3.8 Frame pipeline

**File:** `cs2/frame_pipeline.rs:15-78`

- Calls `fast_extract_cs2_v1` (not tennis/baseball extractor).
- `match_completed` checks `free_text.trim().eq_ignore_ascii_case("Closed")` — correct for CS2 (not "Ended" like other sports).
- Scores default to 0 on parse failure (`fast_parse_score(...).unwrap_or(0)`).
- Dispatch follows the same pattern as other sports: intents → pop presigned orders → batch → send. Noop mode logs without touching the pool.
- Tick logging happens AFTER dispatch (correct ordering per Critical Invariant #1).

---

### 3.9 Plan loading (`engine.rs`)

**File:** `cs2/engine.rs:48-356`

- `maps_to_win` read from `sets_to_win` in plan JSON (line 115): correct — reuses the tennis field.
- `map_moneyline` vector grows dynamically via `parse_map_number_from_strategy_key` (lines 224-229). Correctly handles out-of-order target processing (grows to fit the highest map number).
- `map_winner_resolved` sized to `(maps_to_win * 2 - 1).max(1)` (line 337): correct max maps for BO1=1, BO3=3, BO5=5.
- `TargetRegistry` built with cloned `target_slots` and `tokens` (lines 342-353): correct pattern.
- Token deduplication via `token_id_to_idx` HashMap (line 189): matches the shared-pool semantics.
- `over_lines` and `under_lines` sorted by `half_int` after all targets processed (lines 316-317): required for the sorted-break optimization in totals evaluation.
- `strategy_keys` HashSet populated for merge_plan dedup (line 201): consistent with other sports.

---

### 3.10 Python compiler — CS2-specific branches

**File:** `compiler.py:507, 742-754, 905-920, 1014`

- Sport validation: `"cs2"` included in the allowed set at lines 507 and 1014.
- `maps_to_win` parsing: `re.search(r"\(BO(\d+)\)", q)` → `(bo_n + 1) // 2` (line 914). Verified: BO1→1, BO3→2, BO5→3. Fail-closed on missing BO format (line 920: `continue` skips the game).
- Map number extraction from slug: `re.search(r"-game(\d+)", slug)` (line 744). Maps `-game1` → 1, `-game2` → 2.
- Strategy keys: `{gid}:CHILD_MONEYLINE:MAP{N}:{HOME/AWAY}` (line 747), `{gid}:MAP_HANDICAP:{semantic}:{line}` (line 754). Both are self-describing and parseable by Rust.
- Child moneyline semantic: label-based team matching with substring and PM code fallback (lines 219-228). Same pattern as baseball moneyline. Correct for most teams (see POTENTIAL-4 for the "OG" edge case).

---

### 3.11 Linker — CS2 league resolution

**File:** `linking/service.py:189-248`

The `sport_raw` fallback (lines 230-238) correctly handles CS2's league resolution:

```python
# Fall back to sport_raw as alias key. Esports titles use sport_raw
# as the stable identifier (e.g., "CS2") while league_raw varies by
# tournament (e.g., "stake ranked episode 2").
if not canonical_league and sport_raw and sport_raw != league_signal:
    canonical_league = _norm(provider_alias_map.get(sport_raw) or "")
```

When `league_raw` is a tournament name (e.g., "stake ranked episode 2"), `league_signal` fails primary resolution. The fallback uses `sport_raw` ("cs2") as an alias key, which maps to the canonical "cs2" league via `PROVIDER_LEAGUE_ALIASES` in `mappings.py` (line 112: `"cs2": "cs2"`).

Slug-hint fallback for empty `pm_event_teams` is handled by the existing slug-based matching in `_score_candidates` — CS2 events have team names in the slug prefix (e.g., `cs2-mgc-nip-...`).

---

### 3.12 `maps_to_win` config and provider setup

**File:** `config/mappings.py:61-65`

```python
"cs2": {
    "polymarket_league_code": "cs2",
    "sport_family": "cs2",
    "provider": ["kalstrop_v1", "boltodds"],
},
```

No `sets_to_win` default — correct, because CS2 BO format is parsed per-game from PM event titles, not per-league. The compile-time `sets_to_win` parameter (default 2) is overridden by the per-game BO parser in `compiler.py:905-920`.

---

## 4. Untested Scenarios

### 4.1 Map winner in overtime (integration test)
The `map_winner` function has extensive unit tests, but there is no integration test that feeds an OT-win round score (e.g., 16-14) through `process_tick_live` → `evaluate_child_moneyline_into` → intent emission. Signal 1 with OT scores should be tested end-to-end.

### 4.2 Behavior B → Signal 2 in multi-map sequences
No test covers a full BO3 sequence where multiple maps use the maps-won fallback (Signal 2) exclusively. A test should simulate: map 1 via Signal 1 (round-13), map 2 via Signal 2 (skipped round-13), map 3 via Signal 2 — verifying correct map attribution across the sequence.

### 4.3 BO5 map numbering
No test covers BO5 (maps_to_win=3). Should verify `map_winner_resolved` sizing (length 5) and that map 4 and map 5 child_moneyline targets fire correctly via both signals.

### 4.4 Match completion via `freeText = "Closed"` without maps reaching threshold
No test covers the case where `match_completed = true` but `maps_home < mtw && maps_away < mtw` (should this happen?). The moneyline evaluator would fire with maps possibly tied, producing no intent (fail-closed). But this scenario should be explicitly tested.

### 4.5 Cold-start mid-match subscription
No test simulates subscribing to a game already at 1-0 in maps with no prior ticks. Should verify that:
- Child moneyline fallback fires for map 1 (intentional behavior)
- Totals over fires for lines below total_maps (POTENTIAL-1)
- These behaviors are acceptable or need guarding

### 4.6 Map handicap with BO5 and non-1.5 lines
The test suite only covers BO3 with ±1.5 lines. BO5 games may have ±2.5 lines. Should test:
- Home -2.5, final 3-0: covers (margin=3, 3-2.5=0.5>0)
- Home -2.5, final 3-1: not covers (margin=2, 2-2.5=-0.5≤0)
- Home -2.5, final 3-2: not covers (margin=1, 1-2.5=-1.5≤0)

### 4.7 Serde fallback path round-score absence
No test sends an array-formatted frame through `process_decoded_frame_sync` and verifies that Signal 1 is correctly disabled (rounds=0, map_winner returns None) while Signal 2 still works.

### 4.8 Empty `map_moneyline` for match-level-only games
No test covers a game with only moneyline/totals/handicap markets but NO child_moneyline markets. Should verify `has_child_moneyline[gi] = false` causes the evaluator to return early without panicking on empty `map_moneyline` vectors.

---

## 5. Cross-Sport Contamination Check

### 5.1 Import isolation — CONFIRMED CLEAN

`cs2/` module files import only from `crate::*` (shared types) and `crate::cs2::types::*` (own types). Zero imports from `crate::baseball`, `crate::soccer`, or `crate::tennis`:

```
$ grep -rn 'use crate::baseball\|use crate::soccer\|use crate::tennis' native/.../src/cs2/
(no output — clean)
```

### 5.2 Own `SpreadSlot` — CONFIRMED

`cs2/types.rs:17-23` defines its own `SpreadSlot` struct, independent of `soccer/types.rs` and `tennis/types.rs`. Uses the shared `SpreadSide` enum from `lib.rs` (which is sport-agnostic).

### 5.3 Own frame extractor — CONFIRMED

`fast_extract_cs2_v1` (`fast_extract.rs:488-588`) is a dedicated function with its own `Cs2V1Extract` struct (`fast_extract.rs:476-484`). Does not share mutable state with `fast_extract_v1` (soccer/baseball), `fast_extract_tennis_v1` (tennis), or `fast_extract_boltodds`. Uses shared prebuilt finders (read-only, constructed once).

### 5.4 `SportEngine::Cs2` dispatch — CONFIRMED COMPLETE

All dispatch points in `lib.rs` include `Cs2` match arms:
- `active_subscriptions_for_candidates` (line 173)
- `merge_plan` (line 186)
- `tokens` (line 195)
- `target_slots` (line 204)
- `set_registry` (line 213)
- `game_ids` (line 222)
- `all_token_ids` (line 231)
- `clone_registry` (line 240)
- `token_ids_by_game_len` (line 249)

`runtime.rs:104-109` constructs `SportEngine::Cs2` for sport="cs2".  
`ws.rs:509-517` dispatches to `cs2::frame_pipeline::process_decoded_frame_sync`.  
`runtime.rs:198-203` extracts league list for startup log.  
`runtime.rs:312` clones `SportEngine::Cs2`.

### 5.5 No CS2 in multiplexed worker — CONFIRMED

`ws_multiplexed.rs` has zero references to `Cs2` or `cs2`. CS2 uses `ws.rs` (V1-only worker) as designed.

### 5.6 Match completion signal — CONFIRMED `"Closed"` (not `"Ended"`)

`cs2/frame_pipeline.rs:145`:
```rust
free_text.trim().eq_ignore_ascii_case("Closed")
```

Other sports use `"Ended"`. CS2 correctly uses `"Closed"`. No cross-contamination.

### 5.7 Log writer — CONFIRMED

`log_writer.rs:41-55` defines `TickPayload::Cs2` with CS2-specific fields (`maps_home`, `maps_away`, `rounds_home`, `rounds_away`, `current_map`). Lines 246-269 serialize these fields. `log_tick` dispatch at line 125 includes the `Cs2` variant.
