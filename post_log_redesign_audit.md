# Post-Log-Redesign Audit

Audit performed 2026-05-25 against working tree on `main`. Read-only — no code changes.

---

## Section 1: Log Schema V2 Regression Risks

### 1.1 `game_leagues` vector grows in lockstep — SAFE

All three engines push to `game_leagues` immediately after `game_ids` in the same loop iteration during `load_plan_from_json`:
- [baseball/engine.rs:128](native/polybot2_native/src/baseball/engine.rs:128)
- [soccer/engine.rs:164](native/polybot2_native/src/soccer/engine.rs:164)
- [tennis/engine.rs:161](native/polybot2_native/src/tennis/engine.rs:161)

`merge_plan` (soccer [line 733](native/polybot2_native/src/soccer/engine.rs:733), tennis [line 880](native/polybot2_native/src/tennis/engine.rs:880)) also pushes to `game_leagues` when creating new games. Baseball `merge_plan` only processes existing games — correct.

All frame pipelines access `game_leagues` via `.get()` with `.unwrap_or("")` fallback, so even a hypothetical desync would degrade logging (empty `lg` field), not panic.

**One gap:** The `#[cfg(test)]` helper `add_game` in [baseball/engine.rs:1083–1118](native/polybot2_native/src/baseball/engine.rs:1083) does NOT push to `game_leagues`. Any test exercising `flush_tick_logs` after `add_game` would silently log an empty league. **Severity: Low** (test-only, no production impact).

### 1.2 `canonical_league` in plan JSON — all paths covered

- [native_engine.py:58](src/polybot2/hotpath/native_engine.py:58): `serialize_compiled_plan` includes `"canonical_league": str(game.canonical_league or "")`
- [compiler.py:818](src/polybot2/hotpath/compiler.py:818): sets `canonical_league=str(meta.canonical_league)`
- [v2_resolver.py:189](src/polybot2/hotpath/v2_resolver.py:189): `compile_for_resolved_game` uses `dataclasses.replace` on a game that already has `canonical_league`
- Incremental refresh reuses existing `CompiledGamePlan` objects, which carry `canonical_league` through

Rust defaults to `""` via `unwrap_or("")` if the field is missing in JSON. No regression risk.

### 1.3 `gid_from_sk` edge cases — SAFE

[log_writer.rs:73–75](native/polybot2_native/src/log_writer.rs:73):
```rust
pub(crate) fn gid_from_sk(sk: &str) -> &str {
    sk.split_once(':').map_or(sk, |(g, _)| g)
}
```

- Empty `sk` → returns `""` (harmless in log output)
- `sk = "_"` (fallback from OOB target resolution) → returns `"_"` (correct diagnostic signal)
- `sk` without colon → returns the full string (degrades gracefully)

No panic path. No wrong behavior.

### 1.4 `corners_home`/`corners_away` carry-forward — SAFE

[soccer/engine.rs:621–648](native/polybot2_native/src/soccer/engine.rs:621): New `SoccerGameState` initializes with `corners_home: prev.corners_home` and `corners_away: prev.corners_away`. Corner values update only when both are `Some` (line 643). Non-corner ticks (V2, BoltOdds without corners) preserve the previous corner state.

### 1.5 Log directory creation — minor

[runtime.rs:160](native/polybot2_native/src/runtime.rs:160): `std::fs::create_dir_all(&log_subdir).ok()` swallows errors. If the directory cannot be created, `LogWriter::open` fails with a less informative error. The process still crashes (fail-closed via `PyValueError`), but the root cause is obscured. **Severity: Low.**

### 1.6 Python V2/V1 field fallback — SAFE

- [live_observer.py:268–295](src/polybot2/hotpath/live_observer.py:268): dispatches on `sport` field to read sport-specific score names (`runs_home`/`goals_home`/`sets_home`) with V1 `h`/`a` fallback. Correct.
- [tracker.py:117–118](src/polybot2/guardian/tracker.py:117): hardcodes `goals_home`/`goals_away` with `h`/`a` fallback. Only activated for soccer (per `GuardianManager`), so the hardcoding is correct for its scope. **Latent risk if Guardian is extended to other sports:** would silently drop all ticks. **Severity: Low.**
- [log_reader.py:9–32](src/polybot2/hotpath/log_reader.py:9): reads only `ev`/`ok`/`sk` fields, unchanged between V1/V2. No fallback needed.

---

## Section 2: Defensive Coding Pattern Risks

### 2.1 CRITICAL — V2 `PostMatch` could fire final evaluators on extra-time score

**File:** [kalstrop_v2_types.rs:152](native/polybot2_native/src/kalstrop_v2_types.rs:152)
```rust
"FullTimeNormalTime" | "PostMatch" => ("Ended", true),
```

**Risk:** For cup matches with extra time (UCL knockout rounds), BetGenius may send `PostMatch` after extra time/penalties with a score that includes ET goals. Two scenarios:

1. **`FullTimeNormalTime` arrives first (likely):** The `final_resolved_games` tombstone ([soccer/eval.rs:72](native/polybot2_native/src/soccer/eval.rs:72)) blocks `PostMatch` re-evaluation, AND the presign pool blocks re-fire. **Safe.**

2. **`FullTimeNormalTime` is skipped, only `PostMatch` arrives (unknown):** The tombstone is not set. Moneyline, spreads, and halftime evaluators fire on the ET-inclusive score. **Wrong bets.** For example: a match 1-1 at full time goes to 2-1 in ET → moneyline fires HOME_YES instead of DRAW_YES.

**Why this is the "Interrupted" pattern:** We have no confirmed BetGenius documentation on the phase sequence for extra-time matches. Accepting `PostMatch` as equivalent to `FullTimeNormalTime` is speculative. Polymarket markets scope to regular full time — the only safe trigger is `FullTimeNormalTime`.

**Severity: Critical** for UCL knockout matches if scenario 2 is possible. We do not have protocol evidence to rule it out.

### 2.2 HIGH — BoltOdds `MATCH_COMPLETED` same extra-time contamination risk

**File:** [boltodds_frame_pipeline.rs:55](native/polybot2_native/src/boltodds_frame_pipeline.rs:55)
```rust
"AT_FULL_TIME" | "MATCH_COMPLETED" => "Ended",
```

**Risk:** Same pattern as 2.1. If BoltOdds sends `MATCH_COMPLETED` after extra time with ET goals in `goalsA`/`goalsB`, and no prior `AT_FULL_TIME` was received, final evaluators fire on the wrong score.

**Mitigating factor:** BoltOdds currently serves EPL only, which has no extra time in regular league matches. UCL uses V1/V2, not BoltOdds. This is latent — only triggers if BoltOdds is used for a cup competition.

**Severity: High** (latent). Would become critical if BoltOdds is added to a knockout-format league.

### 2.3 MEDIUM — V1 extraction returns `""` for missing scores instead of rejecting the frame

**File:** [fast_extract.rs:241–243](native/polybot2_native/src/fast_extract.rs:241)
```rust
home_score: home_score.unwrap_or(""),
away_score: away_score.unwrap_or(""),
free_text: free_text.unwrap_or(""),
```

**Risk:** A V1 frame with a valid `fixtureId` and `"type":"next"` but missing `homeScore`/`awayScore` fields produces a `V1Extract` with empty score strings instead of returning `None`. Downstream: `fast_parse_score("") = None`, and the engine's `.or(prev)` pattern ([baseball/engine.rs:494](native/polybot2_native/src/baseball/engine.rs:494), [soccer/engine.rs:637](native/polybot2_native/src/soccer/engine.rs:637)) preserves the previous score — so the engine state is not corrupted. However, the dedup row IS overwritten with empty strings, causing the next real frame to always pass dedup (because `"3" != ""`), which produces a false delta in the baseball NRFI evaluator. The presign pool prevents any double-fire, so no wrong bets occur.

**The real problem:** The frame should have been rejected (returned `None` from `fast_extract_v1`), not accepted with degraded data. The current behavior masks a protocol anomaly that should be logged as a warning.

**Severity: Medium.** No wrong bets due to presign pool gate, but the `unwrap_or("")` default is fail-open: it admits frames that should be rejected, corrupts dedup state, and masks provider issues.

### 2.4 MEDIUM — Baseball score delta uses `unwrap_or(0)` where 0 is a valid score

**File:** [baseball/engine.rs:473–474](native/polybot2_native/src/baseball/engine.rs:473)
```rust
goal_delta_home = goals_home.unwrap_or(0) - row.goals_home.unwrap_or(0);
goal_delta_away = goals_away.unwrap_or(0) - row.goals_away.unwrap_or(0);
```

**Risk:** If `goals_home` is `None` (from an empty-score frame per 2.3) and `row.goals_home` was `Some(3)`, the delta becomes `0 - 3 = -3`. A negative delta doesn't trigger NRFI (the only consumer, which checks `run_delta > 0`), so no wrong intent fires. But the dedup row now stores `goals_home: None`, and the NEXT real tick computes `3 - 0 = 3`, producing a false-positive `run_delta > 0`. If the game is still in the first inning, NRFI-NO would fire — but this is correct because the score IS 3. And the presign pool prevents a second fire.

**Severity: Medium.** Structurally safe due to presign pool, but the delta computation is semantically wrong when `None` (unknown) is treated as 0 (known zero score). The correct pattern would be to return `None` delta when either operand is `None`.

### 2.5 MEDIUM — Tennis frame pipeline `unwrap_or(0)` on `games_home`/`games_away`

**File:** [tennis/frame_pipeline.rs:42–43](native/polybot2_native/src/tennis/frame_pipeline.rs:42)
```rust
let games_home = fast_parse_score(extract.games_home).unwrap_or(0);
let games_away = fast_parse_score(extract.games_away).unwrap_or(0);
```

**Risk:** At match end, `currentPhase` is `null`, so `games_home`/`games_away` are empty → `unwrap_or(0)` is intentionally correct for this case. But if a mid-match frame has a malformed `currentPhase` (present but with unparseable scores), games default to 0 while `total_games` (from `phases[]` sum) reflects the real count. The evaluators use `total_games`, not `games_home + games_away`, so no wrong bets occur.

**Severity: Medium** (masks mid-match data corruption, but evaluators are structurally immune).

### 2.6 LOW — Walkoff evaluator `unwrap_or(0)` on scores

**File:** [baseball/eval.rs:154–157](native/polybot2_native/src/baseball/eval.rs:154)
```rust
let inning = state.inning_number.unwrap_or(0);
if inning >= 9
    && state.inning_half == "bottom"
    && state.home.unwrap_or(0) > state.away.unwrap_or(0)
```

**Risk:** If both `state.home` and `state.away` are `None`, both default to 0, `0 > 0` is false — safe. The asymmetric case (one `Some`, one `None`) is prevented by the engine guard at [baseball/engine.rs:528](native/polybot2_native/src/baseball/engine.rs:528): `if home.is_some() && away.is_some()`.

**Severity: Low.** Structurally prevented. The `unwrap_or(0)` in the evaluator does not independently validate this assumption, but the invariant is enforced upstream.

### 2.7 LOW — Python moneyline label matching uses bidirectional substring containment

**File:** [compiler.py:144–146](src/polybot2/hotpath/compiler.py:144)
```python
if home_norm and (home_norm in label or (len(label) >= 4 and label in home_norm)):
    return "home"
```

**Risk:** For baseball/tennis moneyline markets, team/player names are matched via substring containment. The `len(label) >= 4` guard prevents very short labels from matching broadly. Soccer moneyline falls through to slug-based detection, so this code only runs for baseball/tennis where labels ARE the team/player names.

**Severity: Low.** Fragile for edge cases with short team names that are substrings of longer names, but mitigated by the length guard and the fact that Polymarket labels are consistently the full team/player name.

### 2.8 LOW — Tennis set totals uses `"over" in label` substring matching

**File:** [compiler.py:194–196](src/polybot2/hotpath/compiler.py:194)
```python
if "over" in label:
    return "over"
```

**Risk:** A label containing "over" as part of a larger word (e.g., "recovery") would incorrectly match. Polymarket tennis set total labels consistently follow "Over X.5"/"Under X.5". Note that the regular totals/corners branch uses `label == "over"` (exact match) — the set totals branch is unnecessarily looser.

**Severity: Low.** Only triggers if Polymarket changes their label format.

---

## Section 3: Recommended Fixes (Prioritized)

### P0 — Separate `PostMatch` from `FullTimeNormalTime` in V2 phase mapping

**File:** [kalstrop_v2_types.rs:152](native/polybot2_native/src/kalstrop_v2_types.rs:152)

**Action:** Remove `PostMatch` from the match-end arm. Either:
- (a) Map `PostMatch` to `("PostMatch", false)` — treated as unknown phase, no final evaluation fires. If `FullTimeNormalTime` was already received, tombstone prevents re-eval regardless. If it was NOT received, `PostMatch` is ignored, and no bets fire (fail-closed).
- (b) Before committing to (a), capture a live UCL knockout match to confirm the V2 phase sequence for extra-time matches. If `FullTimeNormalTime` is guaranteed before `PostMatch`, document it and keep the current code.

**Rationale:** This is the exact "Interrupted" pattern. Accepting a phase we haven't confirmed is the correct match-end signal for Polymarket's full-time scope risks firing bets on the wrong score. The safe default is to accept ONLY the narrowest confirmed signal.

### P0 — Same treatment for BoltOdds `MATCH_COMPLETED`

**File:** [boltodds_frame_pipeline.rs:55](native/polybot2_native/src/boltodds_frame_pipeline.rs:55)

**Action:** Remove `MATCH_COMPLETED` from the match-end arm, keeping only `AT_FULL_TIME`. Same reasoning as above. Currently latent (EPL only), but should be fixed before BoltOdds is used for any knockout competition.

### P1 — Reject V1 frames with missing score fields

**File:** [fast_extract.rs:239–246](native/polybot2_native/src/fast_extract.rs:239)

**Action:** Require `fixture_id`, `home_score`, AND `away_score` to all be present. If any are missing, return `None` (reject the frame). Specifically:
```rust
Some(V1Extract {
    fixture_id: fixture_id?,
    home_score: home_score?,  // was unwrap_or("")
    away_score: away_score?,  // was unwrap_or("")
    free_text: free_text.unwrap_or(""),  // freeText can legitimately be empty
    corners_home,
    corners_away,
})
```

`free_text` can stay `unwrap_or("")` since some V1 frames legitimately have no `freeText` during prematch→live transitions. But scores should never be missing on a `"type":"next"` frame with a valid `fixtureId`.

### P2 — Use `Option`-aware delta computation for NRFI

**File:** [baseball/engine.rs:473–474](native/polybot2_native/src/baseball/engine.rs:473)

**Action:** Compute delta only when both current and previous scores are `Some`:
```rust
let goal_delta_home = match (goals_home, row.goals_home) {
    (Some(curr), Some(prev)) => curr - prev,
    _ => 0,
};
```

This preserves `delta = 0` semantics when either is unknown (no NRFI fires), but is explicit about the "unknown" case rather than silently treating `None` as `0`.

### P3 — Add `game_leagues.push` to test helper `add_game`

**File:** [baseball/engine.rs:1083–1118](native/polybot2_native/src/baseball/engine.rs:1083)

**Action:** Add `self.game_leagues.push(Arc::from(""));` (or parameterize it) to keep the test helper in sync with `load_plan_from_json`. Prevents silent empty-league logging in future tests.

### P3 — Tighten set totals label matching

**File:** [compiler.py:194–196](src/polybot2/hotpath/compiler.py:194)

**Action:** Use `label.startswith("over")` / `label.startswith("under")` instead of `"over" in label` / `"under" in label`. Or better, split on space and check `label.split()[0].lower() == "over"`. Aligns with the stricter exact-match pattern used for regular totals.
