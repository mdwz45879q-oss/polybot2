# Tennis Sniper Correctness Audit

**File:** `kalshi/tennis_sniper_v3.py`
**Date:** 2026-06-28 (updated after set_handicap removal, set_total/game_handicap match-end-only changes)
**Scope:** 8 audit dimensions — engine evaluator correctness, retirement handling, score parsing, matching correctness, order dispatch safety, BO3/BO5 correctness, discovery/subscription lifecycle, edge cases/race conditions

## Summary

The 6 active evaluators (moneyline, first_set_winner, set_n_winner, match_totals, exact_match, game_handicap) are **correct in direction** — no wrong-side bet bugs found. No scenario was identified where the engine fires YES when it should fire NO, or fires for the wrong player. Cold-start tombstones correctly prevent stale fires on all active evaluators.

**No CRITICAL findings.** One IMPORTANT finding remains open: I-2 (batch order result indexing assumption).

Three evaluators are dead code (no Kalshi ticker series maps to them): `_eval_first_set_totals`, `_eval_set_totals`, `_eval_completed_match`. They consume CPU cycles but cannot cause wrong bets today.

**Resolved findings:** C-1/C-2 (set_handicap tombstone gap — code deleted), I-1 (substring matching — replaced with exact lookup), I-3 (line conversion — verified correct), I-4 (cold-start first_set_winner — now fires at 1-0).

**Recent changes verified:**
1. Set handicap removal — eliminates the tombstone gap (former C-1/C-2). Clean deletion, no orphaned references.
2. Set total OVER moved to match-end only — eliminates retirement risk on mid-match guaranteed-certainty bets. OVER now fires alongside UNDER under the `decided` gate. `set_total_over_emitted` tracking correctly removed (no longer needed since the `set_total_under_emitted` tombstone gates both directions). **CORRECT.**
3. Game handicap match-end only — eliminates retirement risk on mid-match early-lock bets. Early `return` when `not decided`. The mid-match `margin ± max_opp_gain` bounds are gone. **CORRECT.**

---

## CRITICAL (must fix before deploying with real money)

*None.* The previously identified C-1/C-2 (missing `set_handicap_resolved` tombstone in cold-start and retirement blocks) were resolved by removing the dead set_handicap code entirely — `HandicapSlot`, `set_handicaps`, `set_handicap_resolved`, `_eval_set_handicap`, and the `add_game` handler are all deleted.

---

## IMPORTANT (should fix, risk of incorrect behavior or monetary loss)

### I-1: ~~`build_kalshi_registry` Stage 2 uses substring matching for ticker→event association~~ — RESOLVED

**Status:** Fixed. Stage 2 now splits the ticker by `"-"`, extracts `tk_parts[1]` as the match code, and uses `code_to_entry.get(tk_code)` for exact dict lookup (line 1118-1121). No more substring collision risk.

### I-2: Batch order result 1:1 indexing assumption

**Code location:** `tennis_sniper_v3.py:1413-1414`

```python
for i, ord_res in enumerate(order_results):
    order, bet_key, acct_key = to_place[i]
```

**Description:** The code indexes `to_place[i]` directly, assuming Kalshi's batch response returns results in the same order and count as submitted orders. If Kalshi reorders, deduplicates (via `client_order_id`), or omits results, the `to_place[i]` access either raises `IndexError` or misattributes results to wrong bet keys. The `client_order_id` response field is not used for correlation.

**Impact:** If Kalshi returns fewer results than submitted: crash (`IndexError`), caught by the outer `except` but all results marked as failed. If Kalshi reorders: wrong `acct_key` added to `account.bets_placed`, potentially blocking or allowing duplicate bets on wrong tickers.

**Suggested fix:** Correlate results by `ticker` from the response object rather than by positional index, or at minimum add a bounds check on `i < len(to_place)`.

### I-3: ~~`_eval_game_handicap` line conversion (`line = line_int - 0.5`) needs verification~~ — VERIFIED CORRECT

**Status:** Confirmed. Ticker `KXATPGSPREAD-26JUN29RUBSAF-RUB5` corresponds to "Andrey Rublev -4.5 games". The suffix `RUB5` → `line_int=5` → `line=4.5`. YES fires when `margin > 4.5` (Rublev leads by 5+ games), which matches the market semantics. Direction is correct.

### I-4: ~~Cold-start at 1-0 score unnecessarily tombstones `first_set_winner`~~ — RESOLVED

**Status:** Fixed. Line 591 now reads `if state.total_sets >= 2 or state.match_completed:` — at 1-0 cold-start (`total_sets=1`, not completed), the tombstone is skipped and the evaluator fires correctly. At 1-1+ or completed, the tombstone correctly prevents stale fires.

---

## MINOR (low risk, cleanup or missed optimization)

### M-1: Three evaluators are dead code

**Code location:** Called from `process_tick` at lines 606, 607, 610

The following evaluators are wired in `process_tick` but no Kalshi ticker series produces their market types:

| Evaluator | Market type | Why dead |
|-----------|-------------|----------|
| `_eval_first_set_totals` (line 631) | `first_set_total` | No ticker handler produces this type |
| `_eval_set_totals` (line 645) | `set_total` | No ticker handler produces this type |
| `_eval_completed_match` (line 688) | `completed_match` | `KXATPSETSWEEP` collected but falls through to unmatched |

**Impact:** Wasted CPU cycles per tick. `_eval_completed_match` fires YES for ALL decided matches (line 697), not just straight-set sweeps — if `KXATPSETSWEEP` were wired, it would fire wrong (YES on 2-1 matches when it should be NO).

*(Previously four dead evaluators — `_eval_set_handicap` was removed entirely.)*

### M-2: `KXATPSETSWEEP` tickers collected but not parsed

`build_kalshi_registry` Stage 2 discovers and assigns `KXATPSETSWEEP` tickers to events, but `build_game_from_fixture` has no handler for this series. The tickers are logged as "unmatched" and silently ignored. If the evaluator `_eval_completed_match` were ever activated, it would fire incorrectly (M-1 note above).

### M-3: Hardcoded Kalstrop credentials in source code

**Code location:** `tennis_sniper_v3.py:130-133`

Kalstrop client ID and shared secret are hardcoded in the source file. The README notes this should be moved to environment variables. These are read-only score feed credentials (not trading credentials), so the blast radius is limited.

---

## Evaluator-by-Evaluator Verification

### Active evaluators (6 — wired to live Kalshi tickers)

#### 1. `_eval_match_totals` (line 617) — KXATPGTOTAL

**OVER (line 619-623):** Fires when `prev_total_games <= half_int < total_games`. Correctly identifies all lines crossed in the delta interval. Cold-start safe: `prev_total_games` is `None` on first tick, so the `is not None` guard blocks stale fires. **CORRECT.**

**UNDER (line 624-629):** Fires at `match_completed` when `half_int >= total_games`. Tombstoned by `match_total_under_emitted` (set on first fire, and in cold-start block at line 593). **CORRECT.**

**Direction check:** OVER fires the `ol.target` which comes from `{"semantic": "over", "target": {"ticker": tk, "side": "yes"}}` (line 949). Buying YES on "Over N.5 games" when total crosses N.5 — correct. UNDER fires `ol.target` from `{"semantic": "under", "target": {"ticker": tk, "side": "no"}}` (line 950). Buying NO on "Over N.5 games" when total is under — correct.

#### 2. `_eval_first_set_winner` (line 660) — KXATPSETWINNER (set 1)

**Fires when:** `first_set_completed` becomes True AND `first_set_winner_resolved` is False.

**Side detection (line 665-670):** Uses `sets_home > sets_away`. At the moment `first_set_completed` first triggers, the score is 1-0 (one side has 1, other has 0). The side with 1 set won the first set. **CORRECT.**

**Cold-start:** Tombstoned at line 590 when `first_set_completed` is True on first tick. At 1-0 this is overly conservative (see I-4) but safe. At 1-1 or later, correctly prevents stale fire since we can't determine the set-1 winner from the current score. **SAFE.**

#### 3. `_eval_moneyline` (line 672) — KXATPMATCH/KXWTAMATCH

**Fires when:** `match_completed OR sets_home >= stw OR sets_away >= stw`.

**Side detection (line 681-686):** `sets_home > sets_away → home`, `sets_away > sets_home → away`. At match decided, the winner always has more sets. If sets are equal and `match_completed` is True (data anomaly), neither branch fires — fail-closed. **CORRECT.**

**Cold-start:** Tombstoned at line 595. **SAFE.**

#### 4. `_eval_set_n_winner` (line 702) — KXATPSETWINNER (set 2+)

**Per-side increment detection (line 713-716):**
```python
home_inc = state.sets_home > state.prev_sets_home
away_inc = state.sets_away > state.prev_sets_away
if home_inc == away_inc:
    return
```

If both increment (frame skip) or neither increments (no set change), returns without firing. If exactly one increments, that side is the winner. **CORRECT.**

**Cold-start:** No explicit tombstone needed — `prev_total_sets >= n` (line 722) effectively blocks sets completed before the first tick. When joining at 1-1, `prev_total_sets=2` on second tick, so sets 1 and 2 are both skipped. **CORRECT.**

**Direction check:** `target = slot_sn.home if winner == "home" else slot_sn.away` (line 725). Fires the winner's YES ticker. **CORRECT.**

**Edge case (0-2 → 1-2):** `home_inc=True, away_inc=False → winner="home"`. For set 3: `prev_total_sets=2 < 3, total_sets=3 >= 3` → fires set 3 for home. **CORRECT** — the old "leader = winner" heuristic would have mis-fired for away.

#### 5. `_eval_exact_match` (line 732) — KXATPEXACTMATCH

**Match-end YES (line 744-757):** Three-way comparison: `aw_side == winner_side AND aw_sets == winner_sets AND al_sets == loser_sets`. All three must match. **CORRECT.** If sets are tied at decided (impossible in tennis), `aw_side=None` → fires NO. **SAFE.**

**Mid-match NO (line 758-764):** `opp > loser_sets OR player > winner_sets`. Fires NO when the predicted scoreline is mathematically impossible. Example for "Home 2-0": if `away_sets=1`, then `opp=1 > loser_sets=0` → fire NO (can't finish 2-0 if away already has 1 set). **CORRECT.**

**Cold-start:** Tombstoned at line 599-600 for all exact_match slots. **SAFE.**

#### 6. `_eval_game_handicap` (line 766) — KXATPGSPREAD

**Match-end only (line 771-789):** Early `return` if `not decided` — no mid-match firing. When `decided`, `margin > line → YES, else → NO`. Margin computed correctly: `(games_home - games_away)` for home side, `(games_away - games_home)` for away. Line is `line_int - 0.5` (see I-3 for verification note). **CORRECT given the line interpretation.**

*(Previously had mid-match early-lock via `margin ± max_opp_gain` bounds. Removed to eliminate retirement risk — if a player retires after a mid-match early-lock bet, Kalshi resolves to "fair value" instead of the mathematically certain outcome.)*

**Cold-start:** Tombstoned at line 601-602. **SAFE.**

### Dead evaluators (3 — no Kalshi tickers produce their targets)

| Evaluator | Lines | Correctness | Notes |
|-----------|-------|-------------|-------|
| `_eval_first_set_totals` | 631-643 | Correct | Same crossing/under pattern as match_totals |
| `_eval_set_totals` | 645-658 | Correct | Match-end only OVER/UNDER under `decided` gate |
| `_eval_completed_match` | 688-700 | **Potentially wrong** | Always fires YES for all decided matches (line 697), not just sweeps. If `KXATPSETSWEEP` were wired, 2-1 matches would fire YES incorrectly |

*(Previously four dead evaluators — `_eval_set_handicap` was removed entirely.)*

---

## Retirement Handling Verification

**Detection (line 1267-1276):** Uses `in` (substring match) for both orderings: `"player 1 retired" in s and "player 2 won" in s` covers both `"player 1 retired, player 2 won"` and `"player 2 won, player 1 retired"`. Equivalent coverage to the Rust 4-string exact match. **CORRECT.**

**What fires on retirement (line 1641-1667):**
- Moneyline for the opponent (winner) — via the `retirement_winner` return value
- All other evaluators tombstoned to prevent firing
- `fx.match_ended = True` prevents all future frame processing

**What does NOT fire:** Set totals, match totals, exact match, game handicap, set-n winner, first-set winner — all correctly skipped via tombstones + `match_ended` gate. **CORRECT** per Kalshi's retirement resolution rules (non-moneyline markets resolve to "fair value").

**Moneyline fire ordering:** `slot.moneyline_resolved = True` is set BEFORE `fire_intents` — if the HTTP call fails and the retirement code re-runs on the next frame, the moneyline tombstone prevents double-fire. `fx.match_ended` is set AFTER the fire — a failure leaves `match_ended=False`, allowing retry, but the tombstone blocks duplicate moneyline. **SAFE.**

---

## Score Parsing Verification

**`parse_kalstrop_push` (line 1282-1328):**

- `sets_home`/`sets_away`: from `ms.homeScore`/`ms.awayScore` (top-level). Int conversion with fallback to 0. **CORRECT.**
- `total_games`/`home_games`/`away_games`: accumulated from `phases[]` (all historical sets including current). **CORRECT** — phases in V1 include the in-progress set.
- `first_set_games`: from `phases[0]` (phase with `n == 1`). Fixed after set 1 completes. **CORRECT.**
- `total_sets`: `sh + sa`. **CORRECT.**
- `match_completed`: `ft.lower() == "ended"`. Matches the V1 completion keyword. **CORRECT.**
- `first_set_completed`: `total_sets >= 1 or ft.lower() == "ended"`. **CORRECT.**
- `current_set`: `int(state["phase"] or 1)` in listener (line 1678). When `currentPhase` is null (match ended), defaults to 1. Harmless because `match_completed=True` controls evaluation flow, not `current_set`. **SAFE.**

---

## Matching Correctness Verification

**`_normalize_name` (line 1010-1015):** Strips accents, lowercases, replaces commas/periods/apostrophes/hyphens with spaces, collapses whitespace. Handles the V1→Kalshi name format difference ("Last, First" → "last first" vs "First Last" → "first last"). **CORRECT** for both orderings since the index stores both directions.

**`find_kalshi_event` (line 1055-1100):** Requires BOTH players to resolve to known canonical keys (line 1072-1074: `if v1_key_h is None or v1_key_a is None: return None`). If either side is unknown, the fixture is skipped entirely. **CORRECT** — no partial matches, no fuzzy fallbacks.

**Home/away alignment (line 1081-1092):** Cross-checks V1 home/away against Kalshi home/away via the canonical key pairs. If V1 has the players in opposite order from Kalshi, the code swaps home/away designations. **CORRECT.**

**Unconfirmed aliases:** 70 of 629 entries (11%) have `# v1_alias_unconfirmed`. These were derived from the Kalshi name format, not confirmed against V1 catalog. Risk: V1 might use a different transliteration (e.g., "Khachanov" vs "Hačanov"). Impact: fixture won't match (fail-closed) — missed opportunity, not a wrong bet.

---

## Order Dispatch Safety Verification

**Three-layer dedup:**
1. `fx.snipe_attempted` (line 1483-1485): per-fixture, keyed by `{ticker}_{side}`. Prevents duplicate intents for the same fixture.
2. `BETS_PLACED` (line 1460, 1472): global set, keyed by `{ticker}_{side}_{fixture_id[:8]}`. Prevents cross-fixture duplicates.
3. `account.bets_placed` (line 1374, 1427): per-account, keyed by `{ticker}_{side}`. Prevents per-account duplicates.

**Direction correctness (line 1346-1349):**
```python
if side == "yes":
    v2_side, p = "bid", price_c       # Buy YES = bid at price_c
else:
    v2_side, p = "ask", 100 - price_c  # Buy NO = sell YES at (100 - price_c)
```
For NO side with `price_c=99`: ask at 1 cent = buy NO at 99 cents. **CORRECT.**

**Quantity calculation (line 1380):**
```python
qty = max(1, int(size * 100 / actual))
```
`actual = min(price_c, max_p)`. For price_c=99, size=$10: qty = 10 * 100 / 99 ≈ 10 contracts at 99¢ each ≈ $9.90. **CORRECT.**

**Deterministic `client_order_id`:** UUID5 from namespace + `(account.name, ticker, side)`. Provides idempotency on retries — same inputs always produce the same order ID. **CORRECT.**

---

## BO3/BO5 Correctness Verification

**Detection (line 879-881):**
```python
_BO5_COMPETITIONS = {
    "french open men singles",
    "wimbledon men singles",
}
```

Default is BO3 (`sets_to_win=2`). BO5 only for exact competition name matches. Australian Open and US Open are noted as pending in the README. **CORRECT** for currently listed tournaments; will need updates when those Grand Slams are added.

**Flow-through:** `sets_to_win` is set in `build_game_from_fixture` (line 880) → `add_game` (line 584) → `GameSlot.sets_to_win`. All evaluators reference `slot.sets_to_win`. **CORRECT.**

**Exact match BO5 scores:** `build_game_from_fixture` generates exact match slots from ticker suffixes (e.g., `FRI30` → winner_sets=3, loser_sets=0). For BO5, valid scores are 3-0, 3-1, 3-2. The ticker parsing at line 975 (`ws, ls = int(sfx[3]), int(sfx[4])`) extracts single-digit set counts from the suffix, which handles all valid tennis scores. **CORRECT.**

---

## Discovery/Subscription Lifecycle Verification

**Discovery loop (line 1705-1748):** Runs every 90 seconds. Refreshes the Kalshi registry, matches new V1 fixtures, adds new subscriptions. Ended fixtures are tracked via `fx.match_ended` and skipped in the WS listener.

**Subscription management:** New fixtures are subscribed via GraphQL `sportsMatchStateUpdatedV2`. The subscription uses fixture IDs. Fixtures are NOT unsubscribed when they end — they stay subscribed but are skipped via the `match_ended` check (line 1627). Minor waste of bandwidth but functionally correct.

**GameSlot persistence across reconnections:** `ENGINE.games` persists across WS reconnections. Tombstones, resolution flags, and `is_first_tick` all survive reconnects. After reconnection and re-subscription, the first frame resumes from the current state without false fires. **CORRECT.**

---

## Edge Cases and Race Conditions

**Asyncio single-threaded:** All WS frame processing and order dispatch happen in the same asyncio event loop. No race conditions between frame processing and order firing. **SAFE.**

**Frame skip (V1 drops frames):** If V1 drops a frame and the score jumps (e.g., sets go from 0-0 to 1-1 in one tick), the per-side increment detection (`home_inc == away_inc → both True → return`) correctly avoids firing set_n_winner. Match totals OVER fires for all crossed lines in the delta. Moneyline waits for `decided`. **SAFE.**

**Score decrease (V1 correction):** If V1 sends a corrected score that is LOWER than the previous tick: `total_games > prev_total_games` is False → OVER doesn't fire. `total_sets > prev_total_sets` is False → set evaluators don't fire. Tombstones persist, preventing re-fire of already-resolved markets. **SAFE.**

**Concurrent retirement + match completion:** Retirement check (line 1641) runs BEFORE `process_tick` (line 1671). If retirement is detected, `continue` skips `process_tick` entirely. If `match_ended` is True (from retirement or normal completion), all subsequent frames are skipped (line 1627). No double-processing possible. **SAFE.**

**Pre-signed header staleness:** Headers refreshed every 0.5s (line 357). Max staleness is ~0.5s in normal operation, up to ~1.5s on signing failure + retry. Well within Kalshi's timestamp validation window (~5s). **SAFE.**

**WS backpressure:** The asyncio event loop processes frames sequentially. If order dispatch is slow (HTTP latency), frames queue in the asyncio buffer. No explicit backpressure mechanism, but frames are timestamped on arrival and processed in order. If the queue grows, intents fire late but correctly (tombstones prevent double-fire on catch-up). **SAFE.**

---

## Test Gaps

1. **No unit tests exist** for the `TennisEngine` evaluators. Given that this is financial code firing real money, each evaluator should have at minimum:
   - Happy path: correct fire direction at the expected trigger point
   - Cold-start: joining mid-match doesn't produce stale fires
   - Edge case: frame skip, score correction, boundary conditions
   - Retirement: tombstones prevent post-retirement fires

2. **No integration test** for the ticker parsing → engine loading → evaluation pipeline. A test that feeds a known set of tickers through `build_game_from_fixture` → `add_game` → `process_tick` with known V1 frames would catch semantic mismatches early.

3. **No test for the `line = line_int - 0.5` conversion** (I-3). A unit test comparing the engine's evaluation against the actual Kalshi market resolution for a known historical GSPREAD ticker would definitively verify the direction.

4. **No test for `detect_retirement`** with all known V1 freeText patterns. The current 4-pattern coverage is correct but untested.

5. **No test for `build_kalshi_registry` Stage 2** ticker assignment. A test with overlapping event codes would catch the substring matching bug (I-1).
