# PandaScore CS2 Hotpath Design

## Overview

PandaScore's Low Latency Feed delivers CS2 match data within ≤5 seconds of the game server — roughly 55 seconds faster than V1's maps-won counter for map-decided detection. This document describes how to integrate the feed into the polybot2 hotpath for live CS2 trading on Polymarket.

---

## Data Source

### Low Latency Feed (primary)

**Endpoint:** `wss://live.pandascore.co/matches/{id}/low_latency_feed?token=...`

**Discovery:** `GET /low_latency_feeds?token=...` — returns matches with active or upcoming (within 15min) WebSocket streams.

**Connection handshake:**
```json
{"type": "hello", "payload": {"status": "running", "match_id": "1394934", "videogame": "csgo"}}
```

**Frame format (pushed on every round state change):**
```json
{
  "updated_at": "2026-06-12T15:16:58.675308Z",
  "game_id": 219829,
  "match_id": 1513132,
  "match_status": "running",
  "game_status": "running",
  "home_team": {"team_id": 3216, "team_name": "Natus Vincere", "score": 12},
  "away_team": {"team_id": 133708, "team_name": "Legacy", "score": 4},
  "current_round": {
    "round_number": 17,
    "timer": 82,
    "bomb": {"status": "planted"},
    "home_team": {"team_id": 3216, "side": "terrorists", "players": [...]},
    "away_team": {"team_id": 133708, "side": "counter_terrorists", "players": [...]}
  },
  "map_id": 2
}
```

**Key fields:**
- `home_team.score` / `away_team.score` — round scores for the current map
- `game_id` — identifies which map (changes when a new map starts)
- `game_status` — `"running"`, `"finished"`, `"not_started"`
- `match_status` — `"running"`, `"finished"`

**What the feed does NOT provide:**
- Maps-won count (no match-level score). Must be tracked internally.
- Previous maps' round scores. Only the current game is shown.
- Explicit forfeit flag (available on REST, not in WS frames).

### REST API (startup only)

**Endpoint:** `GET /low_latency_feeds?token=...`

Used once at startup to get the current match state before the first WebSocket frame:
- `results: [{team_id: X, score: 1}, {team_id: Y, score: 0}]` — maps-won count
- `games[]: [{status, position, winner, forfeit}]` — per-map status and results
- `number_of_games` — BO format (1, 3, or 5)
- `opponents[]` — team names and IDs for mapping to Polymarket tokens

### Standard Frames Feed (validation, not latency-critical)

**Endpoint:** `wss://live.pandascore.co/matches/{id}?token=...`

Snapshots every ~2s. Each frame contains per-team `score` (maps won in the series) and `round_score` (rounds in current map). Used as a safety net to confirm internal maps-won tracking. Not on the critical trading path.

---

## Map Winner Detection

### Primary signal: score-reset detection (~55s faster than V1)

The low-latency feed resets `home_team.score` / `away_team.score` from the winning score to `(0, 0)` at the moment the map-deciding round resolves. This is the fastest signal — it arrives before V1's maps-won counter update and before PandaScore's own `game_status: "finished"` frame.

**Detection logic:**

Track `prev_score` per game. When `current_score == (0, 0)` and `prev_score != (0, 0)`:

1. Check if the previous score is **already** a decided state (LL sometimes shows the final score before resetting):
   - `map_winner(prev_home, prev_away)` → if true, the higher score wins
2. If not, check if it was **one round away** from decided (LL resets from the pre-decided score):
   - `map_winner(prev_home + 1, prev_away)` → if true, home won
   - `map_winner(prev_home, prev_away + 1)` → if true, away won
3. If none returns true → anomalous reset (forfeit, data glitch) → don't fire child_moneyline
4. If one returns true → fire child_moneyline + update internal maps-won counter

**Two observed patterns (both must be handled):**
- Aurora vs Spirit Map 1: `12-9 → 0-0` (pre-decided reset, step 2 catches it)
- Vitality vs 9z Map 1: `9-13 → 0-0` (decided score shown ~1.7s before reset, step 1 catches it)
- Navi vs Legacy Map 1: `12-4 → 0-0` (pre-decided reset, step 2 catches it)

**Why this is safe:**
- In any given round, only one team can possibly win the map (never both)
- `12-12` goes to overtime (scores keep climbing), never resets
- The proven `map_winner()` condition handles regulation, all OT sets, and rejects impossible scores
- Anomalous resets (forfeit at `2-4`) are caught by the guard

### Secondary signal: `game_status` transition

The low-latency feed eventually sends `game_status: "finished"` with the final round score (e.g., `13-4`). This arrives ~70 seconds after the score-reset. Used as confirmation, not for trading.

### Tertiary signal: `game_id` change

When `game_id` changes between frames, a new map started. The previous map is complete. This is another confirmation signal.

---

## Maps-Won Tracking

The low-latency feed has no match-level score. Maps-won must be tracked internally.

### At startup

One REST call to `GET /low_latency_feeds` provides:
- `results[].score` — maps won per team
- `games[].status` / `games[].winner` — which maps are complete and who won

Initialize internal `maps_home` / `maps_away` from this.

### During the match

When a score-reset is detected (map decided):
1. Determine the winner from `prev_score` + `map_winner()` guard
2. Increment `maps_home` or `maps_away`
3. Use the updated count for match-level evaluators (moneyline, totals, map handicap)

### Validation

Optionally connect to the standard frames feed. Its per-team `score` field = maps won. Periodically compare against internal tracking. Log a warning on mismatch.

---

## Effective Maps Optimization

Same principle as the V1 hotpath: when we detect map-decided via score-reset, immediately compute effective maps and fire match-end evaluators (moneyline, totals under, map handicap covers) if the match is decided.

With the ~55-second edge over V1, this fires match-end bets a full minute before V1-based bots.

---

## Match Format (BO1/BO3/BO5)

`number_of_games` is available from:
- The REST discovery endpoint (`/low_latency_feeds` response)
- The catalog in `provider_games.extra_json` (synced by `polybot2 provider sync --provider pandascore`)
- The Polymarket event title `(BON)` parser (existing compiler logic)

The Rust engine's `maps_to_win` is set from this at plan compile time (same as today).

---

## Team ID Mapping

PandaScore uses numeric `team_id` values (e.g., `3216` for Natus Vincere). The compiled plan needs to map these to Polymarket tokens.

The mapping chain:
1. `polybot2 provider sync --provider pandascore` → stores team names in `provider_games.home_raw` / `away_raw`
2. `polybot2 link build` → matches PandaScore team names to Polymarket events via `TEAM_MAP_CS2` aliases
3. The compiled plan assigns `provider_game_id` (PandaScore match ID) + canonical home/away
4. At runtime, the low-latency feed's `home_team.team_id` / `away_team.team_id` are matched to the plan's home/away by the first frame (or the hello payload)

**Important:** PandaScore's home/away assignment is consistent within a match. `opponents[0]` = home throughout. But it may differ from V1's home/away for the same match. The linker handles this via team name matching, not position.

---

## Forfeit Detection

The score-reset guard handles this naturally:
- Normal completion: `prev=(12, 4)`, `map_winner(13, 4) = true` → fire
- Forfeit: `prev=(2, 4)`, `map_winner(3, 4) = false`, `map_winner(2, 5) = false` → don't fire
- No need for the V1 phases-array approach — the guard is simpler and the signal is faster

For match-level markets (moneyline, totals, handicap), forfeited maps still count in the maps-won tally. Only child_moneyline is suppressed.

The REST endpoint also provides explicit `forfeit: true/false` per game, which can be checked for validation.

---

## Latency Summary (empirical, 3 matches on 2026-06-12)

### Map-decided detection: LL score-reset vs V1 maps counter

| Match | Map | LL prev score | LL reset ts | V1 maps update ts | **LL edge** |
|---|---|---|---|---|---|
| Navi vs Legacy | 1 | 12-4 → 0-0 | baseline | +55.7s | **55.7s** |
| Aurora vs Spirit | 1 | 12-9 → 0-0 | baseline | +57.4s | **57.4s** |
| Aurora vs Spirit | 2 | 4-12 → 0-0 | baseline | +52.1s | **52.1s** |
| Vitality vs 9z | 1 | 9-13 → 0-0 | baseline | +65.3s | **65.3s** |

**Consistent 52-65 second edge** across 4 map completions in 3 different matches.

### Round-level updates: LL vs V1

~600-1200ms faster for LL on most transitions. V1 occasionally wins by ~600ms. Marginal difference — the real value is map-decided detection.

### LL vs Standard Frames Feed

Standard frames are ~45-67 seconds slower than LL for everything. Only useful for the maps-won counter (`score` field), not for trading signals.

### Notes

- V1 exhibited Behavior B (skipped round 13) on the Navi game. PandaScore LL always delivers score data.
- LL shows two different reset patterns: pre-decided reset (12-9 → 0-0) and decided-then-reset (9-13 → 0-0). Both handled by the detection logic.

---

## Open Questions

1. ~~**Is the 55-second edge consistent across matches?**~~ **Yes — 52-65s across 4 map completions in 3 matches.**
2. **How does the score-reset behave in overtime?** Need to observe an OT game on the LL feed.
3. **What happens when PandaScore data is unavailable for a match?** Only LL-supported matches are synced (default). Fall back to V1 for non-LL games.
4. **Connection limits:** 3 simultaneous connections per match per endpoint. Sufficient for one hotpath process.
5. **Cost-benefit:** Is the latency edge enough to consistently beat other market participants to the best prices?
6. **Score-reset inconsistency:** LL sometimes shows the decided score before resetting (9-13 → 0-0), sometimes resets from pre-decided (12-9 → 0-0). Detection handles both, but need to verify no other patterns exist.
