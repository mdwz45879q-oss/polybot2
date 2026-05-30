# CS2 Esports Hotpath Design

## Overview

CS2 (Counter-Strike 2) is the first esports title for the polybot2 hotpath.
The evaluation pipeline uses Kalstrop V1 as the primary score data provider.
BoltOdds livescores may be added later as a multiplexed backup for coverage
redundancy (latency analysis shows V1 is 3-16s faster for map-completion
signals).

---

## Match Format

CS2 matches are played as a series of **maps** (games). Each map consists of
**rounds** (up to 24 in regulation, plus overtime if tied at 12-12).

| Format | Maps to win | Max maps | Common in | PM count |
|--------|-------------|----------|-----------|----------|
| **BO3** | 2 | 3 | Most tournaments | ~430 |
| **BO1** | 1 | 1 | Group stages | ~107 |
| **BO5** | 3 | 5 | Grand finals | ~2 |

### Determining `maps_to_win` per game

The BO format is **not available from Kalstrop V1** — the provider catalog
only has team names and tournament name, neither of which specifies BO1/3/5.

The BO format **is available from Polymarket** — the event title contains it
in parentheses, e.g.:
- `"Counter-Strike: magic vs NIP (BO3) - Stake Ranked Episode 2 Playoffs"`
- `"Counter-Strike: 9daplug vs Alzon (BO1) - Gamers Club Liga Série A Group C"`
- `"Counter-Strike: M80 vs Voca (BO5) - ESL Challenger League NA Finals"`

The compiler must parse `maps_to_win` from the PM event title at plan
compile time. **If the BO format cannot be parsed, the game must be excluded
from the plan — no silent default.** A wrong `maps_to_win` would cause
irrecoverable incorrect bets (e.g., treating a BO5 as BO3 fires match
moneyline after 2 map wins when the match isn't over).

Parsing rule: extract `(BO<N>)` from the event title or question string,
derive `maps_to_win = ceil(N / 2)`. If not found, log a warning and skip
the game.

**Map round structure (MR12 format):**

Each map is played under the MR12 (Max Rounds 12) format:

**Regulation (up to 24 rounds):**
- Two halves of up to 12 rounds each. Teams switch sides (CT/T) at halftime
  after round 12.
- A team wins the map by reaching **13 rounds** — at that point the other
  team cannot tie or overtake, so the map ends immediately.
- Examples: **13-7** (ended after round 20), **13-11** (ended after round 24,
  the maximum regulation rounds).

**Overtime (if regulation ends 12-12):**
- Overtime uses the **MR3** format: 3 rounds per side, 6 overtime rounds
  total per OT set.
- A team must win **4 overtime rounds** within the OT set to win the map.
  The first team to reach 16 rounds total (12 regulation + 4 OT) wins.
- Examples: **16-12** (clean OT sweep), **16-14** (close OT).
- If the OT set ties at **15-15** (3-3 in OT), another OT set is played.
  This repeats until one team wins an OT set outright.
- In subsequent OT sets, the round counts continue accumulating: a second
  OT could end at **19-17**, a third at **22-20**, etc.

**Map completion signal:** A map ends when one team reaches 13 rounds in
regulation, or wins an overtime set after a 12-12 tie. Formally: the map
ends when `max(home_rounds, away_rounds) >= 13` AND the leading team's
round count exceeds the trailing team's by a sufficient margin given the
OT structure. In practice, for evaluation purposes:
- **Regulation:** `home_rounds >= 13 OR away_rounds >= 13` → map decided.
- **Overtime:** The round scores continue climbing beyond 12-12. The map
  ends when one team's rounds are high enough that the other cannot catch
  up within the current OT set. The simplest check: after round 24
  (regulation exhausted), the map is decided when the score difference
  reaches 2 at an OT set boundary (i.e., total rounds played is a multiple
  of 6 beyond 24, and one team leads).

For the hotpath, the **practical detection** is: fire the map winner intent
when the round score for either team reaches 13+ AND exceeds the opponent's
by at least 1 (to handle OT correctly — a team at 16 with the opponent at
14 has won, but 15-15 is a tie requiring another OT set).

---

## Target Market Types

Four market types are targeted, using the concrete example of the CS2 game
**"NIP vs magic"** with event slug `cs2-mgc-nip-2026-05-29`.

### 1. `moneyline` — Match Winner

The overall match winner. Resolves when one team wins enough maps (2 in BO3,
3 in BO5).

**Example:**
- Slug: `cs2-mgc-nip-2026-05-29`
- Question: `"Counter-Strike: magic vs NIP (BO3) - Stake Ranked Episode 2 Playoffs"`
- Outcomes: `index 0` = `magic`, `index 1` = `NIP`

**Outcome label ordering — INCONSISTENT.** Outcome index 0 is usually the
home team (first in the event slug), but not always. Empirical analysis of
17 CS2 moneyline markets matched against V1 home/away data:

- 16 of 17: outcome index 0 = V1 home team ✓
- 1 of 17: outcome index 0 = V1 **away** team ✗
  (`cs2-yn-pha-2026-05-29`: PM puts Young Ninjas at index 0, but V1 says
  Phantom Esports is home and Young Ninjas is away)

The compiler **must resolve semantics from the outcome labels** (matching
team names against the canonical home/away from the provider game), not
from outcome index position. Hardcoding `index 0 = home, index 1 = away`
will produce incorrect bets on events with flipped ordering.

**Resolution logic:** Fire the winning team's outcome when maps-won reaches
the `maps_to_win` threshold (2 for BO3, 3 for BO5).

### 2. `child_moneyline` — Individual Map Winner

The winner of a specific map (map 1, map 2, etc.). Each map has its own
market.

**Example (Map 1):**
- Slug: `cs2-mgc-nip-2026-05-29-game1`
- Question: `"Counter-Strike: magic vs NIP - Map 1 Winner"`
- Outcomes: same label ordering as the match `moneyline` for this event

**Example (Map 2):**
- Slug: `cs2-mgc-nip-2026-05-29-game2`
- Question: `"Counter-Strike: magic vs NIP - Map 2 Winner"`

**Map 3 in BO3:** Polymarket does NOT list a separate map 3 winner market
for BO3 matches. If the match goes to map 3 (score 1-1), the map 3 winner
IS the match winner — so the `moneyline` market covers it. For BO5 matches,
map 3, 4, and 5 winner markets may exist.

**Outcome label ordering:** Same inconsistency as `moneyline` — the compiler
must match team names from labels, not rely on outcome index.

**Resolution logic:** Fire the winning team's outcome when that map's round
score reaches 13 (or the OT winner is determined). The map number is
extracted from the market slug suffix (`-game1`, `-game2`, etc.).

### 3. `totals` — Total Maps Played

Over/under on the total number of maps played in the match.

**Example:**
- Slug: `cs2-mgc-nip-2026-05-29-total-games-2pt5`
- Question: `"Games Total: O/U 2.5"`
- Line: `2.5`
- Outcomes: `index 0` = Over, `index 1` = Under

**Resolution logic:**
- **Over 2.5:** The match goes to a 3rd map (score reaches 1-1 before a
  team wins). Since the minimum maps for a deciding map to start is 2, and
  a 3rd map means at least 3 maps played, over 2.5 fires when both teams
  have won at least 1 map. In BO3, this is equivalent to the score reaching
  1-1.
- **Under 2.5:** One team wins the first 2 maps (2-0). Under fires at match
  completion when total maps = 2.

For BO5, additional lines may exist (e.g., 3.5, 4.5). The logic generalizes:
over fires progressively when the total maps played crosses the line, under
fires at match completion.

**Outcome labels:** "Over" / "Under" — consistent, no home/away ambiguity.

### 4. `map_handicap` — Map Spread

Similar to baseball spreads or tennis set handicap. The handicap is on the
number of maps won.

**Example (map-handicap-home):**
- Slug: `cs2-mgc-nip-2026-05-29-map-handicap-home-1pt5`
- Question: `"Map Handicap: NIP (-1.5) vs magic (+1.5)"`
- Line: `-1.5`
- Outcomes: index 0 = `NIP` (favored), index 1 = `magic` (underdog)

**Example (map-handicap-away):**
- Slug: `cs2-mgc-nip-2026-05-29-map-handicap-away-1pt5`
- Question: `"Map Handicap: MGC (-1.5) vs NIP (+1.5)"`
- Line: `-1.5`
- Outcomes: index 0 = `magic` (favored), index 1 = `NIP` (underdog)

**Handicap slug naming — INVERTED from intuitive reading.** In this game,
V1 says Magic = home, NIP = away. But:

- `map-handicap-home-1pt5` has **NIP** (the away team) as favored (-1.5)
- `map-handicap-away-1pt5` has **Magic** (the home team) as favored (-1.5)

The `home`/`away` in the handicap slug refers to **which team gets the
positive spread (+1.5)**, i.e., the underdog side — NOT which team is
favored. Verified across multiple games:

| Handicap slug | Who gets +1.5 (underdog) | Who gets -1.5 (favored) |
|---------------|--------------------------|-------------------------|
| `map-handicap-home-1pt5` | Home team (1st in event slug) | Away team (2nd in event slug) |
| `map-handicap-away-1pt5` | Away team (2nd in event slug) | Home team (1st in event slug) |

**The `home`/`away` in the handicap slug refers to Polymarket's home/away
(slug position), NOT V1's home/away.** PM and V1 can disagree on which
team is home (observed in `cs2-yn-pha-2026-05-29` where PM considers
Young Ninjas home but V1 considers Phantom Esports home). The compiler
MUST NOT use the slug's `home`/`away` to determine spread sides.

**Compiler resolution for `map_handicap` — dual-method with cross-validation:**

1. **Match outcome labels to canonical team names** (from V1 via the
   linker) to determine which outcome token corresponds to which team.
2. **Parse the question text** to extract which team has the negative
   spread. The question always follows the format `"Map Handicap:
   TEAM_A (-1.5) vs TEAM_B (+1.5)"` — the team with `(-1.5)` is the
   favored team (covers if they sweep).
3. **Cross-validate:** outcome index 0's label should match the team
   with `(-1.5)` in the question. If they disagree, log an error and
   **skip the market** — do not guess.

**Outcome label ordering:** The team with the negative spread (-1.5) is
always at outcome index 0. The team with the positive spread (+1.5) is
always at outcome index 1. However, team name format varies across markets
within the same event (e.g., `"magic"` vs `"MGC"` for the same team), so
label matching must be fuzzy (case-insensitive, substring-tolerant).

**Resolution logic for BO3 with ±1.5 line:**
- A team covers -1.5 → that team wins 2-0 (sweeps). `margin = 2 - 0 = 2`,
  `2 + (-1.5) = 0.5 > 0` → covers.
- A team doesn't cover -1.5 → match goes to 3 maps or the other team
  sweeps.
- Resolves at match completion (all maps played).

For BO5 with ±2.5 line: team must win 3-0 to cover -2.5.

---

## Outcome Semantic Resolution Strategy

Because outcome label ordering (home vs away at index 0 vs 1) is
inconsistent across CS2 events, the compiler **must** resolve semantics by
matching outcome labels against canonical team names, not by outcome index.

**Approach (same as baseball moneyline resolution):**

1. Extract canonical home and away team names from the provider game record.
2. For each outcome token, check if the outcome label (or a normalized
   version) matches the home team or away team.
3. Assign `home` / `away` semantic based on the label match.
4. For handicap markets, additionally use the slug suffix (`-home-1pt5` /
   `-away-1pt5`) to determine which side is favored.

**For totals:** Outcome labels are "Over" / "Under" — resolve by label text,
same as baseball/soccer totals.

**For child_moneyline:** Same team-name matching as match moneyline. The map
number is parsed from the slug suffix.

---

## Kalstrop V1 Frame Structure

V1 delivers CS2 match state via the `sportsMatchStateUpdatedV2` GraphQL
subscription, same as baseball/soccer/tennis. The frame structure for CS2:

```
matchSummary:
  homeScore: "1"          ← maps won by home
  awayScore: "0"          ← maps won by away
  matchStatusDisplay:
    [0].freeText: "2nd map"   ← current map label, or "Closed" at match end
  currentPhase:
    phase: 2              ← current map number (1-based)
    homeScore: "5"        ← rounds won by home in current map
    awayScore: "3"        ← rounds won by away in current map
  phases:                 ← array of completed + current map phases
    [0]: {phase: 1, homeScore: "13", awayScore: "3"}   ← map 1 final
    [1]: {phase: 2, homeScore: "5", awayScore: "3"}     ← map 2 in progress
```

**Key fields for evaluation:**
- `homeScore` / `awayScore` (top level): maps won — increments when a map
  completes
- `currentPhase.homeScore` / `currentPhase.awayScore`: rounds in the current
  map — use to detect round 13 (map completion)
- `freeText`: `"1st map"`, `"2nd map"`, `"3rd map"`, `"Closed"` — map
  label and match end signal
- `phases[]`: array of all map phases — includes completed maps' final
  round scores and the current map's live score

### Map Completion Detection — Three Signals

There are three places in the V1 frame where map completion can be detected.
Empirical analysis of 10 map completions across 4 CS2 matches reveals two
distinct behaviors:

**Behavior A (7 of 10 maps): `currentPhase` shows round-13 before switching**

In most cases, `currentPhase` reports the winning round score (e.g.,
`homeScore: "13"`) and holds it for 29-104 seconds before the maps-won
counter increments and `currentPhase` switches to the next map.

```
Frame N:   maps=0-0  cp.phase=1  cp=4-12       ← map 1 in progress
Frame N+1: maps=0-0  cp.phase=1  cp=4-13       ← round-13! map 1 decided
  ... 30-100 seconds pass ...
Frame N+X: maps=0-1  cp.phase=2  cp=0-0        ← maps-won updated, next map
           phases=[p1: 4-13, p2: 0-0]
```

This is the **fastest signal** — fires 30-100 seconds before `maps` updates.

**Behavior B (3 of 10 maps, all from nemesis_vs_tdk): `currentPhase` skips past 13**

In some cases, V1 skips the round-13 frame entirely. `currentPhase` jumps
from a pre-completion score (e.g., `12-4`) directly to `0-0` on the next
map, with `maps` updating simultaneously:

```
Frame N:   maps=0-1  cp.phase=2  cp=12-4        ← map 2 in progress
Frame N+1: maps=1-1  cp.phase=3  cp=0-0          ← SKIPPED round-13!
           phases=[p1: 7-13, p2: 12-4]            ← phases lag (not yet 13)
Frame N+2: maps=1-1  cp.phase=3  cp=0-0
           phases=[p1: 7-13, p2: 13-4]            ← phases catch up 1 frame later
```

In this case, `currentPhase` never showed `13-4` — and the `phases` array
itself lags by one frame (shows `12-4` on the transition frame, catches up
to `13-4` on the next frame).

**Empirical results:**

| Game | Maps | Round-13 on `currentPhase`? | Edge over maps-won |
|------|------|-----------------------------|---------------------|
| bestia_vs_galorys | 2, 3 | ✅ YES | 31-37s |
| magic_vs_faze | 1, 2, 3 | ✅ YES | 29-35s |
| nemesis_vs_tdk | 2, 3, 4 | ❌ SKIPPED | 0s (simultaneous) |
| ww_team_vs_ex_ruby | 2, 3 | ✅ YES | 71-104s |

**Detection strategy — use all three signals, whichever fires first:**

1. **Primary: `currentPhase` round score reaches 13+** (or OT win condition).
   Fastest signal when available (70% of observed cases). Fires 30-100s
   before maps-won updates. The evaluator checks `currentPhase.homeScore`
   and `currentPhase.awayScore` on every tick.

2. **Fallback: `maps` (homeScore/awayScore) increments.** Catches the case
   where `currentPhase` skipped past 13 (Behavior B). Always fires
   eventually. The evaluator detects a maps-won change by comparing against
   the previous tick's maps-won values.

3. **Tertiary: `phases[]` array entry reaches 13+.** Arrives 1 frame after
   `maps` changes in Behavior B. Redundant with signal 2 but confirms the
   final round score. Useful for logging and verification.

The presign pool's one-shot gate (`Option::take()`) prevents double-firing
when multiple signals trigger for the same map completion — same pattern as
baseball's walkoff + game-end evaluators.

**Match end detection:**
`freeText = "Closed"` signals match completion. This arrives after the final
map's round-13 signal (or simultaneously with the maps-won update if
`currentPhase` skipped). Used as the definitive match-end confirmation for
`moneyline`, `totals` under, and `map_handicap` resolution.

---

## BoltOdds Livescores Frame Structure (Reference)

BoltOdds delivers CS2 data via the livescores WebSocket with `action:
"new_play"`. The frame structure:

```json
{
  "action": "new_play",
  "event": "Eternal Fire vs Fnatic, 2026-05-27, 01",
  "league": "CS2",
  "play_info": {
    "currentMap": 2,
    "currentRound": 17,
    "teams": {
      "home": {"currentSide": "CT", "score": 1},
      "away": {"currentSide": "T", "score": 1}
    },
    "maps": [
      {
        "number": 1,
        "map": "DE_ANCIENT",
        "home": {"score": 7},
        "away": {"score": 13},
        "winner": "AWAY"
      },
      {
        "number": 2,
        "map": "DE_DUST2",
        "home": {"score": 13},
        "away": {"score": 4},
        "winner": "HOME"
      }
    ]
  }
}
```

**Key fields:**
- `teams.home.score` / `teams.away.score`: maps won
- `maps[].home.score` / `maps[].away.score`: per-map round scores
- `maps[].winner`: `"HOME"` or `"AWAY"` — explicit map winner (only present
  after map completion)
- `currentMap`: current map number
- `currentRound`: current round number within the map

**BoltOdds availability note:** Esports games require the correct game labels
from `/api/playbyplay/esports` (not from the standard `/api/get_games`
catalog). Standard labels return `Code 1` errors. The provider sync already
handles this — see `boltodds.py` `_ESPORTS_SPORTS` and
`_fetch_esports_pbp_labels()`.

---

## Latency Analysis Summary

Based on captured data from 4 CS2 matches (May 2026):

| Signal | V1 | BoltOdds LS | Delta |
|--------|-----|------------|-------|
| Round-13 (map end) | 3 wins | 0 wins | V1 faster by 3.5-16.5s |
| Map score change | Mixed | Mixed | V1 30s faster via round-13 |

**Conclusion:** V1's `currentPhase` round scores provide the earliest map
completion signal. BoltOdds is useful for coverage redundancy (V1 missed some
maps in captures) but is not faster.

---

## Provider Architecture

### Launch: V1 only

The CS2 hotpath launches with **Kalstrop V1 as the sole provider**. V1 is
faster for map completion signals (3.5-16.5s edge over BoltOdds at the
round-13 level), and the existing V1 WS infrastructure (`ws.rs`,
`SportEngine` dispatch) supports CS2 without additional connections.

BoltOdds multiplexing is deferred — not because it lacks value (coverage
redundancy for maps where V1 skips round-13), but because of a connection
limit constraint: BoltOdds limits WebSocket connections, and each hotpath
process currently opens its own connection. Adding CS2 as a third sport
(alongside baseball and soccer) would exceed the budget.

### Future: Shared BoltOdds proxy

When BoltOdds multiplexing is needed, a **shared Rust proxy** will replace
the per-process direct connections:

- A single Rust binary holds one BoltOdds WS connection, subscribes to
  game labels from all running hotpath processes.
- Hotpath processes connect to the proxy via local Unix domain sockets
  instead of to BoltOdds directly.
- The proxy routes frames by game label (`FxHashMap` lookup) and forwards
  raw bytes to the correct process — sub-microsecond overhead.
- A Python orchestrator manages lifecycle: starts the proxy, handles
  registration/unregistration as hotpath processes start and stop.
- The Rust frame pipeline code (`fast_extract_boltodds`,
  `process_boltodds_frame_sync`) stays completely unchanged — the frames
  arrive in the same format, just from a local socket instead of a WS.
- Hotpath processes can start and stop independently at any time; the proxy
  dynamically updates the BoltOdds subscription.
