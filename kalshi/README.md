# Kalshi Tennis Sniper

Single-file Python sniper that watches live tennis fixtures via Kalstrop V1 WebSocket and fires math-locked bets on Kalshi when set boundaries and match-end events resolve markets to YES or NO.

## How It Works

```
Kalstrop V1 WS (score feed) → TennisEngine (evaluators) → Kalshi REST API (batched orders)
```

1. **Discovery:** Enumerates live tennis fixtures from Kalstrop `/tournaments/tennis`, matches them against Kalshi's market registry using deterministic player mappings
2. **Score feed:** Subscribes to matched fixtures via Kalstrop V1 GraphQL WS (`sportsMatchStateUpdatedV2`)
3. **Engine:** Evaluates each score tick against all loaded markets — fires intents when a market outcome is mathematically certain
4. **Dispatch:** Batches intents into Kalshi orders (up to 20 per account per HTTP request), using pre-signed headers for zero-latency auth

## Files

| File | Purpose |
|------|---------|
| `tennis_sniper_v3.py` | Main sniper — discovery, engine, WS listener, order dispatch |
| `tennis_mappings.py` | `KALSHI_PLAYER_MAP` — deterministic player name registry (629 players) |
| `config.py` | Account config template (API key, RSA secret, bet sizing, per-type overrides) |
| `kalshi_tennis_matches.txt` | Dump of all Kalshi tennis matches (open + settled) — for mapping verification |
| `kalshi_tennis_players.txt` | Dump of unique Kalshi player names + 3-letter abbreviations |

## Usage

```bash
# Dry run (no orders placed)
python3 tennis_sniper_v3.py --config-files config.py --bet-size-usd 1 --limit-cents 99

# Live trading
python3 tennis_sniper_v3.py --live --config-files config.py --bet-size-usd 10 --limit-cents 99

# Override betting date (default: today ET)
python3 tennis_sniper_v3.py --config-files config.py --date 26JUN29

# Disable date filter entirely
python3 tennis_sniper_v3.py --config-files config.py --date 0
```

## Engine Evaluators

| Evaluator | When it fires | Market types |
|-----------|--------------|--------------|
| `MONEYLINE` | sets >= sets_to_win | `KXATPMATCH`, `KXWTAMATCH`, `KXATPCHALLENGERMATCH`, `KXWTACHALLENGERMATCH` |
| `FIRST_SET_WINNER` | set 1 completes | `KXATPSETWINNER`, `KXWTASETWINNER` (set_n=1) |
| `SET_N_WINNER` | set N completes (per-side increment detection) | `KXATPSETWINNER`, `KXWTASETWINNER` (set_n=2,3,4,5) |
| `MATCH_TOTAL_OVER` | total games crosses line | `KXATPGTOTAL` |
| `MATCH_TOTAL_UNDER` | match ends, total below line | `KXATPGTOTAL` (NO side) |
| `SET_TOTAL_OVER` | guaranteed-certainty mid-match | Derived from set counts |
| `SET_TOTAL_UNDER` | match ends, sets below line | Derived from set counts |
| `SET_HANDICAP` | match end + early not_covers mid-match | Derived from set margin |
| `EXACT_MATCH_YES` | match ends with exact score | `KXATPEXACTMATCH` |
| `EXACT_MATCH_NO` | score becomes mathematically impossible | `KXATPEXACTMATCH` (NO side) |
| `GAME_HANDICAP` | match end + early lock mid-match | `KXATPGSPREAD` |
| `COMPLETED_MATCH` | match decided | `KXATPSETSWEEP` |

## Retirement Handling

When a player retires (detected from V1 `freeText` patterns like `"Player 1 retired, Player 2 won"`):
- **Moneyline fires** for the opponent (the winner)
- **All other markets are skipped** — Kalshi resolves non-moneyline markets to "fair value" which is not predictable
- All tombstones are set to prevent subsequent ticks from firing anything

## Player Matching

Uses deterministic exact matching via `KALSHI_PLAYER_MAP` in `tennis_mappings.py`. No fuzzy matching.

**How it works:**
1. At startup, builds a name index from `KALSHI_PLAYER_MAP`: normalized player name → canonical key
2. V1 fixture names and Kalshi `yes_sub_title` names are both looked up in the index
3. A match requires BOTH sides to resolve to known canonical keys, and the key sets must be equal
4. If either side is unknown, the fixture is skipped (no match, no bets)

**Player map sources (629 entries):**
- 519 players: V1 aliases verified from `PLAYER_MAP_TENNIS` (Polymarket mappings, built progressively from daily V1 catalog checks)
- 40 players: V1 aliases verified from current V1 catalog snapshot
- 70 players: V1 aliases derived from Kalshi name ("First Last" → "Last, First"), marked with `# v1_alias_unconfirmed`

**Adding a new player:**
```python
# In kalshi/tennis_mappings.py:
"draper, jack": {
    "kalshi_name": "Jack Draper",
    "v1_aliases": ["Draper, Jack"],
},
```

## BO3/BO5 Detection

Deterministic via exact V1 competition name match. Default is BO3.

```python
_BO5_COMPETITIONS = {
    "french open men singles",
    "wimbledon men singles",
}
```

Australian Open and US Open entries will be added when their V1 competition names are confirmed.

## Account Config

Each account config file (`config.py`) supports:

| Field | Type | Purpose |
|-------|------|---------|
| `API_KEY` | str | Kalshi API key |
| `SECRET_KEY` | str | RSA private key (PEM format) |
| `BET_SIZE_DOLLARS` | float | Default bet size per order |
| `MAX_PRICE_CENTS` | int | Default max price (e.g., 99 = $0.99) |
| `ALLOWED_BET_TYPES` | list | Whitelist of market types to bet on (None = all) |
| `BET_SIZES_BY_TYPE` | dict | Per-market-type size overrides |
| `BET_PRICES_BY_TYPE` | dict | Per-market-type max price overrides |

Market type names in config are case-insensitive and match the engine's intent names (e.g., `"moneyline"`, `"exact_match_no"`, `"game_handicap_yes"`).

## Network Optimizations

- **TCP_NODELAY** on both Kalshi (order) and Kalstrop (score) connections — eliminates Nagle's 0-40ms buffering delay
- **Pre-signed Kalshi headers** — RSA signatures refreshed every 0.5s in background, 0ms auth on the hot path
- **Kalshi connection warmup** — `GET /exchange/status` at startup pre-establishes TCP + TLS
- **Batched orders** — up to 20 orders per HTTP request per account
- **Deterministic `client_order_id`** — same (account, ticker, side) always produces the same UUID, providing idempotency on retries
- **uvloop** event loop (falls back to stdlib if not installed)
- **orjson** for JSON parsing (falls back to stdlib if not installed)

## Safety Guards

- **`streamExists` gate:** If V1 reports `streamExists=false` 3 times consecutively, the fixture is skipped (Kalstrop is inferring scores from odds, not real data)
- **Doubles filter:** Fixtures with ` / ` in player names or "doubles" in competition are rejected
- **Cold-start tombstones:** When joining a fixture mid-match, end-of-match events are pre-resolved to prevent stale fires
- **`match_ended` early-exit:** Once a match ends (normal or retirement), all subsequent frames are skipped
- **One-shot intents:** `snipe_attempted` set per fixture + `BETS_PLACED` global set prevent duplicate fires

## Kalstrop Credentials

Currently hardcoded in lines 127-129. Should be moved to environment variables (`KALSTROP_CLIENT_ID`, `KALSTROP_SHARED_SECRET_RAW`) before production deployment.

## Key Differences from Polymarket Tennis Hotpath

| Aspect | Polymarket (Rust) | Kalshi (Python) |
|--------|-------------------|-----------------|
| Language | Rust via PyO3, zero-alloc hot path | Pure Python/asyncio |
| Set totals timing | Match-end only (Option A, retirement-safe) | Mid-match guaranteed-certainty OVER |
| Set handicap timing | Match-end only (Option A) | Early not_covers mid-match + match-end |
| Retirement | Full 50/50 retirement presign pool | Moneyline only, all others skipped |
| Set N winner | First-set winner only | Per-set winner for sets 1-5 |
| Exact match | Not implemented for tennis | Mid-match NO + match-end YES |
| Game handicap | Not implemented for tennis | Mid-match early lock |
| Order dispatch | Rust SPSC ring → submitter thread | Python aiohttp batched POST |
| One-shot gate | Presign pool `std::mem::take()` | `snipe_attempted` + `BETS_PLACED` sets |
| Plan compilation | DB-driven compiler with linking | Runtime Kalshi API discovery |
