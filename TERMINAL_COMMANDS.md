# Polybot2 Terminal Commands (Practical Reference)

This is a copy-paste command guide for day-to-day `polybot2` work.

## 1) One-Time Setup

```bash
pip install -e /Users/reda/polymarket_bot/polybot2
```

Load environment variables: 

```bash 
set -a; source .env; set +a
```

If you prefer module execution without relying on the console script:

```bash
export PYTHONPATH=/Users/reda/polymarket_bot/polybot2/src
python -m polybot2 --help
```

Optional helper alias:

```bash
alias pb='PYTHONPATH=/Users/reda/polymarket_bot/polybot2/src python -m polybot2'
```

## 2) Environment Variables

```bash
export POLYBOT2_DB_PATH=/Users/reda/polymarket_bot/polybot2/data/prediction_markets.db
export KALSTROP_CLIENT_ID='YOUR_CLIENT_ID'
export KALSTROP_SHARED_SECRET_RAW='YOUR_SHARED_SECRET'
```

- `POLYBOT2_DB_PATH`: default DB path for all commands.
- Default provider is Kalstrop via policy (`config/live_trading.py`).
- Kalstrop creds are required unless you explicitly override `--provider boltodds`.

## 3) Core Data + Linking Flow

### Sync Polymarket metadata

```bash
polybot2 data sync --markets
```

What it does:
- Syncs events, markets, tokens, tags, and sports reference tables into SQLite.

### Sync provider games

```bash
polybot2 provider sync
```

What it does:
- Pulls provider catalog rows into `provider_games`.

### Validate config files

```bash
polybot2 mapping validate
```

What it does:
- Validates hardcoded config modules:
  - `/Users/reda/polymarket_bot/polybot2/config/mappings.py`
  - `/Users/reda/polymarket_bot/polybot2/config/live_trading.py`

### Build links

```bash
polybot2 link build --league-scope live
```

What it does:
- Builds deterministic provider-to-Polymarket links.
- Writes latest-state bindings and immutable per-run review snapshot rows.

### Show link run report

```bash
polybot2 link report
```

What it does:
- Prints high-level link quality summary for the latest run.

## 4) Link Review Commands

### 90% Workflow (Recommended)
One-game deep dive:

```bash
polybot2 link review card --run-id 123 --provider-game-id 2f8d1e1462ce
```

Record decision directly:

```bash
polybot2 link review decide --run-id 123 --provider-game-id 2f8d1e1462ce --decision approve --note "checked manually"
```

Interactive operator loop:

```bash
polybot2 link review session --run-id 123
```

What it does:
- Opens single-key review workflow over mapped pending cards by default.

### Advanced Diagnostics

Candidate-level comparison for one game:

```bash
polybot2 link review candidates --run-id 123 --provider-game-id 2f8d1e1462ce
```

## 5) Hotpath Launch (Scoped)

```bash
polybot2 hotpath run --league mlb --link-run-id 123
```

What it does:
- Runs league-scoped hotpath using compiled plan from the approved run.
- Fails fast if review gate/preflight requirements are not met.

Break-glass override:

```bash
polybot2 hotpath run --league mlb --link-run-id 123 --force-launch
```

Paper trading:
```bash
polybot2 hotpath run --league mlb --link-run-id 123  --execution-mode paper
```

Optional subscription intersection filter:

```bash
export POLYBOT2_SUBSCRIBE_UNIVERSAL_IDS='2f8d1e1462ce,36354015caf6'
polybot2 hotpath run --league mlb --link-run-id 123
```

## 6) One-Game Websocket Capture

```bash
polybot2 provider capture \
  --universal-id 36354015caf6 \
  --league mlb \
  --out /Users/reda/polymarket_bot/captures \
  --tail-seconds 120 \
  --max-duration-seconds 21600
```

What it does:
- Uses provider stream profile for one game:
  - Kalstrop (default): scores + odds.
  - BoltOdds override: scores + play-by-play.
- Writes `raw/` and `parsed/` JSONL artifacts plus `manifest.json`.
- Auto-stops on completion + tail window, or max duration.

Override provider explicitly when needed:

```bash
polybot2 provider sync --provider boltodds
polybot2 hotpath run --provider boltodds --league mlb --link-run-id 123
```

## 7) Useful DB/Run-ID Inspection Commands

List recent link runs:

```bash
sqlite3 "$POLYBOT2_DB_PATH" "
SELECT run_id, provider, league_scope, gate_result, n_games_seen, n_games_linked, n_targets_tradeable,
       datetime(run_ts, 'unixepoch') AS run_utc
FROM link_runs
ORDER BY run_id DESC
LIMIT 20;
"
```

See decisions for one run:

```bash
sqlite3 "$POLYBOT2_DB_PATH" "
SELECT provider_game_id, decision, actor, datetime(created_at,'unixepoch') AS ts_utc
FROM link_review_decisions
WHERE run_id = 123 AND provider = 'boltodds'
ORDER BY created_at DESC
LIMIT 100;
"
```

Wipe prior link history and restart IDs:

```bash
sqlite3 "$POLYBOT2_DB_PATH" "
BEGIN;
DELETE FROM link_run_market_targets;
DELETE FROM link_run_event_candidates;
DELETE FROM link_run_game_reviews;
DELETE FROM link_run_provider_games;
DELETE FROM link_review_decisions;
DELETE FROM link_launch_audit;
DELETE FROM link_runs;
DELETE FROM sqlite_sequence
 WHERE name IN ('link_runs','link_review_decisions','link_launch_audit');
COMMIT;
VACUUM;
"
```
## 8) Test Commands

Run full suite:

```bash
PYTHONPATH=/Users/reda/polymarket_bot/polybot2/src pytest -q polybot2/tests
```

Run focused capture tests:

```bash
PYTHONPATH=/Users/reda/polymarket_bot/polybot2/src pytest -q polybot2/tests/test_polybot2_provider_capture.py
```

Run focused linking tests:

```bash
PYTHONPATH=/Users/reda/polymarket_bot/polybot2/src pytest -q polybot2/tests/test_polybot2_linking_v2_matching.py
```
