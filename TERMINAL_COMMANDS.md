# Polybot2 Terminal Commands (Practical Reference)

Copy-paste command guide for day-to-day `polybot2` work.

## 1) Setup

```bash
pip install -e ".[dev]"
```

Build the Rust native module (required for hotpath):

```bash
# macOS (conda + system Python conflict):
env -u CONDA_PREFIX maturin build --release --manifest-path native/polybot2_native/Cargo.toml --interpreter python3
pip install --force-reinstall native/polybot2_native/target/wheels/polybot2_native-*.whl

# Linux / clean virtualenv:
maturin develop --release --manifest-path native/polybot2_native/Cargo.toml
```

Load environment variables (required for most commands; `hotpath live` auto-loads `.env`):

```bash
set -a; source .env; set +a
```

### Rebuild After Pull (EC2)

```bash
maturin build --release --manifest-path native/polybot2_native/Cargo.toml && \
pip install --force-reinstall native/polybot2_native/target/wheels/polybot2_native-*.whl && \
pip install -e ".[dev]"
```

## 2) Environment Variables

```bash
export POLYBOT2_DB_PATH=data/prediction_markets.db
export POLYBOT2_LOG_DIR=logs
export KALSTROP_CLIENT_ID='...'
export KALSTROP_SHARED_SECRET_RAW='...'
export BOLTODDS_API_KEY='...'
```

- `POLYBOT2_DB_PATH`: default DB path for all commands.
- `POLYBOT2_LOG_DIR`: directory for hotpath JSONL log files (default: current directory).
- Kalstrop creds required for `kalstrop_v1` provider. BoltOdds key required for `boltodds` provider.
- Provider per league is derived from `config/mappings.py` — not passed as a CLI flag.

## 3) Prerequisite Pipeline

Run these in order before launching the hotpath:

```bash
polybot2 market sync
polybot2 provider sync
polybot2 link build
polybot2 link review --run-id <N>
```

### Sync Polymarket metadata

```bash
polybot2 market sync              # open markets only (default)
polybot2 market sync --all        # include resolved/closed markets
```

Optional tuning flags: `--batch-size`, `--concurrency`, `--max-rps`, `--open-max-pages`, `--fast-mode`.

### Sync provider games

```bash
polybot2 provider sync                          # sync all configured providers
polybot2 provider sync --provider kalstrop_v1   # sync a single provider
polybot2 provider sync --provider boltodds
polybot2 provider sync --provider kalstrop_v2
```

### Build links

```bash
polybot2 link build                      # live leagues only (default)
polybot2 link build --league-scope all   # include non-live leagues
```

Processes all (league, provider) pairs from config in one `run_id`.

### Review links

Review is opt-out: all linked games enter the plan unless explicitly rejected.

```bash
polybot2 link review --run-id <N>                          # interactive session (mapped_pending scope)
polybot2 link review --run-id <N> --scope all              # review all games
polybot2 link review --run-id <N> --scope mapped           # only mapped games
polybot2 link review --run-id <N> --include-inactive       # include inactive markets
polybot2 link review --run-id <N> --limit 100              # cap number of items
```

## 4) Hotpath

### Launch (one process per league)

```bash
polybot2 hotpath live --league mlb --execution-mode live
polybot2 hotpath live --league epl --execution-mode live
```

Paper trading:

```bash
polybot2 hotpath live --league mlb --execution-mode paper
```

With explicit link run and refresh interval:

```bash
polybot2 hotpath live --league mlb --execution-mode live --link-run-id <N> --refresh-interval 300
```

Optional subscription filter:

```bash
export POLYBOT2_SUBSCRIBE_UNIVERSAL_IDS='game_label_1,game_label_2'
polybot2 hotpath live --league epl --execution-mode paper
```

### Live observer (terminal scoreboard)

In a separate terminal while the hotpath is running:

```bash
# Auto-discover latest log file:
polybot2 hotpath observe --run-id <N> --league mlb

# Specify the log file directly:
polybot2 hotpath observe --log-file logs/hotpath_1_20260504T183200Z.jsonl

# With team name resolution:
polybot2 hotpath observe --run-id 1--link-run-id 1--league epl
```

### Inspect log files with jq

```bash
# All order events:
jq 'select(.ev == "order")' logs/hotpath_*.jsonl

# Failed orders only:
jq 'select(.ev == "order" and .ok == false)' logs/hotpath_*.jsonl

# Score progression for one game:
jq 'select(.ev == "tick" and .gid == "Chelsea vs Arsenal, 2026-05-04, 15")' logs/hotpath_*.jsonl

# Tail live:
tail -f logs/hotpath_*.jsonl | jq --unbuffered 'select(.ev == "order")'
```

## 5) Raw Score Frame Capture

Standalone scripts in `scripts/` (not part of the CLI):

```bash
# Multi-provider comparison capture:
python scripts/capture_multi.py --games-file games.json --out ./captures --duration 14400

# Single Kalstrop V1 fixture:
python scripts/capture_kalstrop_v1.py --fixture-id <UUID> --out ./captures --duration 7200

# Single BoltOdds capture:
python scripts/capture_boltodds.py --games-file games.json --out ./captures --duration 7200

# Kalstrop V2 capture:
python scripts/capture_kalstrop_v2.py --fixture-id <EVENT_ID> --out ./captures --duration 7200
```

## 6) Tests

```bash
# Rust tests:
cargo test --manifest-path native/polybot2_native/Cargo.toml

# Python full suite (excluding live tests):
pytest tests/ --ignore=tests/live -q

# Single test:
pytest tests/test_polybot2_hotpath_observe.py -k "heartbeat"
```

## 7) DB Inspection

List recent link runs:

```bash
sqlite3 "$POLYBOT2_DB_PATH" "
SELECT run_id, provider, league, league_scope, gate_result,
       n_games_seen, n_games_linked, n_targets_tradeable,
       datetime(run_ts, 'unixepoch') AS run_utc
FROM link_runs
ORDER BY run_id DESC
LIMIT 20;
"
```

See review decisions for a run:

```bash
sqlite3 "$POLYBOT2_DB_PATH" "
SELECT provider_game_id, decision, actor, datetime(decided_at,'unixepoch') AS ts_utc
FROM link_review_decisions
WHERE run_id = <N>
ORDER BY decided_at DESC
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
DELETE FROM link_runs;
DELETE FROM sqlite_sequence
 WHERE name IN ('link_runs','link_review_decisions');
COMMIT;
VACUUM;
"
```
