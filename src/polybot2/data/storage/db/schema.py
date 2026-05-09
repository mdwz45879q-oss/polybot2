"""SQLite schema for polybot2."""

SCHEMA_VERSION = 7

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS _schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sync_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pm_events (
    event_id                TEXT PRIMARY KEY,
    title                   TEXT NOT NULL DEFAULT '',
    ticker                  TEXT NOT NULL DEFAULT '',
    slug                    TEXT NOT NULL DEFAULT '',
    slug_raw                TEXT NOT NULL DEFAULT '',
    sport_key               TEXT NOT NULL DEFAULT '',
    league_key              TEXT NOT NULL DEFAULT '',
    game_id                 INTEGER,
    game_date_et            TEXT NOT NULL DEFAULT '',
    kickoff_ts_utc          INTEGER,
    start_ts_utc            INTEGER,
    end_ts_utc              INTEGER,
    status                  TEXT NOT NULL DEFAULT '',
    updated_at              INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pm_markets (
    condition_id            TEXT PRIMARY KEY,
    market_id               TEXT NOT NULL DEFAULT '',
    event_id                TEXT NOT NULL DEFAULT '',
    question                TEXT NOT NULL DEFAULT '',
    question_id             TEXT NOT NULL DEFAULT '',
    slug                    TEXT NOT NULL DEFAULT '',
    sports_market_type      TEXT NOT NULL DEFAULT '',
    line                    REAL,
    event_start_ts_utc      INTEGER,
    game_start_ts_utc       INTEGER,
    resolved                INTEGER NOT NULL DEFAULT 0,
    resolution_value        REAL,
    volume                  REAL NOT NULL DEFAULT 0,
    end_date                TEXT NOT NULL DEFAULT '',
    end_ts_utc              INTEGER,
    updated_at              INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pm_market_tokens (
    token_id                TEXT PRIMARY KEY,
    condition_id            TEXT NOT NULL DEFAULT '',
    outcome_index           INTEGER NOT NULL DEFAULT 0,
    outcome_label           TEXT NOT NULL DEFAULT '',
    updated_at              INTEGER NOT NULL DEFAULT 0,
    UNIQUE (condition_id, outcome_index)
);

CREATE TABLE IF NOT EXISTS pm_market_tags (
    condition_id            TEXT NOT NULL DEFAULT '',
    tag_id                  INTEGER NOT NULL DEFAULT 0,
    label                   TEXT NOT NULL DEFAULT '',
    slug                    TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (condition_id, slug)
);

CREATE TABLE IF NOT EXISTS pm_event_teams (
    event_id                TEXT NOT NULL DEFAULT '',
    team_index              INTEGER NOT NULL DEFAULT 0,
    team_id                 INTEGER,
    provider_team_id        INTEGER,
    name                    TEXT NOT NULL DEFAULT '',
    league                  TEXT NOT NULL DEFAULT '',
    abbreviation            TEXT NOT NULL DEFAULT '',
    alias                   TEXT NOT NULL DEFAULT '',
    record                  TEXT NOT NULL DEFAULT '',
    logo                    TEXT NOT NULL DEFAULT '',
    color                   TEXT NOT NULL DEFAULT '',
    updated_at              INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (event_id, team_index)
);

CREATE TABLE IF NOT EXISTS pm_sports_market_types_ref (
    market_type             TEXT PRIMARY KEY,
    synced_at               INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS provider_games (
    provider                TEXT NOT NULL DEFAULT '',
    provider_game_id        TEXT NOT NULL DEFAULT '',
    game_label              TEXT NOT NULL DEFAULT '',
    orig_teams              TEXT NOT NULL DEFAULT '',
    sport_raw               TEXT NOT NULL DEFAULT '',
    league_raw              TEXT NOT NULL DEFAULT '',
    category_name           TEXT NOT NULL DEFAULT '',
    category_country_code   TEXT NOT NULL DEFAULT '',
    when_raw             TEXT NOT NULL DEFAULT '',
    start_ts_utc            INTEGER,
    game_date_et            TEXT NOT NULL DEFAULT '',
    home_raw                TEXT NOT NULL DEFAULT '',
    away_raw                TEXT NOT NULL DEFAULT '',
    parse_status            TEXT NOT NULL DEFAULT '',
    parse_reason            TEXT NOT NULL DEFAULT '',
    updated_at              INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (provider, provider_game_id)
);

CREATE TABLE IF NOT EXISTS link_runs (
    run_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_ts                  INTEGER NOT NULL DEFAULT 0,
    provider                TEXT NOT NULL DEFAULT '',
    league                  TEXT NOT NULL DEFAULT '',
    league_scope            TEXT NOT NULL DEFAULT '',
    mapping_version         TEXT NOT NULL DEFAULT '',
    mapping_hash            TEXT NOT NULL DEFAULT '',
    n_games_seen            INTEGER NOT NULL DEFAULT 0,
    n_games_linked          INTEGER NOT NULL DEFAULT 0,
    n_games_tradeable       INTEGER NOT NULL DEFAULT 0,
    n_targets               INTEGER NOT NULL DEFAULT 0,
    n_targets_tradeable     INTEGER NOT NULL DEFAULT 0,
    gate_result             TEXT NOT NULL DEFAULT 'fail',
    report_json             TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS link_game_bindings (
    provider                TEXT NOT NULL DEFAULT '',
    provider_game_id        TEXT NOT NULL DEFAULT '',
    canonical_league        TEXT NOT NULL DEFAULT '',
    canonical_home_team     TEXT NOT NULL DEFAULT '',
    canonical_away_team     TEXT NOT NULL DEFAULT '',
    event_slug_prefix       TEXT NOT NULL DEFAULT '',
    binding_status          TEXT NOT NULL DEFAULT '',
    reason_code             TEXT NOT NULL DEFAULT '',
    is_tradeable            INTEGER NOT NULL DEFAULT 0,
    mapping_version         TEXT NOT NULL DEFAULT '',
    mapping_hash            TEXT NOT NULL DEFAULT '',
    run_id                  INTEGER,
    updated_at              INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (provider, provider_game_id)
);

CREATE TABLE IF NOT EXISTS link_event_bindings (
    provider                TEXT NOT NULL DEFAULT '',
    provider_game_id        TEXT NOT NULL DEFAULT '',
    event_id                TEXT NOT NULL DEFAULT '',
    event_slug_prefix       TEXT NOT NULL DEFAULT '',
    updated_at              INTEGER NOT NULL DEFAULT 0,
    run_id                  INTEGER,
    PRIMARY KEY (provider, provider_game_id, event_id)
);

CREATE TABLE IF NOT EXISTS link_market_bindings (
    provider                TEXT NOT NULL DEFAULT '',
    provider_game_id        TEXT NOT NULL DEFAULT '',
    condition_id            TEXT NOT NULL DEFAULT '',
    outcome_index           INTEGER NOT NULL DEFAULT 0,
    token_id                TEXT NOT NULL DEFAULT '',
    market_slug             TEXT NOT NULL DEFAULT '',
    sports_market_type      TEXT NOT NULL DEFAULT '',
    binding_status          TEXT NOT NULL DEFAULT '',
    reason_code             TEXT NOT NULL DEFAULT '',
    is_tradeable            INTEGER NOT NULL DEFAULT 0,
    mapping_version         TEXT NOT NULL DEFAULT '',
    mapping_hash            TEXT NOT NULL DEFAULT '',
    run_id                  INTEGER,
    updated_at              INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (provider, provider_game_id, condition_id, outcome_index)
);

CREATE TABLE IF NOT EXISTS link_run_provider_games (
    run_id                   INTEGER NOT NULL,
    provider                 TEXT NOT NULL DEFAULT '',
    provider_game_id         TEXT NOT NULL DEFAULT '',
    parse_status             TEXT NOT NULL DEFAULT '',
    parse_reason             TEXT NOT NULL DEFAULT '',
    game_label               TEXT NOT NULL DEFAULT '',
    sport_raw                TEXT NOT NULL DEFAULT '',
    league_raw               TEXT NOT NULL DEFAULT '',
    when_raw              TEXT NOT NULL DEFAULT '',
    start_ts_utc             INTEGER,
    game_date_et             TEXT NOT NULL DEFAULT '',
    home_raw                 TEXT NOT NULL DEFAULT '',
    away_raw                 TEXT NOT NULL DEFAULT '',
    canonical_league         TEXT NOT NULL DEFAULT '',
    canonical_home_team      TEXT NOT NULL DEFAULT '',
    canonical_away_team      TEXT NOT NULL DEFAULT '',
    event_slug_prefix        TEXT NOT NULL DEFAULT '',
    binding_status           TEXT NOT NULL DEFAULT '',
    reason_code              TEXT NOT NULL DEFAULT '',
    is_tradeable             INTEGER NOT NULL DEFAULT 0,
    updated_at               INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, provider, provider_game_id)
);

CREATE TABLE IF NOT EXISTS link_run_game_reviews (
    run_id                   INTEGER NOT NULL,
    provider                 TEXT NOT NULL DEFAULT '',
    provider_game_id         TEXT NOT NULL DEFAULT '',
    resolution_state         TEXT NOT NULL DEFAULT '',
    reason_code              TEXT NOT NULL DEFAULT '',
    selected_event_id        TEXT NOT NULL DEFAULT '',
    selected_event_slug      TEXT NOT NULL DEFAULT '',
    used_slug_fallback       INTEGER NOT NULL DEFAULT 0,
    kickoff_tolerance_minutes INTEGER NOT NULL DEFAULT 0,
    kickoff_delta_sec        INTEGER,
    score_tuple              TEXT NOT NULL DEFAULT '',
    trace_json               TEXT NOT NULL DEFAULT '{}',
    updated_at               INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, provider, provider_game_id)
);

CREATE TABLE IF NOT EXISTS link_run_event_candidates (
    run_id                   INTEGER NOT NULL,
    provider                 TEXT NOT NULL DEFAULT '',
    provider_game_id         TEXT NOT NULL DEFAULT '',
    candidate_rank           INTEGER NOT NULL DEFAULT 0,
    event_id                 TEXT NOT NULL DEFAULT '',
    event_slug               TEXT NOT NULL DEFAULT '',
    kickoff_ts_utc           INTEGER,
    team_set_match           INTEGER NOT NULL DEFAULT 0,
    kickoff_within_tolerance INTEGER,
    slug_hint_match          INTEGER NOT NULL DEFAULT 0,
    ordering_bonus           INTEGER NOT NULL DEFAULT 0,
    kickoff_delta_sec        INTEGER,
    score_tuple              TEXT NOT NULL DEFAULT '',
    is_selected              INTEGER NOT NULL DEFAULT 0,
    reject_reason            TEXT NOT NULL DEFAULT '',
    updated_at               INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, provider, provider_game_id, candidate_rank, event_id)
);

CREATE TABLE IF NOT EXISTS link_run_market_targets (
    run_id                   INTEGER NOT NULL,
    provider                 TEXT NOT NULL DEFAULT '',
    provider_game_id         TEXT NOT NULL DEFAULT '',
    condition_id             TEXT NOT NULL DEFAULT '',
    outcome_index            INTEGER NOT NULL DEFAULT 0,
    token_id                 TEXT NOT NULL DEFAULT '',
    market_slug              TEXT NOT NULL DEFAULT '',
    sports_market_type       TEXT NOT NULL DEFAULT '',
    binding_status           TEXT NOT NULL DEFAULT '',
    reason_code              TEXT NOT NULL DEFAULT '',
    is_tradeable             INTEGER NOT NULL DEFAULT 0,
    updated_at               INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, provider, provider_game_id, condition_id, outcome_index)
);

CREATE TABLE IF NOT EXISTS link_review_decisions (
    decision_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                   INTEGER NOT NULL,
    provider                 TEXT NOT NULL DEFAULT '',
    provider_game_id         TEXT NOT NULL DEFAULT '',
    decision                 TEXT NOT NULL DEFAULT '',
    note                     TEXT NOT NULL DEFAULT '',
    actor                    TEXT NOT NULL DEFAULT '',
    decided_at               INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pm_markets_event_id ON pm_markets(event_id);
CREATE INDEX IF NOT EXISTS idx_pm_market_tokens_condition_id ON pm_market_tokens(condition_id);
CREATE INDEX IF NOT EXISTS idx_pm_event_teams_event_id ON pm_event_teams(event_id);
CREATE INDEX IF NOT EXISTS idx_pm_event_teams_team_id ON pm_event_teams(team_id);
CREATE INDEX IF NOT EXISTS idx_pm_event_teams_provider_team_id ON pm_event_teams(provider_team_id);
CREATE INDEX IF NOT EXISTS idx_pm_event_teams_league_abbrev ON pm_event_teams(league, abbreviation);
CREATE INDEX IF NOT EXISTS idx_provider_games_provider_game ON provider_games(provider, provider_game_id);
CREATE INDEX IF NOT EXISTS idx_link_market_bindings_provider_game_tradeable
    ON link_market_bindings(provider, provider_game_id, is_tradeable);
CREATE INDEX IF NOT EXISTS idx_link_run_provider_games_run_provider
    ON link_run_provider_games(run_id, provider, provider_game_id);
CREATE INDEX IF NOT EXISTS idx_link_run_game_reviews_run_provider
    ON link_run_game_reviews(run_id, provider, provider_game_id);
CREATE INDEX IF NOT EXISTS idx_link_run_event_candidates_run_provider
    ON link_run_event_candidates(run_id, provider, provider_game_id, candidate_rank);
CREATE INDEX IF NOT EXISTS idx_link_run_market_targets_run_provider
    ON link_run_market_targets(run_id, provider, provider_game_id, condition_id, outcome_index);
CREATE INDEX IF NOT EXISTS idx_link_review_decisions_run_provider_game
    ON link_review_decisions(run_id, provider, provider_game_id, decision_id);
CREATE INDEX IF NOT EXISTS idx_pm_events_league_date
    ON pm_events(league_key, game_date_et);
"""
