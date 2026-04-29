"""Live trading scope policy for polybot2."""

LIVE_TRADING_VERSION = "v1"

DEFAULT_PROVIDER = "kalstrop"

LIVE_BETTING_LEAGUES = {
    "mlb",
    # "bundesliga",
}

LIVE_BETTING_MARKET_TYPES = {
    "bundesliga": ["moneyline", "totals", "both_teams_to_score"],
    "mlb": ["nrfi", "totals", "moneyline",],
}

# Centralized hotpath execution profile used by league runtime plugins.
HOTPATH_EXECUTION_POLICY = {
    "mlb": {
        "amount_usdc": 15.0,
        "size_shares": 15.0,
        "time_in_force": "GTC",
        "require_presign": True,
        "limit_price": 0.99,
    }
}

# Runtime timing controls for live snapshot refresh and subscription windows.
HOTPATH_RUNTIME_POLICY = {
    "mlb": {
        "plan_horizon_hours": 12,
        "subscribe_lead_minutes": 5,
        "reload_interval_seconds": 120,
        "provider_catalog_max_age_seconds": 20000,
    },
    "bundesliga": {
        "plan_horizon_hours": 24,
        "subscribe_lead_minutes": 90,
        "reload_interval_seconds": 120,
        "provider_catalog_max_age_seconds": 600,
    },
}