"""Live trading scope policy for polybot2."""

LIVE_TRADING_VERSION = "v1"

DEFAULT_PROVIDER = None

LIVE_BETTING_LEAGUES = {
    "mlb",
    "epl",
    "ucl",
}
LIVE_BETTING_MARKET_TYPES = {
    "mlb": ["nrfi", 
            "totals", 
            "moneyline",
            "spreads"],
    "epl":["moneyline", 
           "totals", 
           "both_teams_to_score", 
           "spreads", 
           "soccer_halftime_result", 
           "soccer_exact_score", 
           "total_corners"],
    "ucl": ["moneyline", 
            "totals", 
            "both_teams_to_score", 
            "spreads", 
            "soccer_halftime_result", 
            "soccer_exact_score"],
}

# Centralized hotpath execution profile used by league runtime plugins.
HOTPATH_EXECUTION_POLICY = {
    "mlb": {
        "amount_usdc": 5.0,
        "size_shares": 5.0,
        "time_in_force": "GTC",
        "require_presign": True,
        "limit_price": 0.99,
    },
    "epl": {
        "amount_usdc": 5.0,
        "size_shares": 5.0,
        "time_in_force": "GTC",
        "require_presign": True,
        "limit_price": 0.99,
    },
    "ucl": {
        "amount_usdc": 5.0,
        "size_shares": 5.0,
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
        "refresh_interval_seconds": 1800,
    },
    "epl": {
        "plan_horizon_hours": 24,
        "subscribe_lead_minutes": 30,
        "reload_interval_seconds": 120,
        "provider_catalog_max_age_seconds": 20000,
        "refresh_interval_seconds": 300,
    },
    "ucl": {
        "plan_horizon_hours": 24,
        "subscribe_lead_minutes": 30,
        "reload_interval_seconds": 120,
        "provider_catalog_max_age_seconds": 600,
        "refresh_interval_seconds": 300,
    },
}
