"""Live trading scope policy for polybot2."""

LIVE_TRADING_VERSION = "v1"

DEFAULT_PROVIDER = None

LIVE_BETTING_LEAGUES = {
    "mlb",
    "epl",
    "ucl",
    "laliga",
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
            "soccer_halftime_result",],
    "laliga": ["moneyline", 
            "totals", 
            "both_teams_to_score", 
            "spreads", 
            "soccer_halftime_result",],

}

# Centralized hotpath execution profile used by league runtime plugins.
HOTPATH_EXECUTION_POLICY = {
    "mlb": {
        "amount_usdc": 200.0,
        "size_shares": 200.0,
        "time_in_force": "GTC",
        "require_presign": True,
        "limit_price": 0.99,
        "secondary_amount_usdc": 100.0,
        "secondary_size_shares": 100.0, 
        "secondary_time_in_force": "FAK",
        "secondary_limit_price": 0.99,
    },
    "epl": {
        "amount_usdc": 15.0,
        "size_shares": 15.0,
        "time_in_force": "GTC",
        "require_presign": True,
        "limit_price": 0.99,
        "secondary_amount_usdc": 15.0,
        "secondary_size_shares": 15.0, 
        "secondary_time_in_force": "FAK",
        "secondary_limit_price": 0.99,
        
    },
    "ucl": {
        "amount_usdc": 5.0,
        "size_shares": 5.0,
        "time_in_force": "GTC",
        "require_presign": True,
        "limit_price": 0.99,
    },
    "laliga": {
        "amount_usdc": 50.0,
        "size_shares": 50.0,
        "time_in_force": "GTC",
        "require_presign": True,
        "limit_price": 0.99,
        "secondary_amount_usdc": 25.0,
        "secondary_size_shares": 25.0, 
        "secondary_time_in_force": "FAK",
        "secondary_limit_price": 0.99,
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
        "ws_core_idx": 5,
        "submitter_core_idx": 6,
    },
    "epl": {
        "plan_horizon_hours": 12,
        "subscribe_lead_minutes": 30,
        "reload_interval_seconds": 120,
        "provider_catalog_max_age_seconds": 20000,
        "refresh_interval_seconds": 300,
        "ws_core_idx": 1,
        "submitter_core_idx": 2,
    },
    "ucl": {
        "plan_horizon_hours": 12,
        "subscribe_lead_minutes": 30,
        "reload_interval_seconds": 120,
        "provider_catalog_max_age_seconds": 20000,
        "refresh_interval_seconds": 300,
        "ws_core_idx": 3,
        "submitter_core_idx": 4,
    },
    "laliga": {
        "plan_horizon_hours": 12,
        "subscribe_lead_minutes": 30,
        "reload_interval_seconds": 120,
        "provider_catalog_max_age_seconds": 20000,
        "refresh_interval_seconds": 300,
        "ws_core_idx": 7,
        "submitter_core_idx": 8,
    },
}
