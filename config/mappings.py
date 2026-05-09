from baseball_mappings import TEAM_MAP_MLB
from soccer_mappings import TEAM_MAP_BUN, TEAM_MAP_EPL, TEAM_MAP_UCL


MAPPING_VERSION = "v1"
STRICT_FAIL_CLOSED = True  # never guess



# =============================================================================
# LEAGUES MAPPINGS AND THEIR SPORTS
# =============================================================================
# This has entries of the form <canonical_league_name> --> dict with polymarket code and sport family
LEAGUES = {
    # Baseball
    ## Major League Baseball
    "mlb":{
        "polymarket_league_code": "mlb", 
        "sport_family": "baseball", 
        "provider": "kalstrop_v1",
    },
    # Soccer
    ## UEFA Champions League
    "ucl":{
        "polymarket_league_code": "ucl", 
        "sport_family": "soccer", 
        "provider": "kalstrop_v2",
    },
    ## Premier League
    "epl": {
        "polymarket_league_code": "epl",
        "sport_family": "soccer",
        "provider": "boltodds",
    },
    ## Bundesliga (catalog-only, not in LIVE_BETTING_LEAGUES — team mappings incomplete)
    "bundesliga": {
        "polymarket_league_code": "bun",
        "sport_family": "soccer",
        "provider": "kalstrop_v1",
    },
}

# Unambiguous provider league name → canonical league key.
# Only include leagues whose provider name uniquely identifies them.
PROVIDER_LEAGUE_ALIASES = {
    "kalstrop_v1": {
        "mlb": "mlb",
        "bundesliga": "bundesliga",
    },
    "kalstrop_v2":{
        "uefa champions league": "ucl",
    },
    "boltodds":{
        "epl": "epl",
    },
}

# Country-qualified disambiguation for ambiguous provider league names.
# Format: { provider: { (country, league_name): canonical_key } }
# country is matched case-insensitively against Kalstrop category.name.
PROVIDER_LEAGUE_COUNTRY = {
    "kalstrop_v1": {
    },
    "kalstrop_v2":{
    },
}
# =============================================================================
# TEAM ABBREVIATIONS - LEAGUE SPECIFIC
# =============================================================================
TEAM_MAP = {
    "mlb": TEAM_MAP_MLB,
    "bundesliga": TEAM_MAP_BUN,
    "epl": TEAM_MAP_EPL,
    "ucl": TEAM_MAP_UCL,
}

# =============================================================================
# POLYMARKET LEAGUE ORDERINGS - LEAGUE SPECIFIC
# =============================================================================
PM_LEAGUE_ORDERINGS = {
    "mlb": "away",
    "epl": "home", 
    "ucl": "home", 
}

# =============================================================================
# LEAGUE MATCH RULES - LEAGUE SPECIFIC
# =============================================================================
LEAGUE_MATCH_RULES = {
    "default":{
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 31,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "mlb": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 59,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    }
}