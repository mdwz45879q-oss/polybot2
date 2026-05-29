from baseball_mappings import TEAM_MAP_MLB
from soccer_mappings import TEAM_MAP_BUNDESLIGA, TEAM_MAP_EPL, TEAM_MAP_UCL, TEAM_MAP_LALIGA
from tennis_mappings import PLAYER_MAP_FRENCH_OPEN_MEN_SINGLES, PLAYER_MAP_FRENCH_OPEN_WOMEN_SINGLES

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
        "provider": ["kalstrop_v1", "boltodds"]
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
        "provider": "kalstrop_v2",
    },
    ## Bundesliga (catalog-only, not in LIVE_BETTING_LEAGUES — team mappings incomplete)
    "laliga": {
        "polymarket_league_code": "lal",
        "sport_family": "soccer",
        "provider": ["kalstrop_v2", "kalstrop_v1"],
    },
    "rolgar": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 3,
    },
    "garros":
    {
        "polymarket_league_code":"wta", 
        "sport_family": "tennis", 
        "provider": "kalstrop_v1",
        "sets_to_win": 2, 
    },
    "cs2":{
        "polymarket_league_code": "cs2",
        "sport_family": "esports",
        "provider": ["kalstrop_v1", "boltodds"],
    },
     "dota2":{
        "polymarket_league_code": "dota2",
        "sport_family": "esports",
        "provider": ["kalstrop_v1", "boltodds"],
    },
    "lol":
    {
        "polymarket_league_code": "lol",
        "sport_family": "esports",
        "provider": ["kalstrop_v1", "boltodds"],
    }


}

# Unambiguous provider league name → canonical league key.
# Only include leagues whose provider name uniquely identifies them.
PROVIDER_LEAGUE_ALIASES = {
    "kalstrop_v1": {
        "mlb": "mlb",
        "bundesliga": "bundesliga",
        "laliga": "laliga",
        "french open men singles": "rolgar",
        "cs2": "cs2",
        "dota2": "dota2",
        "lol": "lol",
        "french open women singles": "garros",
    },
    "kalstrop_v2":{
        "uefa-champions-league": "ucl",
        "english-premier-league": "epl",
        "spanish-la-liga-primera": "laliga",
        "french-open-mens-singles": "rolgar",
    },
    "kalstrop_opta": {
        "premier league": "epl",
        "laliga": "laliga",
        "bundesliga": "bundesliga",
        "uefa champions league": "ucl",
        "mlb": "mlb",
    },
    "boltodds":{
        "epl": "epl",
        "mlb": "mlb",
        "bundesliga": "bundesliga",
        "champions league": "ucl",
        "la liga": "laliga",
        "cs2": "cs2",
        "dota": "dota2",
        "league of legends": "lol",
        "roland garros (m) - tennis": "rolgar",
    },
}

# Country-qualified disambiguation for ambiguous provider league names.
# Format: { provider: { "country|league_name": canonical_key } }
# country is matched case-insensitively against Kalstrop category.name.
PROVIDER_LEAGUE_COUNTRY = {
    "kalstrop_v1": {
        "england|premier league": "epl",
        "spain|laliga": "laliga",
        "atp|french open men singles": "rolgar",
        "wta|french open women singles": "garros",
    },
}

KALSTROP_V2_SLUGS = {
    "laliga": {
        "category_slug": "spain",
        "tournament_slug": "spanish-la-liga-primera",
    },
    "bundesliga": {
        "category_slug": "germany", 
        "tournament_slug": "german-bundesliga",
    },
    "ucl": {
        "category_slug": "europe",
        "tournament_slug": "uefa-champions-league",
    },
}
# =============================================================================
# TEAM ABBREVIATIONS - LEAGUE SPECIFIC
# =============================================================================
TEAM_MAP = {
    "mlb": TEAM_MAP_MLB,
    "bundesliga": TEAM_MAP_BUNDESLIGA,
    "epl": TEAM_MAP_EPL,
    "ucl": TEAM_MAP_UCL,
    "laliga": TEAM_MAP_LALIGA,
    "rolgar": PLAYER_MAP_FRENCH_OPEN_MEN_SINGLES,
    "garros": PLAYER_MAP_FRENCH_OPEN_WOMEN_SINGLES,
}

# =============================================================================
# POLYMARKET LEAGUE ORDERINGS - LEAGUE SPECIFIC
# =============================================================================
PM_LEAGUE_ORDERINGS = {
    "mlb": "away",
    "epl": "home", 
    "ucl": "home", 
    "rolgar": "home",
    "garros": "home",
}

# =============================================================================
# LEAGUE MATCH RULES - LEAGUE SPECIFIC
# =============================================================================
LEAGUE_MATCH_RULES = {
    "default":{
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 30,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "mlb": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 30,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    }
}