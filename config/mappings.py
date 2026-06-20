from baseball_mappings import TEAM_MAP_MLB
from soccer_mappings import TEAM_MAP_BUNDESLIGA, TEAM_MAP_EPL, TEAM_MAP_UCL, TEAM_MAP_LALIGA, TEAM_MAP_FIFA_FRIENDLY, TEAM_MAP_WC
from tennis_mappings import TENNIS_LEAGUES as _TENNIS_LEAGUES, PLAYER_MAP_TENNIS as _PLAYER_MAP_TENNIS
from cs2_mappings import TEAM_MAP_CS2
from lol_mappings import TEAM_MAP_LOL
from dota2_mappings import TEAM_MAP_DOTA2

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
        "provider": ["kalstrop_v2","kalstrop_v1", "boltodds"],
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
        "provider": ["boltodds", "kalstrop_v2", "kalstrop_v1"],
    },

    ## Bundesliga (catalog-only, not in LIVE_BETTING_LEAGUES — team mappings incomplete)
    "fifwc": {
        "polymarket_league_code": "fifwc",
        "sport_family": "soccer",
        "provider": ["kalstrop_v2", "kalstrop_v1"],
    },
    **{
        lk: {
            "polymarket_league_code": cfg["polymarket_league_code"],
            "sport_family": "tennis",
            "provider": cfg.get("provider", "kalstrop_v1"),
            "sets_to_win": cfg.get("sets_to_win", 2),
        }
        for lk, cfg in _TENNIS_LEAGUES.items()
    },
    "cs2":{
        "polymarket_league_code": "cs2",
        "sport_family": "cs2",
        "provider": "pandascore",
    },
    "dota2":{
        "polymarket_league_code": "dota2",
        "sport_family": "moba",
        "provider": "boltodds",
    },
    "lol":{
        "polymarket_league_code": "lol",
        "sport_family": "moba",
        "provider": "boltodds",
    },
    "fifa_friendly": {
        "polymarket_league_code": "fif",
        "sport_family": "soccer",
        "provider": "kalstrop_v2",
    },
}

# Unambiguous provider league name → canonical league key.
# Only include leagues whose provider name uniquely identifies them.
PROVIDER_LEAGUE_ALIASES = {
    "kalstrop_v1": {
        "mlb": "mlb",
        "bundesliga": "bundesliga",
        "laliga": "laliga",
        **{a: lk for lk, cfg in _TENNIS_LEAGUES.items() for a in cfg.get("v1_aliases", [])},
        "cs2": "cs2",
        "dota2": "dota2",
        "lol": "lol",
        "uefa champions league": "ucl",
        "fifa world cup": "fifwc",
    },
    "kalstrop_v2":{
        "uefa-champions-league": "ucl",
        "english-premier-league": "epl",
        "spanish-la-liga-primera": "laliga",
        "international-friendlies": "fifa_friendly",
        "world-cup-2026": "fifwc",
        # Tennis
        "atp-stuttgart": "stuttgart",
        "atp-halle": "halle_m",
        "atp-london": "queen_m",
        "atp-challenger-bratislava": "bratislava",
        "atp-challenger-ilkley": "ilkley_m",
        "atp-challenger-nottingham": "nott_m",
        "wta-berlin": "berlin_w",
        "wta-modena": "modena",
        "wta-nottingham": "nott_w",
        "wta-s-hertogenbosch": "libema_w",
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
        "world cup": "fifwc",
        "la liga": "laliga",
        **{a: lk for lk, cfg in _TENNIS_LEAGUES.items() for a in cfg.get("boltodds_aliases", [])},
        "cs2": "cs2",
        "dota": "dota2",
        "league of legends": "lol",
    },
    "pandascore": {
        # Sport-level fallback (when league name is ambiguous across games)
        "counter-strike": "cs2",
        "lol": "lol",
        "dota 2": "dota2",
        # CS2
        "iem": "cs2",
        "esl pro league": "cs2",
        "european pro league": "cs2",
        "cct europe": "cs2",
        "esea": "cs2",
        "nodwin clutch series": "cs2",
        "united21": "cs2",
        "dfrag": "cs2",
        "dust2.dk ligaen": "cs2",
        "tesfed league": "cs2",
        "dach cs masters": "cs2",
        "exort series": "cs2",
        "ukic": "cs2",
        "xse pro league": "cs2",
        # LoL
        "lck": "lol",
        "lpl": "lol",
        "lcs": "lol",
        "vcs": "lol",
        "lrn": "lol",
        "lrs": "lol",
        "esports world cup": "lol",
        # Dota 2
        "winline star series": "dota2",
    },
}

# Country-qualified disambiguation for ambiguous provider league names.
# Format: { provider: { "country|league_name": canonical_key } }
# country is matched case-insensitively against Kalstrop category.name.
PROVIDER_LEAGUE_COUNTRY = {
    "kalstrop_v1": {
        "england|premier league": "epl",
        "spain|laliga": "laliga",
        "atp|french open men singles": "rgm",
        "wta|french open women singles": "rgw",
        "international clubs|uefa champions league": "ucl",
    },
    "pandascore": {
        # "European Pro League" appears in both CS2 and Dota 2.
        # Disambiguate via sport_raw (stored in category_country_code... actually
        # the linker uses category_name for country-qualified lookup, but PandaScore
        # stores sport_raw separately). The sport_raw fallback in the linker handles this.
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
    "fifa_friendly": {
        "category_slug": "international",
        "tournament_slug": "international-friendlies",
    },
    "fifwc": {
        "category_slug": "international",
        "tournament_slug": "world-cup-2026",
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
    **{lk: _PLAYER_MAP_TENNIS for lk in _TENNIS_LEAGUES},
    "cs2": TEAM_MAP_CS2,
    "lol": TEAM_MAP_LOL,
    "dota2": TEAM_MAP_DOTA2,
    "fifa_friendly": TEAM_MAP_FIFA_FRIENDLY,
    "fifwc": TEAM_MAP_WC,
}

# =============================================================================
# POLYMARKET LEAGUE ORDERINGS - LEAGUE SPECIFIC
# =============================================================================
PM_LEAGUE_ORDERINGS = {
    "mlb": "away",
    "epl": "home", 
    "ucl": "home", 
    "rgm": "home",
    "rgw": "home",
    "birmm": "home",
    "birmw": "home",
    "tyler": "home",
    "centurion": "home",
    "perugia": "home",
    "heilbronn": "home",
    "prostejov": "home",
    "foggia": "home",
    "makarska": "home",
    "queen": "home",
    "stuttgart": "home",
    "libema_m": "home",
    "libema_w": "home",
    "modena": "home",
    "bratislava": "home",
    "ilkley_w": "home",
    "cattolica": "home",
    "ilkley_m": "home",
    "cs2": "home",
    "lol": "home",
    "dota2": "home",
    "fifa_friendly": "home",
    "fifwc": "home",
}

# =============================================================================
# LEAGUE MATCH RULES - LEAGUE SPECIFIC
# =============================================================================
LEAGUE_MATCH_RULES = {
    "default":{
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 240,
        "wide_kickoff_tolerance_minutes": 2880,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "mlb": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 45,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    # Esports: PM uses coarse tournament-level timestamps (e.g., 10:30 for all
    # matches in a day), while V1 has per-match times. Need wider tolerance.
    "cs2": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 360,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "lol": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 360,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "dota2": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 360,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
}
