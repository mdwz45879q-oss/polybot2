from baseball_mappings import TEAM_MAP_MLB
from soccer_mappings import TEAM_MAP_BUNDESLIGA, TEAM_MAP_EPL, TEAM_MAP_UCL, TEAM_MAP_LALIGA, TEAM_MAP_FIFA_FRIENDLY, TEAM_MAP_WC
from tennis_mappings import PLAYER_MAP_FRENCH_OPEN_MEN_SINGLES, PLAYER_MAP_FRENCH_OPEN_WOMEN_SINGLES, PLAYER_MAP_BIRMINGHAM_MEN, PLAYER_MAP_BIRMINGHAM_WOMEN, PLAYER_MAP_TYLER, PLAYER_MAP_CENTURION, PLAYER_MAP_PERUGIA, PLAYER_MAP_HEILBRONN, PLAYER_MAP_PROSTEJOV, PLAYER_MAP_FOGGIA, PLAYER_MAP_MAKARSKA, PLAYER_MAP_QUEEN, PLAYER_MAP_STUTTGART, PLAYER_MAP_LIBEMA_M, PLAYER_MAP_LIBEMA_W, PLAYER_MAP_MODENA, PLAYER_MAP_BRATISLAVA, PLAYER_MAP_ILKLEY_W, PLAYER_MAP_CATTOLICA, PLAYER_MAP_ILKLEY_M, PLAYER_MAP_LYON, PLAYER_MAP_TUCUMAN, PLAYER_MAP_HALLE_M, PLAYER_MAP_QUEEN_M, PLAYER_MAP_BERLIN_W, PLAYER_MAP_NOTTINGHAM_W, PLAYER_MAP_NOTTINGHAM_M
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
    "rgm": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 3,
    },
    "rgw":
    {
        "polymarket_league_code":"wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "birmm": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "birmw": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "tyler": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "centurion": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "perugia": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "heilbronn": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "prostejov": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "foggia": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "makarska": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "queen": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "stuttgart": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "libema_m": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "libema_w": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "modena": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "bratislava": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "ilkley_w": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "cattolica": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "ilkley_m": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "lyon": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "tucuman": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "halle_m": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "queen_m": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "berlin_w": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "nott_w": {
        "polymarket_league_code": "wta",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "nott_m": {
        "polymarket_league_code": "atp",
        "sport_family": "tennis",
        "provider": "kalstrop_v1",
        "sets_to_win": 2,
    },
    "cs2":{
        "polymarket_league_code": "cs2",
        "sport_family": "cs2",
        "provider": "kalstrop_v1",
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
        "french open men singles": "rgm",
        "french open women singles": "rgw",
        "atp challenger birmingham": "birmm",
        "wta 125k birmingham": "birmw",
        "atp challenger tyler": "tyler",
        "atp challenger centurion 2": "centurion",
        "atp challenger perugia": "perugia",
        "atp challenger bad rappenau": "heilbronn",
        "atp challenger prostejov": "prostejov",
        "wta 125k foggia": "foggia",
        "wta 125k makarska": "makarska",
        "wta london": "queen",
        "atp stuttgart": "stuttgart",
        "atp s-hertogenbosch": "libema_m",
        "wta s-hertogenbosch": "libema_w",
        "wta 125k modena": "modena",
        "atp challenger bratislava": "bratislava",
        "wta 125k ilkley": "ilkley_w",
        "atp challenger cattolica": "cattolica",
        "atp challenger ilkley": "ilkley_m",
        "atp challenger lyon": "lyon",
        "atp challenger san miguel de tucuman": "tucuman",
        "atp halle": "halle_m",
        "atp london": "queen_m",
        "berlin": "berlin_w",
        "wta nottingham": "nott_w",
        "atp challenger nottingham 2": "nott_m",
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
        "roland garros (m) - tennis": "rgm",
        "roland garros (w) - tennis": "rgw",
        "challenger birmingham - tennis": "birmm",
        "wta birmingham - tennis": "birmw",
        "challenger tyler - tennis": "tyler",
        "challenger centurion - tennis": "centurion",
        "challenger perugia - tennis": "perugia",
        "challenger heilbronn - tennis": "heilbronn",
        "challenger prostejov - tennis": "prostejov",
        "wta foggia - tennis": "foggia",
        "wta makarska - tennis": "makarska",
        "wta london - tennis": "queen",
        "atp stuttgart - tennis": "stuttgart",
        "atp s-hertogenbosch - tennis": "libema_m",
        "wta s-hertogenbosch - tennis": "libema_w",
        "wta modena - tennis": "modena",
        "challenger bratislava - tennis": "bratislava",
        "wta ilkley - tennis": "ilkley_w",
        "challenger cattolica - tennis": "cattolica",
        "challenger ilkley - tennis": "ilkley_m",
        "challenger lyon - tennis": "lyon",
        "challenger san miguel de tucuman - tennis": "tucuman",
        "atp halle - tennis": "halle_m",
        "atp london - tennis": "queen_m",
        "berlin - tennis": "berlin_w",
        "wta nottingham - tennis": "nott_w",
        "challenger nottingham - tennis": "nott_m",
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
    "rgm": PLAYER_MAP_FRENCH_OPEN_MEN_SINGLES,
    "rgw": PLAYER_MAP_FRENCH_OPEN_WOMEN_SINGLES,
    "birmm": PLAYER_MAP_BIRMINGHAM_MEN,
    "birmw": PLAYER_MAP_BIRMINGHAM_WOMEN,
    "tyler": PLAYER_MAP_TYLER,
    "centurion": PLAYER_MAP_CENTURION,
    "perugia": PLAYER_MAP_PERUGIA,
    "heilbronn": PLAYER_MAP_HEILBRONN,
    "prostejov": PLAYER_MAP_PROSTEJOV,
    "foggia": PLAYER_MAP_FOGGIA,
    "makarska": PLAYER_MAP_MAKARSKA,
    "queen": PLAYER_MAP_QUEEN,
    "stuttgart": PLAYER_MAP_STUTTGART,
    "libema_m": PLAYER_MAP_LIBEMA_M,
    "libema_w": PLAYER_MAP_LIBEMA_W,
    "modena": PLAYER_MAP_MODENA,
    "bratislava": PLAYER_MAP_BRATISLAVA,
    "ilkley_w": PLAYER_MAP_ILKLEY_W,
    "cattolica": PLAYER_MAP_CATTOLICA,
    "ilkley_m": PLAYER_MAP_ILKLEY_M,
    "lyon": PLAYER_MAP_LYON,
    "tucuman": PLAYER_MAP_TUCUMAN,
    "halle_m": PLAYER_MAP_HALLE_M,
    "queen_m": PLAYER_MAP_QUEEN_M,
    "berlin_w": PLAYER_MAP_BERLIN_W,
    "nott_w": PLAYER_MAP_NOTTINGHAM_W,
    "nott_m": PLAYER_MAP_NOTTINGHAM_M,
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
        "kickoff_tolerance_minutes": 30,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "mlb": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 30,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "rgm": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 120,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "rgw": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 120,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "birmm": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 120,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    },
    "birmw": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 120,
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
