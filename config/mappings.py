MAPPING_VERSION = "v1"
STRICT_FAIL_CLOSED = True  # never guess


LEAGUES = {
    # BasketBall
    ## NBA
    "nba": {
        "polymarket_league_code": "nba",
        "sport_family": "basketball",
    },
    # Baseball
    ## Major League Baseball
    "mlb":{
        "polymarket_league_code": "mlb", 
        "sport_family": "baseball", 
    },
    # Hockey
    ## NHL
    "nhl":{
        "polymarket_league_code": "nhl", 
        "sport_family": "hockey",
    },
    # Soccer
    ## UEFA Champions League
    "ucl":{
        "polymarket_league_code": "ucl", 
        "sport_family": "soccer", 
    },
    ## Fifa World Cup
    "fifa-world-cup": {
        "polymarket_league_code": "fifwc", 
        "sport_family": "soccer", 
    },
    ## Premier League
    "epl": {
        "polymarket_league_code": "epl", 
        "sport_family": "soccer", 
    },
   ## Bundesliga (German League)
   "bundesliga":{
       "polymarket_league_code": "bun", 
       "sport_family": "soccer",
   },
   ## La Liga (Spanish League)
   "laliga":{
       "polymarket_league_code": "lal", 
       "sport_family": "soccer",
   }
}


PROVIDER_LEAGUE_ALIASES = {
    "boltodds": {
        # Basketball
        "nba": "nba",
        # Baseball
        "mlb": "mlb",
        # Hockey
        "nhl": "nhl",
        # Soccer
        "champions league": "ucl",
        "world cup": "fifa-world-cup",
        "epl": "epl",
        "bundesliga": "bundesliga",
        "la liga": "laliga",
    },
    "kalstrop":{
    },
}


# =============================================================================
# TEAM ABBREVIATIONS - LEAGUE SPECIFIC
# =============================================================================

TEAM_MAP_BUN={
    "augsburg": {
        "polymarket_code": "aug",
        "provider_aliases": {
            "boltodds": ["Augsburg"],
            "kalstrop": [],
        },
    },
    "bayer leverkusen": {
        "polymarket_code": "b04",
        "provider_aliases": {
            "boltodds": ["Leverkusen"],
            "kalstrop": [],
        },
    },
    "bayern munchen": {
        "polymarket_code": "bay",
        "provider_aliases": {
            "boltodds": ["Bayern Munchen"],
            "kalstrop": [],
        },
    },
    "borussia dortmund": {
        "polymarket_code": "dor",
        "provider_aliases": {
            "boltodds": ["Dortmund"],
            "kalstrop": [],
        },
    },
    "borussia monchengladbach": {
        "polymarket_code": "moe",
        "provider_aliases": {
            "boltodds": ["Monchengladbach"],
            "kalstrop": [],
        },
    },
    "eintracht frankfurt": {
        "polymarket_code": "ein",
        "provider_aliases": {
            "boltodds": ["Eintracht Frankfurt"],
            "kalstrop": [],
        },
    },
    "freiburg": {
        "polymarket_code": "fre",
        "provider_aliases": {
            "boltodds": ["Freiburg"],
            "kalstrop": [],
        },
    },
    "hamburger sv": {
        "polymarket_code": "hsv",
        "provider_aliases": {
            "boltodds": ["Hamburg"],
            "kalstrop": [],
        },
    },
    "heidenheim": {
        "polymarket_code": "hei",
        "provider_aliases": {
            "boltodds": ["FC Heidenheim"],
            "kalstrop": [],
        },
    },
    "hoffenheim": {
        "polymarket_code": "hof",
        "provider_aliases": {
            "boltodds": ["Hoffenheim"],
            "kalstrop": [],
        },
    },
    "koln": {
        "polymarket_code": "koe",
        "provider_aliases": {
            "boltodds": ["FC Koln", "Cologne"],
            "kalstrop": [],
        },
    },
    "mainz": {
        "polymarket_code": "mai",
        "provider_aliases": {
            "boltodds": ["Mainz"],
            "kalstrop": [],
        },
    },
    "rb leipzig": {
        "polymarket_code": "lei",
        "provider_aliases": {
            "boltodds": ["RB Leipzig"],
            "kalstrop": [],
        },
    },
    "st pauli": {
        "polymarket_code": "pau",
        "provider_aliases": {
            "boltodds": ["St Pauli"],
            "kalstrop": [],
        },
    },
    "stuttgart": {
        "polymarket_code": "stu",
        "provider_aliases": {
            "boltodds": ["Stuttgart"],
            "kalstrop": [],
        },
    },
    "union berlin": {
        "polymarket_code": "uni",
        "provider_aliases": {
            "boltodds": ["Union Berlin", "FC Union Berlin"],
            "kalstrop": [],
        },
    },
    "werder bremen": {
        "polymarket_code": "wer",
        "provider_aliases": {
            "boltodds": ["Werder Bremen"],
            "kalstrop": [],
        },
    },
    "wolfsburg": {
        "polymarket_code": "wol",
        "provider_aliases": {
            "boltodds": ["Wolfsburg"],
            "kalstrop": [],
        },
    },
}

TEAM_MAP_MLB = {
    # Arizona Diamondbacks
    "arizona diamondbacks": {
            "polymarket_code": "ari",
            "provider_aliases": {
                "kalstrop": ['arizona diamondbacks',],
                "boltodds": ['ari diamondbacks','arizona diamondbacks',],
            },
    },
    # Atlanta Braves
    "atlanta braves": {
            "polymarket_code": "atl",
            "provider_aliases": {
                "kalstrop": ['atlanta braves',],
                "boltodds": ['atl braves', 'atlanta braves',],
            },
    },
    # Baltimore Orioles
    "baltimore orioles": {
            "polymarket_code": "bal",
            "provider_aliases": {
                "kalstrop": ['baltimore orioles',],
                "boltodds": ['bal orioles', 'baltimore orioles',],
            },
    },
    # Boston Red Sox
    "boston red sox": {
            "polymarket_code": "bos",
            "provider_aliases": {
                "kalstrop": ['boston red sox',],
                "boltodds": ['bos red sox', 'boston red sox',],
            },
    },
    # Chicago Cubs
    "chicago cubs": {
            "polymarket_code": "chc",
            "provider_aliases": {
                "kalstrop": ['chicago cubs',],
                "boltodds": ['chi cubs', 'chicago cubs',],
            },
    },
    # Chicago White Sox
    "chicago white sox": {
            "polymarket_code": "cws",
            "provider_aliases": {
                "kalstrop": ['chicago white sox',],
                "boltodds": ['chi white sox', 'chicago white sox',],
            },
    },
    # Cincinnati Reds
    "cincinnati reds": {
            "polymarket_code": "cin",
            "provider_aliases": {
                "kalstrop": ['cincinnati reds',],
                "boltodds": ['cin reds', 'cincinnati reds',],
            },
    },
    # Cleveland Guardians
    "cleveland guardians": {
            "polymarket_code": "cle",
            "provider_aliases": {
                "kalstrop": ['cleveland guardians'],
                "boltodds": ['cle guardians', 'cleveland guardians',],
            },
    },
    # Colorado Rockies
    "colorado rockies": {
            "polymarket_code": "col",
            "provider_aliases": {
                "kalstrop": ['colorado rockies',],
                "boltodds": ['col rockies', 'colorado rockies',],
            },
    },
    # Detroit Tigers
    "detroit tigers": {
            "polymarket_code": "det",
            "provider_aliases": {
                "kalstrop": ['detroit tigers',],
                "boltodds": ['det tigers', 'detroit tigers',],
            },
    },
    # Houston Astros
    "houston astros": {
            "polymarket_code": "hou",
            "provider_aliases": {
                "kalstrop": ['houston astros',],
                "boltodds": ['hou astros', 'houston astros',],
            },
    },
    # Kansas City Royals
    "kansas city royals": {
            "polymarket_code": "kc",
            "provider_aliases": {
                "kalstrop": ['kansas city royals',],
                "boltodds": ['kansas city royals', 'kc royals',],
            },
    },
    # Los Angeles Angels
    "los angeles angels": {
            "polymarket_code": "laa",
            "provider_aliases": {
                "kalstrop": ['los angeles angels',],
                "boltodds": ['la angels', 'los angeles angels',],
            },
    },
    # Los Angeles Dodgers
    "los angeles dodgers": {
            "polymarket_code": "lad",
            "provider_aliases": {
                "kalstrop": ['los angeles dodgers',],
                "boltodds": ['la dodgers', 'los angeles dodgers',],
            },
    },
    # Miami Marlins
    "miami marlins": {
            "polymarket_code": "mia",
            "provider_aliases": {
                "kalstrop": ['miami marlins',],
                "boltodds": ['mia marlins', 'miami marlins',],
            },
    },
    # Milwaukee Brewers
    "milwaukee brewers": {
            "polymarket_code": "mil",
            "provider_aliases": {
                "kalstrop": ['milwaukee brewers',],
                "boltodds": ['mil brewers', 'milwaukee brewers',],
            },
    },
    # Minnesota Twins
    "minnesota twins": {
            "polymarket_code": "min",
            "provider_aliases": {
                "kalstrop": ['minnesota twins',],
                "boltodds": ['min twins', 'minnesota twins',],
            },
    },
    # New York Mets
    "new york mets": {
            "polymarket_code": "nym",
            "provider_aliases": {
                "kalstrop": ['new york mets',],
                "boltodds": [ 'new york mets', 'ny mets',],
            },
    },
    # New York Yankees
    "new york yankees": {
            "polymarket_code": "nyy",
            "provider_aliases": {
                "kalstrop": ['new york yankees',],
                "boltodds": ['new york yankees', 'ny yankees',],
            },
    },
    # Oakland Athletics (Kalshi uses "ATH" not "OAK")
    "oakland athletics": {
            "polymarket_code": "oak",
            "provider_aliases": {
                "kalstrop": ['athletics',],
                "boltodds": ['oakland athletics', 'athletics'],
            },
    },
    # Philadelphia Phillies
    "philadelphia phillies": {
            "polymarket_code": "phi",
            "provider_aliases": {
                "kalstrop": ['philadelphia phillies',],
                "boltodds": ['phi phillies', 'philadelphia phillies',],
            },
    },
    # Pittsburgh Pirates
    "pittsburgh pirates": {
            "polymarket_code": "pit",
            "provider_aliases": {
                "kalstrop": ['pittsburgh pirates',],
                "boltodds": ['pit pirates', 'pittsburgh pirates',],
            },
    },
    # San Diego Padres (Kalshi uses "SD" not "SDP")
    "san diego padres": {
            "polymarket_code": "sd",
            "provider_aliases": {
                "kalstrop": ['san diego padres',],
                "boltodds": ['san diego padres', 'sd padres',],
            },
    },
    # San Francisco Giants (Kalshi uses "SF" not "SFG")
    "san francisco giants": {
            "polymarket_code": "sf",
            "provider_aliases": {
                "kalstrop": ['san francisco giants',],
                "boltodds": ['san francisco giants', 'sf giants',],
            },
    },
    # Seattle Mariners
    "seattle mariners": {
            "polymarket_code": "sea",
            "provider_aliases": {
                "kalstrop": ['seattle mariners',],
                "boltodds": ['sea mariners', 'seattle mariners',],
            },
    },
    # St. Louis Cardinals
    "st. louis cardinals": {
            "polymarket_code": "stl",
            "provider_aliases": {
                "kalstrop": ['st. louis cardinals',],
                "boltodds": ['st louis cardinals', 'stl cardinals',],
            },
    },
    # Tampa Bay Rays
    "tampa bay rays": {
            "polymarket_code": "tb",
            "provider_aliases": {
                "kalstrop": ['tampa bay rays',],
                "boltodds": ['tampa bay rays', 'tb rays',],
            },
    },
    # Texas Rangers
    "texas rangers": {
            "polymarket_code": "tex",
            "provider_aliases": {
                "kalstrop": ['texas rangers',],
                "boltodds": ['tex rangers', 'texas rangers',],
            },
    },
    # Toronto Blue Jays
    "toronto blue jays": {
            "polymarket_code": "tor",
            "provider_aliases": {
                "kalstrop": ['toronto blue jays',],
                "boltodds": ['tor blue jays', 'toronto blue jays',],
            },
    },
    # Washington Nationals
    "washington nationals": {
            "polymarket_code": "wsh",
            "provider_aliases": {
                "kalstrop": ['washington nationals',],
                "boltodds": ['was nationals', 'washington nationals'],
            },
    },
}

PM_LEAGUE_ORDERINGS = {
    "mlb": "away",
    "bundesliga": "home",
}
LEAGUE_MATCH_RULES = {
    "mlb": {
        "date_tolerance_days": 0,
        "kickoff_tolerance_minutes": 180,
        "provider_order_reliable": False,
        "pm_order_reliable": True,
    }
}
TEAM_MAP = {
    "mlb": TEAM_MAP_MLB,
    "bundesliga": TEAM_MAP_BUN
}