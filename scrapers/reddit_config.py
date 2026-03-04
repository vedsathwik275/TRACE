# reddit_config.py
"""
Configuration constants for the TRACE Reddit scraper.

This module contains only data constants — no classes, no functions, no scraping logic.
"""

# =============================================================================
# TARGET PLAYERS: Confirmed Achilles Injury Cases
# =============================================================================

TARGET_PLAYERS: dict[str, str] = {
    "Kevin Durant": "2019-05-13",
    "Klay Thompson": "2019-06-13",
    "DeMarcus Cousins": "2018-01-26",
    "John Wall": "2019-01-25",
    "Wesley Matthews": "2015-03-05",
    "Rudy Gay": "2017-01-28",
    "Rodney Hood": "2019-04-20",
    "Jeremy Lin": "2017-08-26",
    "Brandon Jennings": "2015-11-24",
    "Danilo Gallinari": "2021-08-07",
    "Bogdan Bogdanovic": "2020-10-19",
    "Chandler Parsons": "2018-01-04",
    "Brook Lopez": "2018-03-29",
    "Marcus Smart": "2022-05-27",
    "Gordon Hayward": "2017-10-17",
}

# =============================================================================
# ACHILLES SEARCH QUERIES
# =============================================================================

ACHILLES_SEARCH_QUERIES: list[str] = [
    # Player-specific queries
    "Kevin Durant achilles",
    "Klay Thompson achilles",
    "DeMarcus Cousins achilles",
    "John Wall achilles",
    "Wesley Matthews achilles",
    "Rudy Gay achilles",
    "Rodney Hood achilles",
    "Jeremy Lin achilles",
    "Brandon Jennings achilles",
    "Danilo Gallinari achilles",
    "Bogdan Bogdanovic achilles",
    "Chandler Parsons achilles",
    "Brook Lopez achilles",
    "Marcus Smart achilles",
    "Gordon Hayward achilles",
    # Standalone queries
    "NBA achilles tear",
    "NBA achilles rupture",
    "achilles tendon NBA",
    "ruptured achilles NBA",
    "torn achilles NBA",
    "achilles surgery NBA",
    "NBA achilles recovery",
    "NBA achilles rehab",
    "NBA achilles return",
    "NBA achilles comeback",
]

# =============================================================================
# SUBREDDIT CONFIGURATIONS
# =============================================================================

SUBREDDITS_PRIMARY: list[str] = [
    "nba",
    "nbadiscussion",
]

SUBREDDITS_TEAM: dict[str, str] = {
    # Eastern Conference - Atlantic Division
    "Boston Celtics": "celtics",
    "Brooklyn Nets": "netstrade",
    "New York Knicks": "nyknicks",
    "Philadelphia 76ers": "sixers",
    "Toronto Raptors": "torontoraptors",
    # Eastern Conference - Central Division
    "Chicago Bulls": "chicagobulls",
    "Cleveland Cavaliers": "clevelandcavs",
    "Detroit Pistons": "detroitpistons",
    "Indiana Pacers": "pacers",
    "Milwaukee Bucks": "mkebucks",
    # Eastern Conference - Southeast Division
    "Atlanta Hawks": "atlantahawks",
    "Charlotte Hornets": "hornets",
    "Miami Heat": "heat",
    "Orlando Magic": "orlandomagic",
    "Washington Wizards": "washingtonwizards",
    # Western Conference - Northwest Division
    "Denver Nuggets": "denvernuggets",
    "Minnesota Timberwolves": "timberwolves",
    "Oklahoma City Thunder": "thunder",
    "Portland Trail Blazers": "ripcity",
    "Utah Jazz": "utahjazz",
    # Western Conference - Pacific Division
    "Golden State Warriors": "warriors",
    "LA Clippers": "clippers",
    "Los Angeles Lakers": "lakers",
    "Phoenix Suns": "suns",
    "Sacramento Kings": "kings",
    # Western Conference - Southwest Division
    "Dallas Mavericks": "mavericks",
    "Houston Rockets": "rockets",
    "Memphis Grizzlies": "memesgrizz",
    "New Orleans Pelicans": "pelicans",
    "San Antonio Spurs": "nbaspurs",
}

SUBREDDITS_SPECIALTY: list[str] = [
    "fantasybball",
    "nbanalytics",
    "nba_draft",
    "Basketball",
]

# =============================================================================
# KEYWORD WEIGHTS FOR RELEVANCE SCORING
# =============================================================================

KEYWORD_WEIGHTS: dict[str, dict[str, float | list[str]]] = {
    # High-weight Achilles-specific phrases (10.0 points each)
    "achilles_terms": {
        "weight": 10.0,
        "terms": [
            "achilles tear",
            "achilles rupture",
            "achilles tendon",
            "ruptured achilles",
            "torn achilles",
            "achilles surgery",
            "achilles repair",
            "achilles recovery",
            "achilles rehab",
            "achilles reinjury",
            "achilles tendinopathy",
        ],
    },
    # Single "achilles" word for headlines (5.0 points)
    "achilles_single": {
        "weight": 5.0,
        "terms": [
            "achilles",
            "achilles injury",
        ],
    },
    # Lower leg injury phrases (3.0 points)
    "lower_leg_terms": {
        "weight": 3.0,
        "terms": [
            "calf strain",
            "plantaris rupture",
            "posterior tibial",
            "peroneal tendon",
            "soleus strain",
            "gastrocnemius",
        ],
    },
    # Single lower leg words (2.0 points)
    "lower_leg_single": {
        "weight": 2.0,
        "terms": [
            "calf",
            "ankle",
            "foot",
            "lower leg",
        ],
    },
    # Recovery phase terms (2.0 points)
    "recovery_terms": {
        "weight": 2.0,
        "terms": [
            "surgery",
            "rehab",
            "recovery",
            "return",
            "comeback",
            "timeline",
            "setback",
            "progress",
            "cleared to play",
            "physical therapy",
            "ahead of schedule",
            "behind schedule",
        ],
    },
    # General injury terms for RSS headlines (2.0 points)
    "injury_general": {
        "weight": 2.0,
        "terms": [
            "injury",
            "injured",
            "out",
            "sidelined",
            "day-to-day",
            "DTD",
            "DNP",
            "inactive",
        ],
    },
    # Medical/professional terms (2.0 points)
    "medical_terms": {
        "weight": 2.0,
        "terms": [
            "MRI",
            "orthopedic",
            "surgeon",
            "tendon",
            "rupture",
            "injury report",
            "imaging",
        ],
    },
}

# =============================================================================
# PLAYER ALIASES
# =============================================================================

PLAYER_ALIASES: dict[str, str] = {
    "KD": "Kevin Durant",
    "Klay": "Klay Thompson",
    "Boogie": "DeMarcus Cousins",
    "Wall": "John Wall",
    "Hayward": "Gordon Hayward",
    "Parsons": "Chandler Parsons",
}

# =============================================================================
# RELEVANCE THRESHOLDS
# =============================================================================

HYPER_RELEVANCE_THRESHOLD: float = 15.0

# =============================================================================
# DATE RANGES FOR SEARCH
# =============================================================================

DATE_RANGES: list[tuple[str, str]] = [
    ("2015-01-01", "2016-12-31"),
    ("2017-01-01", "2018-12-31"),
    ("2019-01-01", "2020-12-31"),
    ("2021-01-01", "2022-12-31"),
    ("2023-01-01", "2026-03-01"),
]

# =============================================================================
# SCRAPER SETTINGS
# =============================================================================

SCRAPER_SETTINGS: dict[str, float | int | str] = {
    "post_delay_seconds": 0.1,
    "subreddit_delay_seconds": 0.5,
    "search_delay_seconds": 1.0,
    "max_posts_per_query": 100,
    "max_comments_per_post": 30,
    "min_post_score": 3,
    "batch_size": 50,
    "checkpoint_dir": "data/checkpoints",
}
