# news_config.py
"""
Configuration constants for the TRACE News scraper.

This module contains only data constants — no classes, no functions, no scraping logic.
"""

from scrapers.reddit_config import TARGET_PLAYERS

# =============================================================================
# NEWS SOURCES
# =============================================================================

NEWS_SOURCES: dict[str, dict[str, str | bool | None]] = {
    "ESPN NBA": {
        "rss_url": "https://www.espn.com/espn/rss/nba/news",
        "fetch_full_article": True,
    },
    "CBS Sports NBA": {
        "rss_url": "https://www.cbssports.com/rss/headlines/nba",
        "fetch_full_article": True,
    },
    "Yahoo Sports NBA": {
        "rss_url": "https://sports.yahoo.com/nba/rss.xml",
        "fetch_full_article": False,
    },
    "NY Post NBA": {
        "rss_url": "https://nypost.com/nba/feed/",
        "fetch_full_article": True,
    },
    "RotoWire NBA": {
        "rss_url": "https://www.rotowire.com/rss/news.php?sport=NBA",
        "fetch_full_article": True,
    },
    "Hoops Rumors": {
        "rss_url": "https://hoopsrumors.com/feed",
        "fetch_full_article": True,
    },
}

# =============================================================================
# GOOGLE NEWS QUERY TEMPLATE
# =============================================================================

GOOGLE_NEWS_QUERY_TEMPLATE: str = (
    "https://www.google.com/search?q={player}+{injury_term}"
    "&tbm=nws&tbs=cdr:1,cd_min:{start_year},cd_end:{end_year}"
    "&siteordomain=espn.com&siteordomain=cbssports.com&siteordomain=theathletic.com"
    "&siteordomain=bleacherreport.com&siteordomain=si.com&siteordomain=usatoday.com"
    "&siteordomain=nbcsports.com"
)

# =============================================================================
# PLAYER INJURY WINDOWS
# =============================================================================

# Computed programmatically: (injury_year - 1, injury_year + 2)
PLAYER_INJURY_WINDOWS: dict[str, tuple[int, int]] = {}
for player_name, injury_date_str in TARGET_PLAYERS.items():
    injury_year = int(injury_date_str.split("-")[0])
    PLAYER_INJURY_WINDOWS[player_name] = (injury_year - 1, injury_year + 2)

# =============================================================================
# ARTICLE SEARCH QUERIES
# =============================================================================

ARTICLE_SEARCH_QUERIES: list[str] = []

# Player-specific queries (3 per player)
for player_name in TARGET_PLAYERS.keys():
    ARTICLE_SEARCH_QUERIES.append(f"{player_name} achilles")
    ARTICLE_SEARCH_QUERIES.append(f"{player_name} achilles injury return")
    ARTICLE_SEARCH_QUERIES.append(f"{player_name} injury recovery NBA")

# Player-agnostic queries
ARTICLE_SEARCH_QUERIES.extend([
    "NBA achilles injury recovery",
    "NBA achilles rupture return to play",
    "achilles tendon NBA surgery outcome",
    "NBA player achilles career",
])

# =============================================================================
# ARTICLE EXTRACTION SETTINGS
# =============================================================================

FULL_ARTICLE_TIMEOUT_SECONDS: int = 15

MIN_ARTICLE_WORD_COUNT: int = 50

# Lower threshold for RSS feeds since descriptions are short (can re-filter with LLM in Phase 2)
# Player name (5.0) + injury (2.0) = 7.0, so threshold of 7.0 catches player+injury combos
RSS_RELEVANCE_THRESHOLD: float = 1.0

# =============================================================================
# BROAD INJURY AND CONTEXT TERMS
# =============================================================================

# Broad injury-related terms for general filtering
BROAD_INJURY_TERMS: list[str] = [
    "injury",
    "injured",
    "out",
    "sidelined",
    "surgery",
    "torn",
    "tear",
    "rupture",
    "strain",
    "sprain",
    "tendon",
    "calf",
    "ankle",
    "knee",
    "leg",
    "operated",
    "procedure",
    "return",
    "recovery",
    "rehab",
    "questionable",
    "doubtful",
    "inactive",
    "missed games",
    "missed game",
    "medical",
    "clearance",
    "cleared",
]

# NBA context terms to ensure basketball relevance
NBA_CONTEXT_TERMS: list[str] = [
    "NBA",
    "basketball",
    "player",
    "team",
    "season",
    "game",
    "contract",
    "roster",
    "signing",
    "trade",
    "draft",
    "rookie",
    "veteran",
    "starter",
    "bench",
]

# =============================================================================
# NEWS SCRAPER SETTINGS
# =============================================================================

NEWS_SCRAPER_SETTINGS: dict[str, float | int | str] = {
    "request_delay_seconds": 2.0,
    "article_fetch_delay_seconds": 1.5,
    "max_articles_per_query": 50,
    "batch_size": 50,
    "checkpoint_dir": "data/news_checkpoints",
    "max_retries": 3,
    "retry_delay_seconds": 5.0,
}
