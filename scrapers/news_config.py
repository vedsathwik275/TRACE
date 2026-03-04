# news_config.py
"""
Configuration constants for the TRACE News scraper.

This module contains only data constants — no classes, no functions, no scraping logic.
"""

# =============================================================================
# NEWS SOURCES (10 total)
# =============================================================================

NEWS_SOURCES: dict[str, dict[str, str | bool | None]] = {
    # Original 6 sources
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
    # New 4 sources
    "RealGM Wiretap": {
        "rss_url": "https://basketball.realgm.com/rss/realgm_nba_wiretap.xml",
        "fetch_full_article": True,
    },
    "Sportsnet NBA": {
        "rss_url": "https://www.sportsnet.ca/basketball/nba/feed/",
        "fetch_full_article": True,
    },
    "ClutchPoints NBA": {
        "rss_url": "https://clutchpoints.com/sports/nba/feed",
        "fetch_full_article": True,
    },
    "The Cold Wire NBA": {
        "rss_url": "https://thecoldwire.com/sports/nba/feed",
        "fetch_full_article": True,
    },
}

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
