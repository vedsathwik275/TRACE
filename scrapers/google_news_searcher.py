# google_news_searcher.py
"""
Google News search for historical article URL discovery.

This module handles only the discovery of historical article URLs via Google News —
it finds URLs but does not fetch article content itself.

NOTE: Google News scraping is subject to rate limiting and IP blocking. Callers
should use conservative delays between requests. If Google News blocking becomes
a problem during development, the _execute_search method can be stubbed to return
empty lists and the rest of the pipeline will still function correctly using
RSS-sourced data alone.
"""

import time
import re
from datetime import datetime
from urllib.parse import quote_plus
from typing import Optional

import requests
from bs4 import BeautifulSoup

from scrapers.news_config import (
    NEWS_SCRAPER_SETTINGS,
    GOOGLE_NEWS_QUERY_TEMPLATE,
    PLAYER_INJURY_WINDOWS,
    ARTICLE_SEARCH_QUERIES,
)
from scrapers.reddit_config import TARGET_PLAYERS
from scrapers.article_fetcher import TRACEArticleFetcher


class TRACEGoogleNewsSearcher:
    """
    Discovers historical article URLs via Google News search.

    Searches Google News archive for player-specific injury articles within
    defined coverage windows. Deduplicates results and respects rate limits.
    """

    def __init__(self) -> None:
        """
        Initialize the searcher with configuration and URL tracking set.
        """
        self.news_settings = NEWS_SCRAPER_SETTINGS
        self.query_template = GOOGLE_NEWS_QUERY_TEMPLATE
        self.player_windows = PLAYER_INJURY_WINDOWS
        self.article_queries = ARTICLE_SEARCH_QUERIES
        self.target_players = TARGET_PLAYERS

        # Use fetcher's session for HTTP requests
        self.fetcher = TRACEArticleFetcher()
        self.session = self.fetcher.session

        # Track seen URLs to prevent duplicates across calls
        self._seen_urls: set[str] = set()

    def build_search_url(self, query: str, start_year: int, end_year: int) -> str:
        """
        Construct a Google News search URL for the given query and date range.

        Args:
            query: The search query string.
            start_year: Start year for date restriction.
            end_year: End year for date restriction.

        Returns:
            Complete Google News search URL suitable for direct GET requests.
        """
        # URL-encode the query
        encoded_query = quote_plus(query)

        # Build URL using template
        url = self.query_template.format(
            player=encoded_query,
            injury_term="achilles",
            start_year=start_year,
            end_year=end_year,
        )

        return url

    def search_for_player(self, player_name: str) -> list[dict]:
        """
        Search for all articles related to a specific player's Achilles injury.

        Args:
            player_name: Full player name as it appears in TARGET_PLAYERS.

        Returns:
            Flat list of article metadata dictionaries, deduplicated by URL.
        """
        if player_name not in self.player_windows:
            print(f"⚠️  Unknown player: {player_name}")
            return []

        # Get player's coverage window
        start_year, end_year = self.player_windows[player_name]
        years = list(range(start_year, end_year + 1))

        # Get the three player-specific queries
        # They are stored consecutively in ARTICLE_SEARCH_QUERIES
        player_query_base = f"{player_name} achilles"
        player_queries = [
            f"{player_name} achilles",
            f"{player_name} achilles injury return",
            f"{player_name} injury recovery NBA",
        ]

        all_results: list[dict] = []

        for query in player_queries:
            for year in years:
                results = self._execute_search(query, year)
                all_results.extend(results)

                # Sleep between searches
                time.sleep(self.news_settings["request_delay_seconds"])

        # Deduplicate by URL
        seen: set[str] = set()
        deduped: list[dict] = []
        for article in all_results:
            if article["url"] not in seen:
                seen.add(article["url"])
                deduped.append(article)

        return deduped

    def _execute_search(self, query: str, year: int) -> list[dict]:
        """
        Execute a Google News search for a specific query and year.

        Args:
            query: The search query string.
            year: The year to search (one-year window: Jan 1 - Dec 31).

        Returns:
            List of article metadata dictionaries with:
            - "title": Article title string.
            - "url": Article URL string.
            - "pub_date_str": Approximate publication date string.
            - "source_name": Domain of the source URL.

            Returns empty list if Google returns no results or blocks the request.
        """
        # Build URL for one-year window
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        # Construct Google News search URL
        encoded_query = quote_plus(query)
        url = (
            f"https://www.google.com/search?q={encoded_query}"
            f"&tbm=nws&tbs=cdr:1,cd_min:{start_date},cd_max:{end_date}"
        )

        try:
            # Make GET request
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            }

            response = self.session.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            # Check if Google is blocking us (captcha, redirect to consent page, etc.)
            if "captcha" in response.text.lower() or "consent" in response.text.lower():
                print(f"⚠️  Google may be blocking requests (captcha/consent detected)")
                return []

            # Parse HTML
            soup = BeautifulSoup(response.content, "html.parser")

            # Google News results are in div elements with specific classes
            # Note: These selectors may need updating if Google changes their HTML structure
            results = []
            max_articles = self.news_settings["max_articles_per_query"]

            # Try to find article result containers
            # Google uses various class names; we'll look for common patterns
            article_divs = soup.find_all("div", class_=lambda x: x and ("g" in x or "result" in x.lower()))

            for div in article_divs[:max_articles]:
                # Extract title
                title_elem = div.find("h3")
                if not title_elem:
                    continue
                title = title_elem.get_text(strip=True)

                # Extract URL
                link_elem = div.find("a", href=True)
                if not link_elem:
                    continue
                article_url = link_elem["href"]

                # Skip already-seen URLs
                if article_url in self._seen_urls:
                    continue

                # Extract source name from URL domain
                domain = self._extract_domain(article_url)

                # Try to find date (often in span with relative time)
                date_elem = div.find("span", string=re.compile(r"\d+ (hour|day|week|month|year)s? ago"))
                if date_elem:
                    pub_date_str = date_elem.get_text(strip=True)
                else:
                    # Look for any date-like text
                    date_span = div.find("span", class_=lambda x: x and "date" in x.lower())
                    pub_date_str = date_span.get_text(strip=True) if date_span else f"{year}-01-01"

                # Add to seen URLs
                self._seen_urls.add(article_url)

                results.append({
                    "title": title,
                    "url": article_url,
                    "pub_date_str": pub_date_str,
                    "source_name": domain,
                })

                if len(results) >= max_articles:
                    break

            return results

        except requests.RequestException as e:
            print(f"⚠️  Google News search failed for '{query}' ({year}): {e}")
            return []
        except Exception as e:
            print(f"⚠️  Unexpected error parsing Google News results for '{query}' ({year}): {e}")
            return []

    def _extract_domain(self, url: str) -> str:
        """
        Extract the domain name from a URL.

        Args:
            url: Full URL string.

        Returns:
            Domain name (e.g., "espn.com" from "https://www.espn.com/nba/story").
        """
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc
            # Remove www. prefix for cleaner source names
            if domain.startswith("www."):
                domain = domain[4:]
            return domain if domain else "unknown"
        except Exception:
            return "unknown"

    def search_all_players(self) -> list[dict]:
        """
        Search for articles for all players in TARGET_PLAYERS.

        Returns:
            Combined list of all article metadata dictionaries found.
        """
        all_results: list[dict] = []

        print(f"🔍 Starting Google News search for {len(self.target_players)} players...")

        for i, player_name in enumerate(self.target_players.keys(), 1):
            print(f"\n[{i}/{len(self.target_players)}] Searching for: {player_name}")

            results = self.search_for_player(player_name)
            all_results.extend(results)

            total_so_far = len(all_results)
            print(f"  ✅ Found {len(results)} articles for {player_name} (total: {total_so_far})")

        print(f"\n📊 Google News search complete: {len(all_results)} total article URLs discovered")
        return all_results
