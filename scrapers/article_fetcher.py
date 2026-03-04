# article_fetcher.py
"""
Article fetching mechanics for TRACE News scraper.

This module handles only the mechanics of fetching article content from URLs —
no scraping orchestration, no relevance scoring, no database interaction.
"""

from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from newspaper import Article
from newspaper.article import ArticleException

from scrapers.news_config import (
    FULL_ARTICLE_TIMEOUT_SECONDS,
    MIN_ARTICLE_WORD_COUNT,
    NEWS_SCRAPER_SETTINGS,
)


class TRACEArticleFetcher:
    """
    Fetches and parses article content from RSS feeds and URLs.

    Handles full article body extraction using newspaper3k and RSS feed
    parsing using BeautifulSoup. Tracks success/failure statistics.
    """

    def __init__(self) -> None:
        """
        Initialize the article fetcher with timeout, session, and counters.
        """
        self.timeout = FULL_ARTICLE_TIMEOUT_SECONDS
        self.min_word_count = MIN_ARTICLE_WORD_COUNT

        # Set up requests session with realistic browser User-Agent
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        })

        # Counters for tracking fetch statistics
        self._success_count = 0
        self._failure_count = 0

    def fetch_full_article(self, url: str) -> dict:
        """
        Fetch and extract full article content from a URL.

        Args:
            url: The URL of the article to fetch.

        Returns:
            Dictionary with four keys:
            - "text": Extracted body text as a string.
            - "word_count": Integer count of words in the extracted text.
            - "fetch_success": Boolean indicating if extraction met quality threshold.
            - "authors": List of author name strings.
        """
        try:
            # Create and configure newspaper Article
            article = Article(url, timeout=self.timeout)
            article.download()
            article.parse()

            # Extract text and compute word count
            text = article.text or ""
            word_count = len(text.split())

            # Check if extraction meets minimum quality threshold
            if word_count >= self.min_word_count:
                self._success_count += 1
                return {
                    "text": text,
                    "word_count": word_count,
                    "fetch_success": True,
                    "authors": article.authors or [],
                }
            else:
                # Below minimum word count - treat as failed extraction
                self._failure_count += 1
                return {
                    "text": "",
                    "word_count": 0,
                    "fetch_success": False,
                    "authors": article.authors or [],
                }

        except (ArticleException, requests.RequestException, Exception):
            # Any error - return failed result
            self._failure_count += 1
            return {
                "text": "",
                "word_count": 0,
                "fetch_success": False,
                "authors": [],
            }

    def fetch_rss_feed(self, source_name: str, rss_url: str) -> list[dict]:
        """
        Fetch and parse an RSS feed.

        Args:
            source_name: Human-readable name of the news source.
            rss_url: The RSS feed URL to fetch.

        Returns:
            List of dictionaries, each representing one RSS item with:
            - "title": Article title string.
            - "url": Article URL string.
            - "pub_date_str": Raw publication date string from feed (or "").
            - "description": Summary text from RSS item (or "").
            - "source_name": The source name argument.

            Returns empty list if fetch fails entirely.
        """
        try:
            # Fetch RSS XML
            response = self.session.get(rss_url, timeout=self.timeout)
            response.raise_for_status()

            # Parse with BeautifulSoup using xml parser
            soup = BeautifulSoup(response.content, "xml")

            # Find all RSS items (item is standard RSS 2.0 element)
            items = soup.find_all("item")

            results = []
            for item in items:
                # Extract title
                title_elem = item.find("title")
                title = title_elem.get_text(strip=True) if title_elem else ""

                # Extract URL (link element)
                link_elem = item.find("link")
                url = link_elem.get_text(strip=True) if link_elem else ""

                # Extract publication date (pubDate is standard)
                pub_date_elem = item.find("pubDate")
                pub_date_str = pub_date_elem.get_text(strip=True) if pub_date_elem else ""

                # Extract description/summary
                desc_elem = item.find("description")
                description = desc_elem.get_text(strip=True) if desc_elem else ""

                results.append({
                    "title": title,
                    "url": url,
                    "pub_date_str": pub_date_str,
                    "description": description,
                    "source_name": source_name,
                })

            return results

        except (requests.RequestException, Exception) as e:
            print(f"⚠️  Failed to fetch RSS feed for {source_name}: {e}")
            return []

    def parse_pub_date(self, raw_date_str: str) -> datetime:
        """
        Parse a raw RSS date string into a Python datetime object.

        Args:
            raw_date_str: Raw date string from RSS feed.

        Returns:
            Parsed datetime object. If no format matches, returns current
            datetime as a fallback with a warning printed.
        """
        if not raw_date_str:
            print(f"⚠️  Empty date string, using current datetime as fallback")
            return datetime.now()

        # Common RSS date formats to try in sequence
        formats = [
            # RFC 822 / RFC 2822 (most common in RSS)
            "%a, %d %b %Y %H:%M:%S %z",  # Mon, 01 Jan 2024 12:00:00 +0000
            "%a, %d %b %Y %H:%M:%S %Z",  # Mon, 01 Jan 2024 12:00:00 GMT
            "%a, %d %b %Y %H:%M:%S",     # Mon, 01 Jan 2024 12:00:00
            # ISO 8601 variants
            "%Y-%m-%dT%H:%M:%S%z",       # 2024-01-01T12:00:00+00:00
            "%Y-%m-%dT%H:%M:%SZ",        # 2024-01-01T12:00:00Z
            "%Y-%m-%dT%H:%M:%S",         # 2024-01-01T12:00:00
            # Simple date formats
            "%Y-%m-%d %H:%M:%S",         # 2024-01-01 12:00:00
            "%Y-%m-%d",                  # 2024-01-01
            "%m/%d/%Y",                  # 01/01/2024
            "%d %b %Y",                  # 01 Jan 2024
            "%B %d, %Y",                 # January 01, 2024
        ]

        for fmt in formats:
            try:
                return datetime.strptime(raw_date_str, fmt)
            except ValueError:
                continue

        # No format matched - use current datetime as fallback
        print(f"⚠️  Could not parse date '{raw_date_str}', using current datetime as fallback")
        return datetime.now()

    def get_fetch_stats(self) -> dict:
        """
        Get statistics on article fetch success and failure counts.

        Returns:
            Dictionary with:
            - "success_count": Number of successful full article fetches.
            - "failure_count": Number of failed fetches (fell back to title-only).
            - "success_rate": Float between 0 and 1 representing overall success rate.
        """
        total = self._success_count + self._failure_count
        success_rate = self._success_count / total if total > 0 else 0.0

        return {
            "success_count": self._success_count,
            "failure_count": self._failure_count,
            "success_rate": success_rate,
        }
