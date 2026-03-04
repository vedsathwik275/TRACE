# news_scraper_v2.py
"""
TRACE News Scraper V2 - Historical news article collection.

Collects articles from RSS feeds and Google News RSS search, fetches full
article content, and applies relevance scoring. Produces output conforming
to the 27-column unified schema.
"""

import json
import time
from datetime import datetime
from typing import Optional
from urllib.parse import quote_plus

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scrapers.news_config import (
    NEWS_SOURCES,
    NEWS_SCRAPER_SETTINGS,
    MIN_ARTICLE_WORD_COUNT,
    RSS_RELEVANCE_THRESHOLD,
    BROAD_INJURY_TERMS,
)
from scrapers.reddit_config import TARGET_PLAYERS, KEYWORD_WEIGHTS, HYPER_RELEVANCE_THRESHOLD
from scrapers.article_fetcher import TRACEArticleFetcher
from scrapers.relevance_scorer import TRACERelevanceScorer
from scrapers.checkpoint_manager import TRACECheckpointManager


class TRACENewsScraperV2:
    """
    Historical news scraper for NBA Achilles injury content.

    Collects articles from RSS feeds, fetches full article content, and applies
    relevance scoring.
    """

    def __init__(self) -> None:
        """
        Initialize the news scraper with fetcher, scorer, and checkpoint manager.
        """
        self.fetcher = TRACEArticleFetcher()
        self.scorer = TRACERelevanceScorer()
        self.checkpoint = TRACECheckpointManager(
            checkpoint_dir=NEWS_SCRAPER_SETTINGS["checkpoint_dir"]
        )
        self.seen_urls: set[str] = set()

    def _build_record(
        self,
        title: str,
        url: str,
        source_name: str,
        pub_datetime: datetime,
        body_text: str,
        fetch_success: bool,
        authors: list[str],
        score: float,
        keywords: list[str],
    ) -> dict:
        """
        Build a complete 27-column record dictionary matching the unified schema.

        Args:
            title: Article title string.
            url: Article URL string.
            source_name: Human-readable source name.
            pub_datetime: Publication datetime object.
            body_text: Full article body text (may be empty if fetch failed).
            fetch_success: Boolean indicating if full article extraction succeeded.
            authors: List of author name strings.
            score: Computed relevance score.
            keywords: List of matched keyword strings.

        Returns:
            Complete record dictionary with all 27 columns.
        """
        # Combine title and body for analysis
        combined_text = f"{title} {body_text}"

        # Check if achilles-related
        achilles_terms = KEYWORD_WEIGHTS["achilles_terms"]["terms"]
        is_achilles_related = any(
            term.lower() in combined_text.lower() for term in achilles_terms
        )

        # Detect recovery phase
        recovery_phase = self.scorer.detect_recovery_phase(combined_text)

        # Extract mentioned players
        mentioned_players = self.scorer.extract_players(combined_text)

        # Format datetime
        created_date = pub_datetime.strftime("%Y-%m-%dT%H:%M:%S")

        # Build record matching 27-column unified schema
        record = {
            "source_platform": "News",
            "source_detail": source_name,
            "author": ", ".join(authors) if authors else "",
            "url": url,
            "text_content": combined_text.strip(),
            "created_date": created_date,
            "engagement_score": 0.0,
            "engagement_secondary": 0.0,
            "engagement_tier": "medium",
            "relevance_score": float(score),
            "recovery_phase": recovery_phase,
            "mentioned_players": json.dumps(mentioned_players),
            "is_achilles_related": is_achilles_related,
            "is_quality_content": fetch_success and len(body_text.split()) >= MIN_ARTICLE_WORD_COUNT,
            "text_length": len(combined_text.strip()),
            "year": pub_datetime.year,
            "month": pub_datetime.month,
            "year_month": pub_datetime.strftime("%Y-%m"),
            "num_comments_extracted": 0,
            "avg_comment_score": 0.0,
            "total_comment_words": 0,
            "num_replies_extracted": 0,
            "avg_reply_likes": 0.0,
            "total_reply_words": 0,
            "body_word_count": len(body_text.split()),
            "fetch_success": fetch_success,
            "uploaded_at": None,
        }

        return record

    def _process_article_url(
        self,
        title: str,
        url: str,
        source_name: str,
        pub_date_str: str,
        description: str = "",
        threshold: float = None,
        is_rss_source: bool = False,
        debug_mode: bool = False,
    ) -> Optional[dict]:
        """
        Process a single article URL through scoring and fetching.

        Args:
            title: Article title string.
            url: Article URL string.
            source_name: Human-readable source name.
            pub_date_str: Raw publication date string from RSS feed.
            description: Optional description/summary text from RSS item.
            threshold: Relevance threshold to apply (default: HYPER_RELEVANCE_THRESHOLD).
            is_rss_source: If True, use RSS-specific scoring before full fetch.
            debug_mode: If True, print filtered items with their scores.

        Returns:
            Complete record dictionary if article passes relevance threshold,
            None otherwise or if URL is a duplicate.
        """
        # Use provided threshold or default
        if threshold is None:
            threshold = RSS_RELEVANCE_THRESHOLD if is_rss_source else HYPER_RELEVANCE_THRESHOLD

        # Check for duplicate URL
        if url in self.seen_urls:
            return None

        # Add URL to seen set immediately (before any scoring or filtering)
        self.seen_urls.add(url)

        # For RSS sources, use compute_score_rss initially (short description scoring)
        if is_rss_source:
            score, keywords = self.scorer.compute_score_rss(title, description or "")
        else:
            score, keywords = self.scorer.compute_score(title, description or "")

        # Filter by relevance threshold
        if score < threshold:
            if debug_mode:
                print(f"   ⏭️  FILTERED (score={score:.1f}): {title[:60]}...")
            return None

        # Parse publication date
        pub_datetime = self.fetcher.parse_pub_date(pub_date_str)

        # Check if source has full article fetching enabled
        # For Google News sources, always attempt full article fetch
        fetch_full = False
        if source_name.startswith("Google News"):
            fetch_full = True
        elif source_name in NEWS_SOURCES:
            fetch_full = NEWS_SOURCES[source_name].get("fetch_full_article", False)

        # Attempt to fetch full article
        body_text = ""
        fetch_success = False
        authors: list[str] = []

        if fetch_full:
            article_data = self.fetcher.fetch_full_article(url)
            body_text = article_data["text"]
            fetch_success = article_data["fetch_success"]
            authors = article_data["authors"]

            # Sleep after fetch attempt
            time.sleep(NEWS_SCRAPER_SETTINGS["article_fetch_delay_seconds"])

        # Fall back to title + description if fetch failed
        if not fetch_success:
            body_text = f"{title} {description}" if description else title

        # Recompute score with full body if fetch succeeded
        if fetch_success and body_text:
            score, keywords = self.scorer.compute_score(title, body_text)
            # After full article extraction, keep record if is_broadly_relevant returns True
            if not self.scorer.is_broadly_relevant(title, body_text):
                if debug_mode:
                    print(f"   ⏭️  FILTERED (not broadly relevant after fetch): {title[:60]}...")
                return None

        # Build and return record
        return self._build_record(
            title=title,
            url=url,
            source_name=source_name,
            pub_datetime=pub_datetime,
            body_text=body_text,
            fetch_success=fetch_success,
            authors=authors,
            score=score,
            keywords=keywords,
        )

    def scrape_rss_sources(self, debug_mode: bool = False) -> list[dict]:
        """
        Scrape all configured RSS sources.

        Args:
            debug_mode: If True, print title and score of every filtered item.

        Returns:
            List of record dictionaries for articles passing relevance filter.
        """
        all_records: list[dict] = []

        print("\n" + "=" * 60)
        print("📰 STAGE 1: RSS Feed Collection")
        print("=" * 60)

        if debug_mode:
            print("🔍 DEBUG MODE: Printing all filtered items\n")

        for source_name, config in NEWS_SOURCES.items():
            rss_url = config.get("rss_url")

            # Skip sources without valid RSS URL
            if not rss_url:
                print(f"⏭️  Skipping {source_name} (no RSS URL)")
                continue

            print(f"\n🔍 Fetching RSS: {source_name}")

            # Fetch RSS feed
            items = self.fetcher.fetch_rss_feed(source_name, rss_url)

            if not items:
                print(f"   No items found in {source_name} RSS feed")
                continue

            # Process each item with RSS-specific threshold
            passed = 0
            filtered = 0
            for item in items:
                record = self._process_article_url(
                    title=item["title"],
                    url=item["url"],
                    source_name=item["source_name"],
                    pub_date_str=item["pub_date_str"],
                    description=item["description"],
                    threshold=RSS_RELEVANCE_THRESHOLD,
                    is_rss_source=True,
                    debug_mode=debug_mode,
                )

                if record is not None:
                    all_records.append(record)
                    passed += 1
                else:
                    filtered += 1

            print(f"   ✅ {passed}/{len(items)} articles passed relevance filter")
            if debug_mode and filtered > 0:
                print(f"   ⏭️  {filtered} items filtered")

            # Sleep between sources
            time.sleep(NEWS_SCRAPER_SETTINGS["request_delay_seconds"])

        # Checkpoint all RSS results
        if all_records:
            self.checkpoint.save_records_batch(all_records)

        # Print stage summary
        print(f"\n📊 Stage 1 Summary: {len(all_records)} total records from RSS")

        # Breakdown by source
        source_counts: dict[str, int] = {}
        for record in all_records:
            source = record["source_detail"]
            source_counts[source] = source_counts.get(source, 0) + 1

        if source_counts:
            print("   Records by source:")
            for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
                print(f"      • {source}: {count}")

        return all_records

    def scrape_rss_sources_broad(self, debug_mode: bool = False) -> list[dict]:
        """
        Scrape all configured RSS sources using broad RSS-specific scoring.

        Uses compute_score_rss for scoring (optimized for short descriptions)
        and RSS_RELEVANCE_THRESHOLD (1.0) for filtering to collect a broader
        range of NBA injury-related content.

        Args:
            debug_mode: If True, print title and score of every filtered item.

        Returns:
            List of record dictionaries for articles passing relevance filter.
        """
        all_records: list[dict] = []

        print("\n" + "=" * 60)
        print("📰 STAGE 1: RSS Feed Collection (Broad)")
        print("=" * 60)

        if debug_mode:
            print("🔍 DEBUG MODE: Printing all filtered items\n")

        for source_name, config in NEWS_SOURCES.items():
            rss_url = config.get("rss_url")

            # Skip sources without valid RSS URL
            if not rss_url:
                print(f"⏭️  Skipping {source_name} (no RSS URL)")
                continue

            print(f"\n🔍 Fetching RSS: {source_name}")

            # Fetch RSS feed
            items = self.fetcher.fetch_rss_feed(source_name, rss_url)

            if not items:
                print(f"   No items found in {source_name} RSS feed")
                continue

            # Process each item with RSS-specific scoring and threshold
            passed = 0
            filtered = 0
            for item in items:
                # Use compute_score_rss directly for scoring
                score, keywords = self.scorer.compute_score_rss(
                    item["title"],
                    item["description"] or ""
                )

                # Filter by RSS threshold
                if score < RSS_RELEVANCE_THRESHOLD:
                    if debug_mode:
                        print(f"   ⏭️  FILTERED (RSS score={score:.1f}): {item['title'][:60]}...")
                    filtered += 1
                    continue

                # Process the URL (will re-score with full text if fetch succeeds)
                record = self._process_article_url(
                    title=item["title"],
                    url=item["url"],
                    source_name=item["source_name"],
                    pub_date_str=item["pub_date_str"],
                    description=item["description"],
                    threshold=RSS_RELEVANCE_THRESHOLD,
                    is_rss_source=True,
                    debug_mode=debug_mode,
                )

                if record is not None:
                    all_records.append(record)
                    passed += 1
                else:
                    filtered += 1

            print(f"   ✅ {passed}/{len(items)} articles passed relevance filter")
            if debug_mode and filtered > 0:
                print(f"   ⏭️  {filtered} items filtered")

            # Sleep between sources
            time.sleep(NEWS_SCRAPER_SETTINGS["request_delay_seconds"])

        # Checkpoint all RSS results
        if all_records:
            self.checkpoint.save_records_batch(all_records)

        # Print stage summary
        print(f"\n📊 Stage 1 Summary: {len(all_records)} total records from RSS")

        # Breakdown by source
        source_counts: dict[str, int] = {}
        for record in all_records:
            source = record["source_detail"]
            source_counts[source] = source_counts.get(source, 0) + 1

        if source_counts:
            print("   Records by source:")
            for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
                print(f"      • {source}: {count}")

        return all_records

    def scrape_google_news_rss(self, debug_mode: bool = False) -> list[dict]:
        """
        Stage 2: Query Google News RSS for each player+injury combination.

        For each of the 15 TARGET_PLAYERS, constructs 3–5 search queries covering
        different angles of their Achilles injury story, fetches the Google News RSS
        feed for each query, processes each result through the existing scoring and
        fetching pipeline, and returns deduplicated records.

        Args:
            debug_mode: If True, print filtered items with their scores.

        Returns:
            List of record dicts conforming to the 27-column unified schema.
        """
        all_records: list[dict] = []

        print("\n" + "=" * 60)
        print("📰 STAGE 2: Google News RSS Historical Search")
        print("=" * 60)

        if debug_mode:
            print("🔍 DEBUG MODE: Printing all filtered items\n")

        # Google News RSS base URL
        GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

        # Build player-specific queries (4 per player)
        player_queries: list[tuple[str, str]] = []  # (query, player_name for logging)
        for player_name in TARGET_PLAYERS.keys():
            player_queries.append((f'"{player_name}" achilles injury NBA', player_name))
            player_queries.append((f'"{player_name}" achilles surgery recovery', player_name))
            player_queries.append((f'"{player_name}" achilles return NBA', player_name))
            player_queries.append((f'"{player_name}" achilles rehab timeline', player_name))

        # Generic achilles queries (one per year 2015-2024)
        for year in range(2015, 2025):
            player_queries.append((f"NBA achilles injury {year}", f"Year {year}"))

        # Non-player-specific Google News queries
        generic_queries = [
            "NBA achilles rupture career",
            "NBA achilles tendon surgery recovery",
            "NBA achilles injury return timeline",
            "basketball achilles tear comeback",
        ]

        # Combine all queries
        all_queries = player_queries + [(q, "Generic") for q in generic_queries]

        total_queries = len(all_queries)
        records_collected = 0

        for i, (query, label) in enumerate(all_queries, 1):
            pct = (i / total_queries) * 100
            print(f"\n[{i}/{total_queries}] ({pct:.0f}%) Query: {query}")

            # Construct Google News RSS URL
            encoded_query = quote_plus(query)
            rss_url = GOOGLE_NEWS_RSS_BASE.format(query=encoded_query)

            try:
                # Fetch Google News RSS feed
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/rss+xml,text/xml,application/xml",
                }
                response = requests.get(rss_url, headers=headers, timeout=30)
                response.raise_for_status()

                # Parse RSS feed with BeautifulSoup (xml parser)
                soup = BeautifulSoup(response.content, "xml")
                items = soup.find_all("item")

                if not items:
                    print(f"   No items found in Google News RSS results")
                    continue

                print(f"   Found {len(items)} items in RSS feed")

                # Process each item
                passed = 0
                for item in items:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pub_date_elem = item.find("pubDate")
                    source_elem = item.find("source")

                    if not all([title_elem, link_elem]):
                        continue

                    title = title_elem.get_text(strip=True)
                    url = link_elem.get_text(strip=True)
                    pub_date_str = pub_date_elem.get_text(strip=True) if pub_date_elem else ""
                    source_name = source_elem.get_text(strip=True) if source_elem else "Google News"

                    # Process through existing pipeline
                    record = self._process_article_url(
                        title=title,
                        url=url,
                        source_name=f"Google News ({source_name})",
                        pub_date_str=pub_date_str,
                        description=title,  # Google News RSS doesn't have description
                        threshold=RSS_RELEVANCE_THRESHOLD,
                        is_rss_source=True,
                        debug_mode=debug_mode,
                    )

                    if record is not None:
                        all_records.append(record)
                        passed += 1

                records_collected += passed
                print(f"   ✅ {passed}/{len(items)} articles passed relevance filter")

                # Checkpoint after each query
                if passed > 0:
                    self.checkpoint.save_records_batch(all_records[-passed:])

            except requests.exceptions.Timeout:
                print(f"   ⚠️  Request timed out (30s)")
            except requests.exceptions.RequestException as e:
                print(f"   ⚠️  Network error: {e}")
            except Exception as e:
                print(f"   ⚠️  Error parsing Google News RSS: {e}")

            # Rate limiting: sleep between queries
            time.sleep(NEWS_SCRAPER_SETTINGS["request_delay_seconds"])

        # Final checkpoint save for all records
        if all_records:
            self.checkpoint.save_records_batch(all_records)
            print(f"💾 Checkpointed {len(all_records)} total records")

        # Print stage summary
        print(f"\n📊 Stage 2 Summary: {records_collected} total records from Google News RSS")

        return all_records

    def _run_gap_filling(self) -> list[dict]:
        """
        Stage 3: Gap-filling pass for underrepresented years.

        Reports which years have thin coverage. Does not perform any web searches.

        Returns:
            Empty list (gap-filling via web search has been removed).
        """
        print("\n" + "=" * 60)
        print("📰 STAGE 3: Gap-Filling Analysis")
        print("=" * 60)

        # Load current checkpointed records
        df = self.checkpoint.load_all_records()

        if df.empty:
            print("   No existing records to analyze for gaps")
            return []

        # Count records by year
        year_counts = df["year"].value_counts().to_dict()

        # Identify underrepresented years (2015-2024 with <100 records)
        underrepresented: list[int] = []
        for year in range(2015, 2025):
            count = year_counts.get(year, 0)
            if count < 100:
                underrepresented.append(year)

        if not underrepresented:
            print("   ✅ All years have ≥100 records - no gap filling needed")
            return []

        # Log warning for each underrepresented year
        print(f"\n⚠️  Underrepresented years (coverage < 100 records):")
        for year in sorted(underrepresented):
            count = year_counts.get(year, 0)
            print(f"   • {year}: {count} records")

        print("\nℹ️  Note: Web search gap-filling has been removed.")
        print("   To improve coverage for thin years, add more RSS sources.")

        return []

    def run_phase1_collection(self, debug_mode: bool = False) -> pd.DataFrame:
        """
        Orchestrate full Phase 1 news collection from RSS and Google News RSS.

        Args:
            debug_mode: If True, print title and score of every filtered RSS item.

        Returns:
            DataFrame of all collected records, deduplicated and sorted.
        """
        # Stage 1: RSS collection (broad scoring for maximum coverage)
        rss_records = self.scrape_rss_sources_broad(debug_mode=debug_mode)

        # Stage 2: Google News RSS historical search
        gnews_records = self.scrape_google_news_rss(debug_mode=debug_mode)

        # Stage 3: Gap-filling analysis (reports thin years, no web searches)
        gap_records = self._run_gap_filling()

        # =====================================================================
        # FINAL: Load, deduplicate, and summarize
        # =====================================================================
        print("\n" + "=" * 60)
        print("📊 Finalizing collection...")
        print("=" * 60)

        # Load all checkpointed records
        df = self.checkpoint.load_all_records()

        if df.empty:
            print("⚠️  No records collected")
            return df

        # Deduplicate by URL
        initial_count = len(df)
        df = df.drop_duplicates(subset=["url"], keep="first")
        dedup_count = initial_count - len(df)

        # Sort by relevance score descending
        df = df.sort_values("relevance_score", ascending=False).reset_index(drop=True)

        # Calculate summary statistics
        total_records = len(df)

        # Records by year
        year_counts = df["year"].value_counts().sort_index().to_dict()

        # Records by source
        source_counts = df["source_detail"].value_counts().to_dict()

        # Achilles-related count and percentage
        achilles_count = int(df["is_achilles_related"].sum())
        achilles_pct = (achilles_count / total_records) * 100 if total_records > 0 else 0

        # Fetch success rate
        fetch_stats = self.fetcher.get_fetch_stats()
        success_rate = fetch_stats["success_rate"]

        # Unique player coverage
        all_players: set[str] = set()
        for players_json in df["mentioned_players"]:
            try:
                players = json.loads(players_json)
                if isinstance(players, list):
                    all_players.update(players)
            except (json.JSONDecodeError, TypeError):
                pass

        # Print summary
        print(f"\n📈 TOTAL RECORDS: {total_records}")
        if dedup_count > 0:
            print(f"🔄 Duplicates removed: {dedup_count}")

        print(f"\n📅 RECORDS BY YEAR:")
        for year, count in sorted(year_counts.items()):
            print(f"   • {year}: {count}")

        print(f"\n📰 RECORDS BY SOURCE:")
        for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
            print(f"   • {source}: {count}")

        print(f"\n🏥 ACHILLES-RELATED: {achilles_count} ({achilles_pct:.1f}%)")

        print(f"\n📄 ARTICLE FETCH STATS:")
        print(f"   Successful: {fetch_stats['success_count']}")
        print(f"   Failed: {fetch_stats['failure_count']}")
        print(f"   Success Rate: {success_rate:.1%}")

        print(f"\n🏀 UNIQUE PLAYERS MENTIONED: {len(all_players)}")
        if all_players:
            print(f"   Players: {', '.join(sorted(all_players))}")

        print("\n" + "=" * 60)
        print("✅ PHASE 1 NEWS COLLECTION COMPLETE")
        print("=" * 60)

        return df
