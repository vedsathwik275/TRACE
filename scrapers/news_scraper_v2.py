# news_scraper_v2.py
"""
TRACE News Scraper V2 - Historical news article collection.

Improved scraper with Google News search, full article fetching, and checkpointing.
Produces output conforming to the 27-column unified schema.
"""

import json
import time
from datetime import datetime
from typing import Optional

import pandas as pd

from scrapers.news_config import (
    NEWS_SOURCES,
    NEWS_SCRAPER_SETTINGS,
    ARTICLE_SEARCH_QUERIES,
    PLAYER_INJURY_WINDOWS,
    MIN_ARTICLE_WORD_COUNT,
    RSS_RELEVANCE_THRESHOLD,
)
from scrapers.reddit_config import TARGET_PLAYERS, KEYWORD_WEIGHTS, HYPER_RELEVANCE_THRESHOLD
from scrapers.article_fetcher import TRACEArticleFetcher
from scrapers.google_news_searcher import TRACEGoogleNewsSearcher
from scrapers.relevance_scorer import TRACERelevanceScorer
from scrapers.checkpoint_manager import TRACECheckpointManager


class TRACENewsScraperV2:
    """
    Historical news scraper for NBA Achilles injury content.

    Collects articles from RSS feeds and Google News archive search,
    fetches full article content, and applies relevance scoring.
    """

    def __init__(self) -> None:
        """
        Initialize the news scraper with fetcher, searcher, scorer, and checkpoint manager.
        """
        self.fetcher = TRACEArticleFetcher()
        self.searcher = TRACEGoogleNewsSearcher()
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
        debug_mode: bool = False,
    ) -> Optional[dict]:
        """
        Process a single article URL through scoring and fetching.

        Args:
            title: Article title string.
            url: Article URL string.
            source_name: Human-readable source name.
            pub_date_str: Raw publication date string from RSS/Google.
            description: Optional description/summary text from RSS item.
            threshold: Relevance threshold to apply (default: HYPER_RELEVANCE_THRESHOLD).
            debug_mode: If True, print filtered items with their scores.

        Returns:
            Complete record dictionary if article passes relevance threshold,
            None otherwise or if URL is a duplicate.
        """
        # Use provided threshold or default
        if threshold is None:
            threshold = HYPER_RELEVANCE_THRESHOLD

        # Check for duplicate URL
        if url in self.seen_urls:
            return None

        # Compute relevance score on title + description
        score, keywords = self.scorer.compute_score(title, description or "")

        # Filter by relevance threshold
        if score < threshold:
            if debug_mode:
                print(f"   ⏭️  FILTERED (score={score:.1f}): {title[:60]}...")
            return None

        # Add URL to seen set
        self.seen_urls.add(url)

        # Parse publication date
        pub_datetime = self.fetcher.parse_pub_date(pub_date_str)

        # Check if source has full article fetching enabled
        fetch_full = False
        if source_name in NEWS_SOURCES:
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

        # Recompute score with full body if available
        if fetch_success and body_text:
            score, keywords = self.scorer.compute_score(title, body_text)

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

    def scrape_player_archives(self, player_list: list[str]) -> list[dict]:
        """
        Search Google News archive for each player and process results.

        Args:
            player_list: List of player names to search for.

        Returns:
            List of record dictionaries for articles passing relevance filter.
        """
        all_records: list[dict] = []
        running_total = 0

        print("\n" + "=" * 60)
        print("📰 STAGE 2: Historical Archive Search (Player-Specific)")
        print("=" * 60)

        total_players = len(player_list)

        for i, player_name in enumerate(player_list, 1):
            pct = (i / total_players) * 100
            print(f"\n[{i}/{total_players}] ({pct:.0f}%) Searching: {player_name}")

            # Get candidate article URLs from Google News
            articles = self.searcher.search_for_player(player_name)

            if not articles:
                print(f"   No articles found for {player_name}")
                continue

            # Process each article
            player_records: list[dict] = []
            for article in articles:
                record = self._process_article_url(
                    title=article["title"],
                    url=article["url"],
                    source_name=article["source_name"],
                    pub_date_str=article["pub_date_str"],
                )

                if record is not None:
                    player_records.append(record)

            # Checkpoint records for this player
            if player_records:
                self.checkpoint.save_records_batch(player_records)
                running_total += len(player_records)

            print(f"   ✅ {len(player_records)} articles passed filter (total: {running_total})")

            all_records.extend(player_records)

            # Sleep between players
            time.sleep(NEWS_SCRAPER_SETTINGS["request_delay_seconds"])

        return all_records

    def _run_gap_filling(self) -> list[dict]:
        """
        Stage 3: Gap-filling pass for underrepresented years.

        Returns:
            List of record dictionaries from gap-filling searches.
        """
        print("\n" + "=" * 60)
        print("📰 STAGE 3: Gap-Filling Pass")
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

        print(f"   Underrepresented years: {underrepresented}")

        # Player-agnostic queries (last 4 in ARTICLE_SEARCH_QUERIES)
        player_agnostic = ARTICLE_SEARCH_QUERIES[-4:]

        gap_records: list[dict] = []

        for year in underrepresented:
            print(f"\n🔍 Gap-filling for year {year}...")

            for query in player_agnostic:
                # Execute search for this query and year
                articles = self.searcher._execute_search(query, year)

                for article in articles:
                    record = self._process_article_url(
                        title=article["title"],
                        url=article["url"],
                        source_name=article["source_name"],
                        pub_date_str=article["pub_date_str"],
                    )

                    if record is not None:
                        gap_records.append(record)

                # Sleep between searches
                time.sleep(NEWS_SCRAPER_SETTINGS["request_delay_seconds"])

            print(f"   Found {len(gap_records)} additional records for {year}")

        # Checkpoint gap-filling results
        if gap_records:
            self.checkpoint.save_records_batch(gap_records)

        return gap_records

    def run_phase1_collection(self, debug_mode: bool = False) -> pd.DataFrame:
        """
        Orchestrate full Phase 1 news collection across three stages.

        Args:
            debug_mode: If True, print title and score of every filtered RSS item.

        Returns:
            DataFrame of all collected records, deduplicated and sorted.
        """
        # Stage 1: RSS collection
        rss_records = self.scrape_rss_sources(debug_mode=debug_mode)

        # Stage 2: Player archive search
        player_records = self.scrape_player_archives(list(TARGET_PLAYERS.keys()))

        # Stage 3: Gap-filling
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
