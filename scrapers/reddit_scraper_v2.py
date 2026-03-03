# reddit_scraper_v2.py
"""
TRACE Reddit Scraper V2 - Historical Achilles injury content collection.

Improved scraper with checkpointing, relevance scoring, and multi-phase collection.
"""

import json
import time
import re
from datetime import datetime, timedelta
from typing import Optional

import praw
import pandas as pd

from scrapers.reddit_config import (
    TARGET_PLAYERS,
    ACHILLES_SEARCH_QUERIES,
    SUBREDDITS_PRIMARY,
    SUBREDDITS_TEAM,
    SUBREDDITS_SPECIALTY,
    DATE_RANGES,
    HYPER_RELEVANCE_THRESHOLD,
    SCRAPER_SETTINGS,
)
from scrapers.relevance_scorer import TRACERelevanceScorer
from scrapers.checkpoint_manager import TRACECheckpointManager, generate_query_key


class TRACERedditScraperV2:
    """
    Historical Reddit scraper for NBA Achilles injury content.

    Implements multi-phase collection with checkpointing, relevance scoring,
    and deduplication. Collects posts from NBA team subreddits, primary
    discussion subreddits, and specialty subreddits.
    """

    def __init__(self) -> None:
        """
        Initialize the scraper with scorer, checkpoint manager, and seen URLs set.
        """
        self.reddit: Optional[praw.Reddit] = None
        self.scorer = TRACERelevanceScorer()
        self.checkpoint = TRACECheckpointManager()
        self.seen_urls: set[str] = set()

    def setup_connection(self, client_id: str, client_secret: str) -> bool:
        """
        Set up PRAW Reddit connection with read-only OAuth.

        Args:
            client_id: Reddit API client ID.
            client_secret: Reddit API client secret.

        Returns:
            True on successful connection, False on failure with printed error.
        """
        try:
            print(f"🔐 Setting up Reddit connection (TRACE Research)...")
            user_agent = (
                "TRACE-NBA-Achilles-Research:v2.0.0 "
                "(by /u/TRACEResearcher, research@trace-project.org)"
            )
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
            )
            # Test connection
            username = self.reddit.user.me()
            print(f"✅ Connected successfully as: {username}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Reddit: {e}")
            return False

    def _process_submission(self, submission: praw.models.Submission, subreddit_name: str) -> Optional[dict]:
        """
        Process a single PRAW submission into a standardized record.

        Args:
            submission: PRAW submission object.
            subreddit_name: Name of the subreddit the submission came from.

        Returns:
            Complete record dictionary matching 27-column unified schema,
            or None if below relevance threshold or already seen.
        """
        # Check for duplicate URL
        if submission.url in self.seen_urls:
            return None

        # Compute relevance score
        title = submission.title
        selftext = submission.selftext or ""
        score, matched_keywords = self.scorer.compute_score(title, selftext)

        # Filter by relevance threshold
        if score < HYPER_RELEVANCE_THRESHOLD:
            return None

        # Add URL to seen set
        self.seen_urls.add(submission.url)

        # Build combined text for further processing
        combined_text = f"{title} {selftext}"

        # Attempt to fetch comments
        comments_text = ""
        num_comments_extracted = 0
        total_comment_words = 0
        avg_comment_score = 0.0

        try:
            submission.comments.replace_more(limit=0)
            comments = submission.comments.list()[: SCRAPER_SETTINGS["max_comments_per_post"]]

            valid_comments = []
            for comment in comments:
                comment_text = comment.body.strip()
                if len(comment_text) >= 20:
                    valid_comments.append(comment_text)
                    total_comment_words += len(comment_text.split())

            num_comments_extracted = len(valid_comments)
            if valid_comments:
                comments_text = " " + " ".join(valid_comments)
                combined_text += comments_text

            # Calculate average comment score
            if comments:
                total_score = sum(getattr(c, "score", 0) for c in comments if hasattr(c, "score"))
                avg_comment_score = float(total_score / len(comments))

        except Exception:
            # Silently continue on any comment fetch error
            pass

        # Recompute score with comments included
        final_score, _ = self.scorer.compute_score(title, combined_text)

        # Extract players
        mentioned_players = self.scorer.extract_players(combined_text)

        # Detect recovery phase
        recovery_phase = self.scorer.detect_recovery_phase(combined_text)

        # Compute engagement tier
        submission_score = int(submission.score or 0)
        if submission_score > 500:
            engagement_tier = "high"
        elif submission_score > 50:
            engagement_tier = "medium"
        else:
            engagement_tier = "low"

        # Check if achilles-related
        is_achilles_related = any(
            term in combined_text.lower()
            for term in ["achilles", "achilles tendon", "achilles tear", "achilles rupture"]
        )

        # Build timestamp
        created_dt = datetime.fromtimestamp(submission.created_utc)
        created_date = created_dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Build record matching 27-column unified schema
        record = {
            "source_platform": "Reddit",
            "source_detail": subreddit_name,
            "author": str(submission.author) if submission.author else "[deleted]",
            "url": submission.url,
            "text_content": combined_text.strip(),
            "created_date": created_date,
            "engagement_score": int(submission.score or 0),
            "engagement_secondary": int(submission.num_comments or 0),
            "engagement_tier": engagement_tier,
            "relevance_score": float(final_score),
            "recovery_phase": recovery_phase,
            "mentioned_players": json.dumps(mentioned_players),
            "is_achilles_related": is_achilles_related,
            "is_quality_content": submission_score >= SCRAPER_SETTINGS["min_post_score"],
            "text_length": len(combined_text.strip()),
            "year": created_dt.year,
            "month": created_dt.month,
            "year_month": created_dt.strftime("%Y-%m"),
            "num_comments_extracted": num_comments_extracted,
            "avg_comment_score": avg_comment_score,
            "total_comment_words": total_comment_words,
            "num_replies_extracted": 0,
            "avg_reply_likes": 0.0,
            "total_reply_words": 0,
            "body_word_count": len(selftext.split()),
            "fetch_success": True,
            "uploaded_at": None,
        }

        return record

    def search_subreddit_for_query(
        self,
        subreddit_name: str,
        query: str,
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        """
        Search a subreddit for a query within a date range.

        Args:
            subreddit_name: The subreddit to search.
            query: The search query string.
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format.

        Returns:
            List of record dictionaries for posts matching the criteria.
        """
        # Generate query key and check checkpoint
        query_key = generate_query_key(subreddit_name, query, (start_date, end_date))
        completed_queries = self.checkpoint.load_completed_queries()

        if query_key in completed_queries:
            print(f"⏭️  Skipping (already completed): {subreddit_name} - {query[:40]}...")
            return []

        # Parse date range
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Get subreddit
        subreddit = self.reddit.subreddit(subreddit_name)

        # Search with PRAW
        results = []
        try:
            submissions = subreddit.search(
                query,
                sort="relevance",
                time_filter="all",
                limit=SCRAPER_SETTINGS["max_posts_per_query"],
            )

            for submission in submissions:
                # Filter by date range in Python
                post_dt = datetime.fromtimestamp(submission.created_utc)
                if start_dt <= post_dt <= end_dt:
                    record = self._process_submission(submission, subreddit_name)
                    if record is not None:
                        results.append(record)

                # Sleep between submissions
                time.sleep(SCRAPER_SETTINGS["post_delay_seconds"])

        except Exception as e:
            print(f"⚠️  Error searching {subreddit_name} for '{query}': {e}")

        # Mark query as complete
        self.checkpoint.mark_query_complete(query_key)

        return results

    def run_phase1_collection(self) -> pd.DataFrame:
        """
        Orchestrate full Phase 1 collection across three stages.

        Returns:
            DataFrame of all collected records, deduplicated and sorted.
        """
        all_records: list[dict] = []

        # =====================================================================
        # STAGE 1: Player-specific historical searches
        # =====================================================================
        print("\n" + "=" * 60)
        print("🚀 STAGE 1: Player-specific historical searches")
        print("=" * 60)

        for player_name in TARGET_PLAYERS.keys():
            query = f"{player_name} achilles"

            for subreddit in SUBREDDITS_PRIMARY:
                for start_date, end_date in DATE_RANGES:
                    records = self.search_subreddit_for_query(
                        subreddit, query, start_date, end_date
                    )

                    if records:
                        self.checkpoint.save_records_batch(records)
                        all_records.extend(records)
                        print(
                            f"✅ {len(records):3d} records | "
                            f"r/{subreddit:15s} | {query[:30]:35s} | {start_date} to {end_date}"
                        )

                    time.sleep(SCRAPER_SETTINGS["search_delay_seconds"])

        # =====================================================================
        # STAGE 2: Achilles-specific queries across all subreddits
        # =====================================================================
        print("\n" + "=" * 60)
        print("🚀 STAGE 2: Achilles-specific queries across all subreddits")
        print("=" * 60)

        # Build full subreddit list
        all_subreddits = (
            SUBREDDITS_PRIMARY
            + list(SUBREDDITS_TEAM.values())
            + SUBREDDITS_SPECIALTY
        )

        for query in ACHILLES_SEARCH_QUERIES:
            for subreddit in all_subreddits:
                for start_date, end_date in DATE_RANGES:
                    records = self.search_subreddit_for_query(
                        subreddit, query, start_date, end_date
                    )

                    if records:
                        self.checkpoint.save_records_batch(records)
                        all_records.extend(records)
                        print(
                            f"✅ {len(records):3d} records | "
                            f"r/{subreddit:15s} | {query[:30]:35s} | {start_date} to {end_date}"
                        )

                # Sleep between subreddit switches
                time.sleep(SCRAPER_SETTINGS["subreddit_delay_seconds"])

        # =====================================================================
        # STAGE 3: Current hot and top post sweep
        # =====================================================================
        print("\n" + "=" * 60)
        print("🚀 STAGE 3: Current hot and top post sweep")
        print("=" * 60)

        one_year_ago = int((datetime.now() - timedelta(days=365)).timestamp())

        for subreddit_name in SUBREDDITS_PRIMARY:
            subreddit = self.reddit.subreddit(subreddit_name)

            # Fetch hot posts
            print(f"🔍 Fetching hot posts from r/{subreddit_name}...")
            try:
                hot_posts = list(subreddit.hot(limit=200))
                for submission in hot_posts:
                    if submission.created_utc >= one_year_ago:
                        record = self._process_submission(submission, subreddit_name)
                        if record is not None:
                            all_records.append(record)
                            self.checkpoint.save_records_batch([record])
            except Exception as e:
                print(f"⚠️  Error fetching hot posts from r/{subreddit_name}: {e}")

            # Fetch top posts from past year
            print(f"🔍 Fetching top posts from r/{subreddit_name}...")
            try:
                top_posts = list(subreddit.top(time_filter="year", limit=200))
                for submission in top_posts:
                    record = self._process_submission(submission, subreddit_name)
                    if record is not None:
                        all_records.append(record)
                        self.checkpoint.save_records_batch([record])
            except Exception as e:
                print(f"⚠️  Error fetching top posts from r/{subreddit_name}: {e}")

        # =====================================================================
        # FINAL: Combine, deduplicate, and summarize
        # =====================================================================
        print("\n" + "=" * 60)
        print("📊 Finalizing collection...")
        print("=" * 60)

        if all_records:
            df = pd.DataFrame(all_records)

            # Deduplicate on URL
            initial_count = len(df)
            df = df.drop_duplicates(subset=["url"], keep="first")
            dedup_count = initial_count - len(df)

            # Sort by relevance score descending
            df = df.sort_values("relevance_score", ascending=False).reset_index(drop=True)

            # Calculate summary statistics
            total_in_checkpoint = self.checkpoint.get_record_count()
            achilles_count = int(df["is_achilles_related"].sum())

            # Date range covered
            min_date = df["created_date"].min()
            max_date = df["created_date"].max()

            # Top 5 mentioned players
            all_players: list[str] = []
            for players_json in df["mentioned_players"]:
                try:
                    players = json.loads(players_json)
                    all_players.extend(players)
                except (json.JSONDecodeError, TypeError):
                    pass

            from collections import Counter
            player_counts = Counter(all_players)
            top_5_players = player_counts.most_common(5)

            # Print summary
            print(f"✅ New records this run: {len(df)}")
            print(f"📦 Total records in checkpoint: {total_in_checkpoint}")
            print(f"🏀 Achilles-related records: {achilles_count}")
            print(f"📅 Date range covered: {min_date} to {max_date}")
            if dedup_count > 0:
                print(f"🔄 Duplicates removed: {dedup_count}")
            print(f"\n📊 Top 5 mentioned players:")
            for player, count in top_5_players:
                print(f"   • {player}: {count} mentions")

            return df
        else:
            # Fall back to loading from checkpoint
            print("⚠️  No new records collected. Loading from checkpoint...")
            return self.checkpoint.load_all_records()
