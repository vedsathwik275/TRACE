# bluesky_scraper_v2.py
"""
TRACE Bluesky Scraper V2 - Historical Achilles injury content collection.

Improved scraper with checkpointing, relevance scoring, and multi-phase collection.
Mirrors the architecture of TRACERedditScraperV2.

This module collects posts from Bluesky via the AT Protocol search API,
applying relevance scoring and thread extraction to build a comprehensive
dataset of NBA Achilles injury discussions.

Attributes:
    BLUESKY_RELEVANCE_THRESHOLD: Minimum relevance score for Bluesky posts (1.0).
    BROADER_QUERIES: List of broader injury-related search queries for Stage 3.
    RATE_LIMIT_SEARCH_SECONDS: Delay between search API calls (0.5s).
    RATE_LIMIT_THREAD_SECONDS: Delay between thread fetch calls (0.3s).
    MAX_POSTS_PER_QUERY: Maximum posts to retrieve per query via pagination (200).
"""

import json
import time
from datetime import datetime
from typing import Optional

import requests
import pandas as pd

from scrapers.reddit_config import (
    TARGET_PLAYERS,
    ACHILLES_SEARCH_QUERIES,
    HYPER_RELEVANCE_THRESHOLD,
)
from scrapers.relevance_scorer import TRACERelevanceScorer
from scrapers.checkpoint_manager import TRACECheckpointManager, generate_query_key

# =============================================================================
# BLUESKY-SPECIFIC SETTINGS
# =============================================================================

# Lower relevance threshold for Bluesky's short posts (300 char limit)
BLUESKY_RELEVANCE_THRESHOLD: float = 1.0

# Broader injury queries for Stage 3 collection
BROADER_QUERIES: list[str] = [
    "NBA injury",
    "NBA sidelined",
    "NBA out indefinitely",
    "NBA recovery timeline",
    "NBA return from injury",
    "NBA injury report",
]

# Rate limiting settings (reduced for faster collection)
RATE_LIMIT_SEARCH_SECONDS: float = 0.5
RATE_LIMIT_THREAD_SECONDS: float = 0.3

# Pagination settings
MAX_POSTS_PER_QUERY: int = 200  # Total posts to fetch per query via pagination


class TRACEBlueskyScraperV2:
    """
    Historical Bluesky scraper for NBA Achilles injury content.

    Implements multi-phase collection with checkpointing, relevance scoring,
    and deduplication. Collects posts via the Bluesky AT Protocol search API.

    Attributes:
        scorer: TRACERelevanceScorer instance for computing relevance scores.
        checkpoint: TRACECheckpointManager for saving/loading collection state.
        seen_uris: Set of post URIs already processed to avoid duplicates.
        session: Requests session for HTTP calls.
        access_token: OAuth access token for authenticated API calls.
    """

    def __init__(self) -> None:
        """
        Initialize the scraper with scorer, checkpoint manager, seen URIs set,
        and requests session.
        """
        self.scorer = TRACERelevanceScorer()
        self.checkpoint = TRACECheckpointManager(checkpoint_dir="data/bluesky_checkpoints")
        self.seen_uris: set[str] = set()
        self.session = requests.Session()
        self.access_token: Optional[str] = None

    def login(self, handle: str, password: str) -> bool:
        """
        Authenticate to Bluesky via app password.

        Args:
            handle: Bluesky handle (e.g., 'user.bsky.social').
            password: App-specific password for the account.

        Returns:
            True on successful authentication, False on failure with printed error.

        Raises:
            requests.exceptions.Timeout: If the login request times out.
            requests.exceptions.HTTPError: If the API returns an HTTP error status.
        """
        try:
            print(f"🔐 Logging in as {handle}...")
            url = "https://bsky.social/xrpc/com.atproto.server.createSession"
            payload = {
                "identifier": handle,
                "password": password,
            }
            response = self.session.post(url, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            self.access_token = data["accessJwt"]
            self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})
            print("✅ Login successful!")
            return True
        except requests.exceptions.Timeout:
            print("❌ Login request timed out")
            return False
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP error during login: {e.response.status_code} - {e.response.text}")
            return False
        except Exception as e:
            print(f"❌ Login failed: {e}")
            return False

    def _search_posts(self, query: str, limit: int = MAX_POSTS_PER_QUERY) -> list[dict]:
        """
        Search for posts containing the query with cursor-based pagination.

        Args:
            query: Search query string to find relevant posts.
            limit: Maximum number of posts to retrieve. Defaults to MAX_POSTS_PER_QUERY.

        Returns:
            List of raw post dictionaries from the API response.
            Returns empty list on failure.

        Raises:
            requests.exceptions.Timeout: If the search request times out.
            requests.exceptions.HTTPError: If the API returns an HTTP error status.
        """
        all_posts: list[dict] = []
        cursor: Optional[str] = None

        try:
            while len(all_posts) < limit:
                batch_limit = min(100, limit - len(all_posts))
                params = {
                    "q": query,
                    "limit": batch_limit,
                }
                if cursor:
                    params["cursor"] = cursor

                print(f"🔍 Searching Bluesky for: {query} (batch {len(all_posts) // 100 + 1})")
                url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                posts = data.get("posts", [])
                if not posts:
                    break

                all_posts.extend(posts)

                # Check for cursor to continue pagination
                cursor = data.get("cursor")
                if not cursor:
                    break

                # Rate limit between pagination batches
                time.sleep(RATE_LIMIT_SEARCH_SECONDS)

        except requests.exceptions.Timeout:
            print(f"⚠️  Search timed out for query: {query}")
        except requests.exceptions.HTTPError as e:
            print(f"⚠️  HTTP error {e.response.status_code} for query: {query}")
        except Exception as e:
            print(f"⚠️  Search failed for '{query}': {e}")

        return all_posts

    def _get_thread(self, uri: str, depth: int = 5) -> dict:
        """
        Get a post and its replies up to a certain depth.

        Args:
            uri: Bluesky post URI (e.g., 'at://did:plc:.../app.bsky.feed.post/...').
            depth: Maximum reply depth to fetch. Defaults to 5.

        Returns:
            Thread dictionary from the API response.
            Returns empty dict on failure.

        Raises:
            requests.exceptions.Timeout: If the thread request times out.
            requests.exceptions.HTTPError: If the API returns an HTTP error status.
        """
        try:
            url = "https://bsky.social/xrpc/app.bsky.feed.getPostThread"
            params = {
                "uri": uri,
                "depth": depth,
            }
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(f"⚠️  Thread fetch timed out for URI: {uri[:50]}...")
            return {}
        except requests.exceptions.HTTPError as e:
            print(f"⚠️  HTTP error {e.response.status_code} fetching thread")
            return {}
        except Exception as e:
            print(f"⚠️  Thread fetch failed: {e}")
            return {}

    def _process_post(self, post_dict: dict) -> Optional[dict]:
        """
        Process a single Bluesky post into a standardized record.

        Extracts text content, fetches reply threads, computes relevance scores,
        and builds a record conforming to the 27-column unified schema.

        Args:
            post_dict: Raw post dictionary from the search API.

        Returns:
            Complete record dictionary matching 27-column unified schema,
            or None if below relevance threshold or already seen.
        """
        # Extract core fields
        uri = post_dict.get("uri", "")
        cid = post_dict.get("cid", "")
        author = post_dict.get("author", {})
        record = post_dict.get("record", {})

        # Check for duplicate URI
        if uri in self.seen_uris:
            return None

        # Extract text and engagement metrics
        text = record.get("text", "")
        created_at = record.get("createdAt", "")
        reply_count = record.get("replyCount", 0)
        repost_count = record.get("repostCount", 0)
        like_count = record.get("likeCount", 0)

        # Build combined text (post + thread replies)
        combined_text = text

        # Fetch thread replies up to depth=5
        num_replies_extracted = 0
        total_reply_words = 0
        reply_likes_sum = 0

        if reply_count > 0:
            # Rate limit between thread fetches
            time.sleep(RATE_LIMIT_THREAD_SECONDS)
            thread_data = self._get_thread(uri, depth=5)

            if "thread" in thread_data and "replies" in thread_data["thread"]:
                replies = thread_data["thread"]["replies"][:10]  # Cap at 10 replies
                for reply in replies:
                    reply_post = reply.get("post", {})
                    reply_record = reply_post.get("record", {})
                    reply_text = reply_record.get("text", "")
                    reply_likes = reply_post.get("likeCount", 0)

                    if len(reply_text.strip()) >= 20:
                        combined_text += f"\n---Reply---\n{reply_text}\n---End Reply---\n"
                        total_reply_words += len(reply_text.split())
                        reply_likes_sum += reply_likes
                        num_replies_extracted += 1

        # Compute relevance score
        score, matched_keywords = self.scorer.compute_score(title="", body=combined_text)

        # Filter by Bluesky-specific relevance threshold (lower than Reddit)
        if score < BLUESKY_RELEVANCE_THRESHOLD:
            return None

        # Add URI to seen set
        self.seen_uris.add(uri)

        # Extract players and detect recovery phase
        mentioned_players = self.scorer.extract_players(combined_text)
        recovery_phase = self.scorer.detect_recovery_phase(combined_text)

        # Check if achilles-related
        is_achilles_related = "achilles" in combined_text.lower()

        # Compute engagement tier
        total_engagement = like_count + repost_count
        if total_engagement > 100:
            engagement_tier = "high"
        elif total_engagement > 20:
            engagement_tier = "medium"
        else:
            engagement_tier = "low"

        # Parse created_at to ISO format and extract year/month
        try:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            created_date_iso = created_dt.strftime("%Y-%m-%dT%H:%M:%S")
            year = created_dt.year
            month = created_dt.month
            year_month = created_dt.strftime("%Y-%m")
        except (ValueError, AttributeError):
            created_dt = datetime.now()
            created_date_iso = created_dt.strftime("%Y-%m-%dT%H:%M:%S")
            year = created_dt.year
            month = created_dt.month
            year_month = created_dt.strftime("%Y-%m")

        # Build URL
        author_handle = author.get("handle", "unknown")
        uri_last_segment = uri.split("/")[-1] if uri else ""
        url = f"https://bsky.app/profile/{author_handle}/post/{uri_last_segment}"

        # Compute average reply likes
        avg_reply_likes = float(reply_likes_sum / num_replies_extracted) if num_replies_extracted > 0 else 0.0

        # Build record matching 27-column unified schema
        record = {
            "source_platform": "Bluesky",
            "source_detail": "Post Search",
            "author": author_handle,
            "url": url,
            "text_content": combined_text.strip(),
            "created_date": created_date_iso,
            "engagement_score": int(like_count),
            "engagement_secondary": int(repost_count),
            "engagement_tier": engagement_tier,
            "relevance_score": float(score),
            "recovery_phase": recovery_phase,
            "mentioned_players": json.dumps(mentioned_players),
            "is_achilles_related": is_achilles_related,
            "is_quality_content": total_engagement >= 10,
            "text_length": len(combined_text.strip()),
            "year": year,
            "month": month,
            "year_month": year_month,
            "num_comments_extracted": 0,
            "avg_comment_score": 0.0,
            "total_comment_words": 0,
            "num_replies_extracted": num_replies_extracted,
            "avg_reply_likes": avg_reply_likes,
            "total_reply_words": total_reply_words,
            "body_word_count": 0,
            "fetch_success": True,
            "uploaded_at": None,
        }

        return record

    def run_phase1_collection(self) -> pd.DataFrame:
        """
        Orchestrate full Phase 1 collection across three stages.

        Stage 1: Player-specific searches for each TARGET_PLAYER.
        Stage 2: Achilles-specific queries from ACHILLES_SEARCH_QUERIES.
        Stage 3: Broader injury queries from BROADER_QUERIES.

        Returns:
            DataFrame of all collected records, deduplicated and sorted.
            Falls back to loading from checkpoint if no new records collected.
        """
        all_records: list[dict] = []

        # =====================================================================
        # STAGE 1: Player-specific searches
        # =====================================================================
        print("\n" + "=" * 60)
        print("🚀 STAGE 1: Player-specific searches")
        print("=" * 60)

        for player_name in TARGET_PLAYERS.keys():
            queries = [
                f"{player_name} achilles",
                f"{player_name} achilles injury",
            ]

            for query in queries:
                # Generate query key for checkpointing
                query_key = generate_query_key("bluesky", query, ("2015-01-01", "2026-12-31"))
                completed_queries = self.checkpoint.load_completed_queries()

                if query_key in completed_queries:
                    print(f"⏭️  Skipping (already completed): {query[:50]}...")
                    continue

                # Search and process with pagination
                posts = self._search_posts(query, limit=MAX_POSTS_PER_QUERY)
                records: list[dict] = []

                for post in posts:
                    record = self._process_post(post)
                    if record is not None:
                        records.append(record)

                if records:
                    self.checkpoint.save_records_batch(records)
                    all_records.extend(records)
                    print(
                        f"✅ {len(records):3d} records | "
                        f"{query[:50]:50s}"
                    )

                # Mark query as complete
                self.checkpoint.mark_query_complete(query_key)

                # Rate limit between searches
                time.sleep(RATE_LIMIT_SEARCH_SECONDS)

        # =====================================================================
        # STAGE 2: Achilles-specific queries
        # =====================================================================
        print("\n" + "=" * 60)
        print("🚀 STAGE 2: Achilles-specific queries")
        print("=" * 60)

        for query in ACHILLES_SEARCH_QUERIES:
            # Generate query key for checkpointing
            query_key = generate_query_key("bluesky", query, ("2015-01-01", "2026-12-31"))
            completed_queries = self.checkpoint.load_completed_queries()

            if query_key in completed_queries:
                print(f"⏭️  Skipping (already completed): {query[:50]}...")
                continue

            # Search and process with pagination
            posts = self._search_posts(query, limit=MAX_POSTS_PER_QUERY)
            records: list[dict] = []

            for post in posts:
                record = self._process_post(post)
                if record is not None:
                    records.append(record)

            if records:
                self.checkpoint.save_records_batch(records)
                all_records.extend(records)
                print(
                    f"✅ {len(records):3d} records | "
                    f"{query[:50]:50s}"
                )

            # Mark query as complete
            self.checkpoint.mark_query_complete(query_key)

            # Rate limit between searches
            time.sleep(RATE_LIMIT_SEARCH_SECONDS)

        # =====================================================================
        # STAGE 3: Broader injury queries
        # =====================================================================
        print("\n" + "=" * 60)
        print("🚀 STAGE 3: Broader injury queries")
        print("=" * 60)

        for query in BROADER_QUERIES:
            # Generate query key for checkpointing
            query_key = generate_query_key("bluesky", query, ("2015-01-01", "2026-12-31"))
            completed_queries = self.checkpoint.load_completed_queries()

            if query_key in completed_queries:
                print(f"⏭️  Skipping (already completed): {query[:50]}...")
                continue

            # Search and process with pagination
            posts = self._search_posts(query, limit=MAX_POSTS_PER_QUERY)
            records: list[dict] = []

            for post in posts:
                record = self._process_post(post)
                if record is not None:
                    records.append(record)

            if records:
                self.checkpoint.save_records_batch(records)
                all_records.extend(records)
                print(
                    f"✅ {len(records):3d} records | "
                    f"{query[:50]:50s}"
                )

            # Mark query as complete
            self.checkpoint.mark_query_complete(query_key)

            # Rate limit between searches
            time.sleep(RATE_LIMIT_SEARCH_SECONDS)

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

            # Recovery phase distribution
            phase_dist = df["recovery_phase"].value_counts().to_dict()

            # Print summary
            print(f"✅ New records this run: {len(df)}")
            print(f"📦 Total records in checkpoint: {total_in_checkpoint}")
            print(f"🏀 Achilles-related records: {achilles_count}")
            if dedup_count > 0:
                print(f"🔄 Duplicates removed: {dedup_count}")
            print(f"\n📊 Top 5 mentioned players:")
            for player, count in top_5_players:
                print(f"   • {player}: {count} mentions")
            print(f"\n📈 Recovery phase distribution:")
            for phase, count in sorted(phase_dist.items()):
                print(f"   • {phase}: {count}")

            return df
        else:
            # Fall back to loading from checkpoint
            print("⚠️  No new records collected. Loading from checkpoint...")
            return self.checkpoint.load_all_records()
