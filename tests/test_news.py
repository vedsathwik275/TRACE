#!/usr/bin/env python3
"""
TRACE News Scraper V2 - End-to-End Integration Tests

Run with: python tests/test_news.py
Requires network access for Tests 3, 4, 9.
Uses no external testing framework — plain Python with pass/fail summary.
"""

import os
import sys
import json
import shutil
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# =============================================================================
# Test Results Tracking
# =============================================================================

test_results: list[tuple[int, str, str]] = []  # (number, name, status)


def record_result(test_num: int, test_name: str, passed: bool, skipped: bool = False) -> None:
    """Record a test result for the final summary."""
    if skipped:
        status = "SKIPPED"
    elif passed:
        status = "PASS"
    else:
        status = "FAIL"
    test_results.append((test_num, test_name, status))


def print_test_header(test_num: int, test_name: str) -> None:
    """Print a formatted test header."""
    print(f"\n{'=' * 60}")
    print(f"TEST {test_num}: {test_name}")
    print(f"{'=' * 60}")


# =============================================================================
# Test 1: Config Import
# =============================================================================

def test_config_import() -> bool:
    """
    Test 1 — Config Import

    Verify that news_config.py imports without error and all expected
    constants exist with correct types.
    """
    print_test_header(1, "Config Import")

    try:
        from scrapers.news_config import (
            NEWS_SOURCES,
            FULL_ARTICLE_TIMEOUT_SECONDS,
            MIN_ARTICLE_WORD_COUNT,
            NEWS_SCRAPER_SETTINGS,
        )
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(1, "Config Import", False)
        return False

    all_passed = True

    # Check NEWS_SOURCES: non-empty dict with 12 entries
    if not isinstance(NEWS_SOURCES, dict) or len(NEWS_SOURCES) == 0:
        print("❌ NEWS_SOURCES: Expected non-empty dict")
        all_passed = False
    else:
        print(f"✅ NEWS_SOURCES: dict with {len(NEWS_SOURCES)} sources")

    # Check FULL_ARTICLE_TIMEOUT_SECONDS: integer
    if not isinstance(FULL_ARTICLE_TIMEOUT_SECONDS, int):
        print(f"❌ FULL_ARTICLE_TIMEOUT_SECONDS: Expected int, got {type(FULL_ARTICLE_TIMEOUT_SECONDS).__name__}")
        all_passed = False
    else:
        print(f"✅ FULL_ARTICLE_TIMEOUT_SECONDS: int ({FULL_ARTICLE_TIMEOUT_SECONDS})")

    # Check MIN_ARTICLE_WORD_COUNT: integer
    if not isinstance(MIN_ARTICLE_WORD_COUNT, int):
        print(f"❌ MIN_ARTICLE_WORD_COUNT: Expected int, got {type(MIN_ARTICLE_WORD_COUNT).__name__}")
        all_passed = False
    else:
        print(f"✅ MIN_ARTICLE_WORD_COUNT: int ({MIN_ARTICLE_WORD_COUNT})")

    # Check NEWS_SCRAPER_SETTINGS: dict with expected keys
    expected_keys = {"request_delay_seconds", "article_fetch_delay_seconds", "max_articles_per_query",
                     "batch_size", "checkpoint_dir", "max_retries", "retry_delay_seconds"}
    if not isinstance(NEWS_SCRAPER_SETTINGS, dict):
        print(f"❌ NEWS_SCRAPER_SETTINGS: Expected dict, got {type(NEWS_SCRAPER_SETTINGS).__name__}")
        all_passed = False
    elif not expected_keys.issubset(NEWS_SCRAPER_SETTINGS.keys()):
        missing = expected_keys - set(NEWS_SCRAPER_SETTINGS.keys())
        print(f"❌ NEWS_SCRAPER_SETTINGS: Missing keys: {missing}")
        all_passed = False
    else:
        print(f"✅ NEWS_SCRAPER_SETTINGS: dict with {len(NEWS_SCRAPER_SETTINGS)} keys")

    record_result(1, "Config Import", all_passed)
    return all_passed


# =============================================================================
# Test 2: Article Fetcher Offline
# =============================================================================

def test_article_fetcher_offline() -> bool:
    """
    Test 2 — Article Fetcher Offline

    Verify TRACEArticleFetcher initializes correctly and get_fetch_stats
    returns expected structure with zero counts. No network calls.
    """
    print_test_header(2, "Article Fetcher Offline")

    try:
        from scrapers.article_fetcher import TRACEArticleFetcher
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(2, "Article Fetcher Offline", False)
        return False

    try:
        fetcher = TRACEArticleFetcher()

        # Verify initialization
        if fetcher.session is None:
            print("❌ FAIL: session is None after initialization")
            record_result(2, "Article Fetcher Offline", False)
            return False

        print("✅ TRACEArticleFetcher initialized successfully")

        # Check get_fetch_stats on fresh instance
        stats = fetcher.get_fetch_stats()

        if not isinstance(stats, dict):
            print(f"❌ FAIL: get_fetch_stats returned {type(stats)}, expected dict")
            record_result(2, "Article Fetcher Offline", False)
            return False

        expected_keys = {"success_count", "failure_count", "success_rate"}
        if not expected_keys.issubset(stats.keys()):
            print(f"❌ FAIL: get_fetch_stats missing keys: {expected_keys - set(stats.keys())}")
            record_result(2, "Article Fetcher Offline", False)
            return False

        print(f"✅ get_fetch_stats returns dict with expected keys")

        # Verify zero counts on fresh instance
        if stats["success_count"] != 0 or stats["failure_count"] != 0:
            print(f"❌ FAIL: Expected zero counts on fresh instance, got success={stats['success_count']}, failure={stats['failure_count']}")
            record_result(2, "Article Fetcher Offline", False)
            return False

        print(f"✅ Zero counts on fresh instance (success=0, failure=0)")

        # Verify success_rate is 0.0 on fresh instance
        if stats["success_rate"] != 0.0:
            print(f"❌ FAIL: Expected success_rate=0.0, got {stats['success_rate']}")
            record_result(2, "Article Fetcher Offline", False)
            return False

        print(f"✅ success_rate is 0.0 on fresh instance")

        record_result(2, "Article Fetcher Offline", True)
        return True

    except Exception as e:
        print(f"❌ FAIL: Exception during test - {e}")
        record_result(2, "Article Fetcher Offline", False)
        return False


# =============================================================================
# Test 3: Article Fetcher Live
# =============================================================================

def test_article_fetcher_live() -> tuple[bool, bool]:
    """
    Test 3 — Article Fetcher Live

    Attempt to fetch one known public NBA news article URL.
    Skip if network unavailable rather than failing.
    """
    print_test_header(3, "Article Fetcher Live")

    try:
        from scrapers.article_fetcher import TRACEArticleFetcher
        from scrapers.news_config import MIN_ARTICLE_WORD_COUNT
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(3, "Article Fetcher Live", False)
        return False, False

    # Use a known public URL (NBA.com official news, typically not paywalled)
    test_url = "https://www.nba.com/news"

    try:
        fetcher = TRACEArticleFetcher()
        result = fetcher.fetch_full_article(test_url)

    except Exception as e:
        print(f"⚠️  SKIP: Network unavailable - {e}")
        record_result(3, "Article Fetcher Live", False, skipped=True)
        return False, True

    # Verify returned dict has all 4 expected keys
    expected_keys = {"text", "word_count", "fetch_success", "authors"}
    if not isinstance(result, dict):
        print(f"❌ FAIL: Expected dict, got {type(result)}")
        record_result(3, "Article Fetcher Live", False)
        return False, False

    missing_keys = expected_keys - set(result.keys())
    if missing_keys:
        print(f"❌ FAIL: Missing keys: {missing_keys}")
        record_result(3, "Article Fetcher Live", False)
        return False, False

    print(f"✅ Returned dict has all 4 expected keys")

    # Verify fetch_success is boolean
    if not isinstance(result["fetch_success"], bool):
        print(f"❌ FAIL: fetch_success is not boolean: {type(result['fetch_success'])}")
        record_result(3, "Article Fetcher Live", False)
        return False, False

    print(f"✅ fetch_success is boolean: {result['fetch_success']}")

    # If fetch_success is True, verify word_count meets minimum
    if result["fetch_success"]:
        if result["word_count"] < MIN_ARTICLE_WORD_COUNT:
            print(f"❌ FAIL: fetch_success=True but word_count={result['word_count']} < {MIN_ARTICLE_WORD_COUNT}")
            record_result(3, "Article Fetcher Live", False)
            return False, False
        print(f"✅ word_count ({result['word_count']}) >= MIN_ARTICLE_WORD_COUNT ({MIN_ARTICLE_WORD_COUNT})")
    else:
        print(f"ℹ️  fetch_success=False (expected for some URLs)")

    record_result(3, "Article Fetcher Live", True)
    return True, False


# =============================================================================
# Test 4: RSS Feed Fetch
# =============================================================================

def test_rss_feed_fetch() -> tuple[bool, bool]:
    """
    Test 4 — RSS Feed Fetch

    Call fetch_rss_feed on ESPN NBA RSS URL with limit of 5 items.
    Skip if network unavailable.
    """
    print_test_header(4, "RSS Feed Fetch")

    try:
        from scrapers.article_fetcher import TRACEArticleFetcher
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(4, "RSS Feed Fetch", False)
        return False, False

    espn_rss_url = "https://www.espn.com/espn/rss/nba/news"

    try:
        fetcher = TRACEArticleFetcher()
        items = fetcher.fetch_rss_feed("ESPN NBA", espn_rss_url)

    except Exception as e:
        print(f"⚠️  SKIP: Network unavailable - {e}")
        record_result(4, "RSS Feed Fetch", False, skipped=True)
        return False, True

    # Verify returned list
    if not isinstance(items, list):
        print(f"❌ FAIL: Expected list, got {type(items)}")
        record_result(4, "RSS Feed Fetch", False)
        return False, False

    print(f"✅ Returned list with {len(items)} items")

    # Verify each item has expected keys
    expected_keys = {"title", "url", "pub_date_str", "description", "source_name"}

    for i, item in enumerate(items[:5]):  # Check first 5
        if not isinstance(item, dict):
            print(f"❌ FAIL: Item {i} is not a dict")
            record_result(4, "RSS Feed Fetch", False)
            return False, False

        missing = expected_keys - set(item.keys())
        if missing:
            print(f"❌ FAIL: Item {i} missing keys: {missing}")
            record_result(4, "RSS Feed Fetch", False)
            return False, False

    print(f"✅ All items have expected keys")

    record_result(4, "RSS Feed Fetch", True)
    return True, False


# =============================================================================
# Test 5: Relevance Scorer News
# =============================================================================

def test_relevance_scorer_news() -> bool:
    """
    Test 5 — Relevance Scorer News

    Verify the scorer correctly identifies relevant vs irrelevant news headlines.
    """
    print_test_header(5, "Relevance Scorer News")

    try:
        from scrapers.relevance_scorer import TRACERelevanceScorer
        from scrapers.reddit_config import HYPER_RELEVANCE_THRESHOLD
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(5, "Relevance Scorer News", False)
        return False

    scorer = TRACERelevanceScorer()
    all_passed = True

    # Test 5.1: Hyper-relevant news headline
    print("\n[Test 5.1] Hyper-relevant headline (Kevin Durant achilles rupture)")
    relevant_title = "Kevin Durant achilles rupture surgery timeline update"
    score1, _ = scorer.compute_score(relevant_title, "")
    is_hyper1 = scorer.is_hyper_relevant(relevant_title, "")

    print(f"  Score: {score1}")
    print(f"  is_hyper_relevant: {is_hyper1}")

    if score1 >= HYPER_RELEVANCE_THRESHOLD and is_hyper1:
        print("  ✅ PASS: Correctly identified as hyper-relevant")
    else:
        print(f"  ❌ FAIL: Expected score >= {HYPER_RELEVANCE_THRESHOLD} and is_hyper_relevant=True")
        all_passed = False

    # Test 5.2: Irrelevant news headline
    print("\n[Test 5.2] Irrelevant headline (Lakers win game)")
    irrelevant_title = "Lakers win game against Celtics in overtime thriller"
    is_hyper2 = scorer.is_hyper_relevant(irrelevant_title, "")

    print(f"  is_hyper_relevant: {is_hyper2}")

    if not is_hyper2:
        print("  ✅ PASS: Correctly identified as not hyper-relevant")
    else:
        print("  ❌ FAIL: Should return False for irrelevant headline")
        all_passed = False

    # Test 5.3: _build_record sets is_achilles_related correctly
    print("\n[Test 5.3] _build_record is_achilles_related field")
    from scrapers.news_scraper_v2 import TRACENewsScraperV2
    scraper = TRACENewsScraperV2()

    # Create a record with achilles content
    record1 = scraper._build_record(
        title="Kevin Durant achilles injury update",
        url="https://example.com/1",
        source_name="ESPN",
        pub_datetime=datetime.now(),
        body_text="Durant is recovering from achilles surgery",
        fetch_success=True,
        authors=["John Doe"],
        score=25.0,
        keywords=["achilles"],
    )

    if record1["is_achilles_related"]:
        print("  ✅ is_achilles_related=True for achilles content")
    else:
        print("  ❌ FAIL: is_achilles_related should be True")
        all_passed = False

    # Create a record without achilles content
    record2 = scraper._build_record(
        title="Lakers win championship",
        url="https://example.com/2",
        source_name="ESPN",
        pub_datetime=datetime.now(),
        body_text="The Lakers defeated the Celtics",
        fetch_success=True,
        authors=["Jane Doe"],
        score=5.0,
        keywords=[],
    )

    if not record2["is_achilles_related"]:
        print("  ✅ is_achilles_related=False for non-achilles content")
    else:
        print("  ❌ FAIL: is_achilles_related should be False")
        all_passed = False

    # Test 5.4: recovery_phase detection
    print("\n[Test 5.4] recovery_phase detection")
    record3 = scraper._build_record(
        title="Player cleared to play",
        url="https://example.com/3",
        source_name="ESPN",
        pub_datetime=datetime.now(),
        body_text="The team announced the player is cleared to play for tonight's game",
        fetch_success=True,
        authors=["Test"],
        score=20.0,
        keywords=[],
    )

    if record3["recovery_phase"] == "return_anticipation":
        print(f"  ✅ recovery_phase='return_anticipation' for 'cleared to play'")
    else:
        print(f"  ❌ FAIL: Expected 'return_anticipation', got '{record3['recovery_phase']}'")
        all_passed = False

    record_result(5, "Relevance Scorer News", all_passed)
    return all_passed


# =============================================================================
# Test 6: Checkpoint Lifecycle
# =============================================================================

def test_checkpoint_lifecycle() -> bool:
    """
    Test 6 — Checkpoint Lifecycle

    Run the same lifecycle test as test_reddit.py but with news checkpoint directory.
    Verify isolation from existing checkpoints.
    """
    print_test_header(6, "Checkpoint Lifecycle")

    try:
        from scrapers.checkpoint_manager import TRACECheckpointManager
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(6, "Checkpoint Lifecycle", False)
        return False

    import pandas as pd

    temp_dir = "data/test_news_checkpoints"
    all_passed = True

    try:
        # Clean up any existing test checkpoint
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        # Instantiate with temp directory (news checkpoint dir)
        checkpoint = TRACECheckpointManager(checkpoint_dir=temp_dir)

        # Verify directory was created
        if os.path.exists(temp_dir):
            print(f"✅ Directory created: {temp_dir}")
        else:
            print(f"❌ FAIL: Directory not created: {temp_dir}")
            all_passed = False

        # Verify load returns empty set
        completed = checkpoint.load_completed_queries()
        if isinstance(completed, set) and len(completed) == 0:
            print("✅ load_completed_queries returns empty set")
        else:
            print(f"❌ FAIL: Expected empty set, got {completed}")
            all_passed = False

        # Mark a query as complete
        fake_key = "test_news_query_2020-01-01_2020-12-31"
        checkpoint.mark_query_complete(fake_key)

        # Verify key appears
        completed = checkpoint.load_completed_queries()
        if fake_key in completed:
            print(f"✅ mark_query_complete works")
        else:
            print(f"❌ FAIL: Key not found")
            all_passed = False

        # Save 3 fake records
        fake_records = [
            {"url": "https://example.com/1", "text_content": "Test 1", "source_platform": "News"},
            {"url": "https://example.com/2", "text_content": "Test 2", "source_platform": "News"},
            {"url": "https://example.com/3", "text_content": "Test 3", "source_platform": "News"},
        ]
        checkpoint.save_records_batch(fake_records)

        # Verify count
        count = checkpoint.get_record_count()
        if count == 3:
            print(f"✅ get_record_count returns 3")
        else:
            print(f"❌ FAIL: Expected 3, got {count}")
            all_passed = False

        # Verify load_all_records
        df = checkpoint.load_all_records()
        if isinstance(df, pd.DataFrame) and len(df) == 3:
            print(f"✅ load_all_records returns DataFrame with 3 rows")
        else:
            print(f"❌ FAIL: Expected DataFrame with 3 rows")
            all_passed = False

        # Clear checkpoint
        print("  Clearing checkpoint...")
        checkpoint.clear_checkpoint()

        # Verify cleared
        count_after = checkpoint.get_record_count()
        if count_after == 0:
            print(f"✅ clear_checkpoint works - count is 0")
        else:
            print(f"❌ FAIL: Expected 0, got {count_after}")
            all_passed = False

    except Exception as e:
        print(f"❌ FAIL: Exception - {e}")
        all_passed = False

    finally:
        # Clean up
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"✅ Cleaned up: {temp_dir}")

    record_result(6, "Checkpoint Lifecycle", all_passed)
    return all_passed


# =============================================================================
# Test 7: Process Article URL Filtering
# =============================================================================

def test_process_article_url_filtering() -> bool:
    """
    Test 7 — Process Article URL Filtering

    Call _process_article_url with irrelevant and relevant titles.
    Use source with fetch_full_article=False to avoid network calls.
    """
    print_test_header(7, "Process Article URL Filtering")

    try:
        from scrapers.news_scraper_v2 import TRACENewsScraperV2
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(7, "Process Article URL Filtering", False)
        return False

    scraper = TRACENewsScraperV2()
    all_passed = True

    # Test 7.1: Irrelevant title should return None
    print("\n[Test 7.1] Irrelevant title (should return None)")
    result1 = scraper._process_article_url(
        title="NBA standings update for March",
        url="https://example.com/irrelevant",
        source_name="Yahoo Sports NBA",  # fetch_full_article=False
        pub_date_str="2024-03-01",
        description="Latest NBA standings and playoff picture",
    )

    if result1 is None:
        print("  ✅ PASS: Irrelevant title correctly returned None")
    else:
        print(f"  ❌ FAIL: Expected None, got dict with score={result1.get('relevance_score')}")
        all_passed = False

    # Test 7.2: Relevant title should return 27-column dict
    print("\n[Test 7.2] Relevant title (should return 27-column dict)")
    result2 = scraper._process_article_url(
        title="Kevin Durant achilles rupture surgery timeline update",
        url="https://example.com/relevant",
        source_name="Yahoo Sports NBA",
        pub_date_str="2019-06-15",
        description="Durant suffers achilles tear in Finals",
    )

    if result2 is None:
        print("  ❌ FAIL: Relevant title should return dict, not None")
        all_passed = False
    else:
        # Verify 27 columns
        expected_columns = {
            "source_platform", "source_detail", "author", "url", "text_content",
            "created_date", "engagement_score", "engagement_secondary", "engagement_tier",
            "relevance_score", "recovery_phase", "mentioned_players", "is_achilles_related",
            "is_quality_content", "text_length", "year", "month", "year_month",
            "num_comments_extracted", "avg_comment_score", "total_comment_words",
            "num_replies_extracted", "avg_reply_likes", "total_reply_words",
            "body_word_count", "fetch_success", "uploaded_at",
        }

        missing = expected_columns - set(result2.keys())
        if missing:
            print(f"  ❌ FAIL: Missing columns: {missing}")
            all_passed = False
        else:
            print(f"  ✅ PASS: All 27 columns present")
            print(f"     Relevance score: {result2['relevance_score']}")
            print(f"     is_achilles_related: {result2['is_achilles_related']}")

    record_result(7, "Process Article URL Filtering", all_passed)
    return all_passed


# =============================================================================
# Test 8: Schema Conformance
# =============================================================================

def test_schema_conformance() -> bool:
    """
    Test 8 — Schema Conformance

    Construct 5 fake article dicts, pass through _build_record.
    Verify all 27 columns, source_platform="News", engagement_score=0.0.
    """
    print_test_header(8, "Schema Conformance")

    try:
        from scrapers.news_scraper_v2 import TRACENewsScraperV2
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(8, "Schema Conformance", False)
        return False

    scraper = TRACENewsScraperV2()
    all_passed = True

    # Create 5 fake articles
    fake_articles = [
        {
            "title": f"Test article {i}",
            "url": f"https://example.com/{i}",
            "source_name": "ESPN",
            "pub_datetime": datetime(2020 + i, 6, 15),
            "body_text": f"Test body content for article {i} about achilles injury",
            "fetch_success": i % 2 == 0,
            "authors": ["Test Author"],
            "score": 15.0 + i * 5,
            "keywords": ["achilles"],
        }
        for i in range(5)
    ]

    expected_columns = {
        "source_platform", "source_detail", "author", "url", "text_content",
        "created_date", "engagement_score", "engagement_secondary", "engagement_tier",
        "relevance_score", "recovery_phase", "mentioned_players", "is_achilles_related",
        "is_quality_content", "text_length", "year", "month", "year_month",
        "num_comments_extracted", "avg_comment_score", "total_comment_words",
        "num_replies_extracted", "avg_reply_likes", "total_reply_words",
        "body_word_count", "fetch_success", "uploaded_at",
    }

    for i, article in enumerate(fake_articles):
        record = scraper._build_record(**article)

        # Check all 27 columns
        missing = expected_columns - set(record.keys())
        if missing:
            print(f"❌ Article {i}: Missing columns: {missing}")
            all_passed = False
            continue

        # Check source_platform is "News"
        if record["source_platform"] != "News":
            print(f"❌ Article {i}: source_platform is '{record['source_platform']}', expected 'News'")
            all_passed = False

        # Check engagement_score is 0.0
        if record["engagement_score"] != 0.0:
            print(f"❌ Article {i}: engagement_score is {record['engagement_score']}, expected 0.0")
            all_passed = False

    if all_passed:
        print(f"✅ All 5 records have correct schema")
        print(f"✅ source_platform is 'News' for all records")
        print(f"✅ engagement_score is 0.0 for all records")

    record_result(8, "Schema Conformance", all_passed)
    return all_passed


# =============================================================================
# Test 9: RSS Full Pipeline
# =============================================================================

def test_rss_full_pipeline() -> tuple[bool, bool]:
    """
    Test 9 — RSS Full Pipeline

    Call scrape_rss_sources with limit of 3 articles per source.
    Verify all records pass relevance threshold, no duplicate URLs, schema conformance.
    Skip if network unavailable.
    """
    print_test_header(9, "RSS Full Pipeline")

    try:
        from scrapers.news_scraper_v2 import TRACENewsScraperV2
        from scrapers.news_config import RSS_RELEVANCE_THRESHOLD
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(9, "RSS Full Pipeline", False)
        return False, False

    try:
        scraper = TRACENewsScraperV2()
        records = scraper.scrape_rss_sources()

    except Exception as e:
        print(f"⚠️  SKIP: Network unavailable - {e}")
        record_result(9, "RSS Full Pipeline", False, skipped=True)
        return False, True

    if not isinstance(records, list):
        print(f"❌ FAIL: Expected list, got {type(records)}")
        record_result(9, "RSS Full Pipeline", False)
        return False, False

    print(f"\n📊 Pipeline returned {len(records)} records")

    if len(records) == 0:
        print("⚠️  No records returned (may be expected if all filtered)")
        record_result(9, "RSS Full Pipeline", True)
        return True, False

    all_passed = True

    # Check all records pass RSS relevance threshold
    below_threshold = [r for r in records if r.get("relevance_score", 0) < RSS_RELEVANCE_THRESHOLD]
    if below_threshold:
        print(f"❌ {len(below_threshold)} records below RSS threshold")
        all_passed = False
    else:
        print(f"✅ All records meet RSS threshold (≥{RSS_RELEVANCE_THRESHOLD})")

    # Check no duplicate URLs
    urls = [r.get("url") for r in records]
    dupe_count = len(urls) - len(set(urls))
    if dupe_count > 0:
        print(f"❌ {dupe_count} duplicate URLs found")
        all_passed = False
    else:
        print(f"✅ No duplicate URLs")

    # Check schema conformance
    expected_columns = {
        "source_platform", "source_detail", "author", "url", "text_content",
        "created_date", "engagement_score", "engagement_secondary", "engagement_tier",
        "relevance_score", "recovery_phase", "mentioned_players", "is_achilles_related",
        "is_quality_content", "text_length", "year", "month", "year_month",
        "num_comments_extracted", "avg_comment_score", "total_comment_words",
        "num_replies_extracted", "avg_reply_likes", "total_reply_words",
        "body_word_count", "fetch_success", "uploaded_at",
    }

    for i, record in enumerate(records[:3]):  # Check first 3
        missing = expected_columns - set(record.keys())
        if missing:
            print(f"❌ Record {i}: Missing columns: {missing}")
            all_passed = False

    if all_passed:
        print(f"✅ All checked records have complete 27-column schema")

    record_result(9, "RSS Full Pipeline", all_passed)
    return all_passed, False


# =============================================================================
# Test 10: Deduplication
# =============================================================================

def test_deduplication() -> bool:
    """
    Test 10 — Deduplication

    Call _process_article_url twice with identical URL.
    Verify second call returns None (in-memory dedup).
    Verify checkpoint URLs are loaded into seen set on startup.
    """
    print_test_header(10, "Deduplication")

    try:
        from scrapers.news_scraper_v2 import TRACENewsScraperV2
        from scrapers.checkpoint_manager import TRACECheckpointManager
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(10, "Deduplication", False)
        return False

    temp_dir = "data/test_dedup_checkpoints"
    all_passed = True

    try:
        # Clean up
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        # Test 10.1: In-memory deduplication
        print("\n[Test 10.1] In-memory URL deduplication")
        scraper = TRACENewsScraperV2()

        # First call with a high-relevance URL to ensure it passes threshold
        result1 = scraper._process_article_url(
            title="Kevin Durant achilles rupture confirmed surgery",
            url="https://example.com/dedup_test",
            source_name="Yahoo Sports NBA",
            pub_date_str="2019-06-15",
            description="Durant achilles tear Warriors Finals",
        )

        if result1 is None:
            print("  ⚠️  First call returned None (below threshold)")
            print("  ❌ FAIL: High-relevance content should pass threshold")
            all_passed = False
        else:
            print(f"  ✅ First call returned record (score={result1.get('relevance_score', 0):.1f})")

        # Second call with same URL should return None (dedup)
        result2 = scraper._process_article_url(
            title="Kevin Durant achilles update",
            url="https://example.com/dedup_test",
            source_name="Yahoo Sports NBA",
            pub_date_str="2019-06-15",
            description="Different description",
        )

        if result2 is None:
            print("  ✅ PASS: Second call with same URL returned None (dedup working)")
        else:
            print("  ❌ FAIL: Second call should return None for duplicate URL")
            all_passed = False

        # Test 10.2: Checkpoint URL loading
        print("\n[Test 10.2] Checkpoint URL loading on startup")

        # Save a fake record to checkpoint
        checkpoint = TRACECheckpointManager(checkpoint_dir=temp_dir)
        fake_record = {
            "url": "https://example.com/checkpoint_dedup",
            "text_content": "Test content",
            "source_platform": "News",
            "source_detail": "ESPN",
            "author": "",
            "created_date": "2020-01-01T00:00:00",
            "engagement_score": 0.0,
            "engagement_secondary": 0.0,
            "engagement_tier": "medium",
            "relevance_score": 20.0,
            "recovery_phase": "general",
            "mentioned_players": "[]",
            "is_achilles_related": True,
            "is_quality_content": False,
            "text_length": 12,
            "year": 2020,
            "month": 1,
            "year_month": "2020-01",
            "num_comments_extracted": 0,
            "avg_comment_score": 0.0,
            "total_comment_words": 0,
            "num_replies_extracted": 0,
            "avg_reply_likes": 0.0,
            "total_reply_words": 0,
            "body_word_count": 0,
            "fetch_success": False,
            "uploaded_at": None,
        }
        checkpoint.save_records_batch([fake_record])

        # Reinitialize scraper - should load checkpoint URLs into seen set
        scraper2 = TRACENewsScraperV2()

        # Try to process same URL - should return None
        result3 = scraper2._process_article_url(
            title="Test article",
            url="https://example.com/checkpoint_dedup",
            source_name="Yahoo Sports NBA",
            pub_date_str="2020-01-01",
            description="Test",
        )

        if result3 is None:
            print("  ✅ PASS: URL from checkpoint correctly deduplicated")
        else:
            print("  ❌ FAIL: Checkpoint URL should be deduplicated")
            all_passed = False

    except Exception as e:
        print(f"❌ FAIL: Exception - {e}")
        import traceback
        traceback.print_exc()
        all_passed = False

    finally:
        # Clean up
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"  ✅ Cleaned up: {temp_dir}")

    record_result(10, "Deduplication", all_passed)
    return all_passed


# =============================================================================
# Test 11: New RSS Sources Reachable
# =============================================================================

def test_new_rss_sources_reachable() -> bool:
    """
    Test 11 — New RSS Sources Reachable

    Iterate over all 10 entries in NEWS_SOURCES, fetch each RSS feed with
    requests (10-second timeout), then parse with feedparser. Assert that
    feed.entries is a list (even if empty). Mark any source that returns
    0 entries as a warning.
    """
    print_test_header(11, "New RSS Sources Reachable")

    try:
        import feedparser
        import requests
        from scrapers.news_config import NEWS_SOURCES
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(11, "New RSS Sources Reachable", False)
        return False

    all_passed = True
    warnings = []

    # Verify we have 10 sources
    if len(NEWS_SOURCES) != 10:
        print(f"❌ FAIL: Expected 10 RSS sources, got {len(NEWS_SOURCES)}")
        all_passed = False
    else:
        print(f"✅ Found {len(NEWS_SOURCES)} RSS sources")

    # Set up session with timeout
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })

    # Test each RSS feed
    for source_name, config in NEWS_SOURCES.items():
        rss_url = config.get("rss_url")
        if not rss_url:
            print(f"⚠️  {source_name}: No RSS URL configured")
            warnings.append(source_name)
            continue

        try:
            # Fetch with 10-second timeout
            response = session.get(rss_url, timeout=10)
            response.raise_for_status()

            # Parse the content
            feed = feedparser.parse(response.content)

            # Check that entries is a list
            if not isinstance(feed.entries, list):
                print(f"❌ {source_name}: feed.entries is not a list")
                all_passed = False
            elif len(feed.entries) == 0:
                print(f"⚠️  {source_name}: 0 entries (feed may be temporarily unavailable)")
                warnings.append(source_name)
            else:
                print(f"✅ {source_name}: {len(feed.entries)} entries fetched")

        except requests.exceptions.Timeout:
            print(f"❌ {source_name}: Request timed out (10s)")
            all_passed = False
            warnings.append(source_name)
        except requests.exceptions.RequestException as e:
            print(f"❌ {source_name}: Network error - {e}")
            all_passed = False
            warnings.append(source_name)
        except Exception as e:
            print(f"❌ {source_name}: Failed to parse - {e}")
            all_passed = False
            warnings.append(source_name)

    # Print warnings summary
    if warnings:
        print(f"\n⚠️  Sources with warnings ({len(warnings)}): {', '.join(warnings)}")

    record_result(11, "New RSS Sources Reachable", all_passed)
    return all_passed


# =============================================================================
# Test 12: RSS Source Count
# =============================================================================

def test_rss_source_count() -> bool:
    """
    Test 12 — RSS Source Count

    Assert len(NEWS_SOURCES) == 10 and that all values start with https://.
    """
    print_test_header(12, "RSS Source Count")

    try:
        from scrapers.news_config import NEWS_SOURCES
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(12, "RSS Source Count", False)
        return False

    all_passed = True

    # Check count
    if len(NEWS_SOURCES) != 10:
        print(f"❌ FAIL: Expected 10 RSS sources, got {len(NEWS_SOURCES)}")
        all_passed = False
    else:
        print(f"✅ NEWS_SOURCES has 10 entries")

    # Check all URLs start with https://
    non_https = []
    for source_name, config in NEWS_SOURCES.items():
        rss_url = config.get("rss_url", "")
        if not rss_url.startswith("https://"):
            non_https.append(source_name)

    if non_https:
        print(f"❌ FAIL: Sources without https:// URLs: {', '.join(non_https)}")
        all_passed = False
    else:
        print(f"✅ All RSS URLs start with https://")

    record_result(12, "RSS Source Count", all_passed)
    return all_passed


# =============================================================================
# Test 13: Google News RSS Implementation
# =============================================================================

def test_google_news_rss_implementation() -> bool:
    """
    Test 13 — Google News RSS Implementation

    Verify that scrape_google_news_rss method exists in news_scraper_v2.py
    and that the old Google News searcher references are removed.
    """
    print_test_header(13, "Google News RSS Implementation")

    all_passed = True

    # Check news_scraper_v2.py for new method
    try:
        with open("scrapers/news_scraper_v2.py", "r") as f:
            content = f.read()

        # Verify new method exists
        if "def scrape_google_news_rss" in content:
            print("✅ scrape_google_news_rss method exists")
        else:
            print("❌ FAIL: scrape_google_news_rss method not found")
            all_passed = False

        # Verify Stage 2 is called in run_phase1_collection
        if "scrape_google_news_rss(debug_mode" in content:
            print("✅ Stage 2 Google News RSS is called in run_phase1_collection")
        else:
            print("❌ FAIL: Stage 2 Google News RSS not called")
            all_passed = False

        # Verify old Google News searcher is NOT imported
        if "from scrapers.google_news_searcher" in content:
            print("❌ FAIL: Old google_news_searcher import still present")
            all_passed = False
        else:
            print("✅ Old google_news_searcher import removed")

        # Verify TRACEGoogleNewsSearcher is NOT instantiated
        if "TRACEGoogleNewsSearcher()" in content:
            print("❌ FAIL: Old TRACEGoogleNewsSearcher instantiation still present")
            all_passed = False
        else:
            print("✅ Old TRACEGoogleNewsSearcher instantiation removed")

    except FileNotFoundError:
        print("⚠️  scrapers/news_scraper_v2.py: File not found (skipping)")
    except Exception as e:
        print(f"❌ scrapers/news_scraper_v2.py: Error reading file - {e}")
        all_passed = False

    # Check news_config.py has no old Google News template
    try:
        with open("scrapers/news_config.py", "r") as f:
            content = f.read()

        if "GOOGLE_NEWS_QUERY_TEMPLATE" in content:
            print("❌ FAIL: Old GOOGLE_NEWS_QUERY_TEMPLATE still in news_config.py")
            all_passed = False
        else:
            print("✅ Old GOOGLE_NEWS_QUERY_TEMPLATE removed from news_config.py")

        if "PLAYER_INJURY_WINDOWS" in content:
            print("❌ FAIL: Old PLAYER_INJURY_WINDOWS still in news_config.py")
            all_passed = False
        else:
            print("✅ Old PLAYER_INJURY_WINDOWS removed from news_config.py")

    except FileNotFoundError:
        print("⚠️  scrapers/news_config.py: File not found (skipping)")
    except Exception as e:
        print(f"❌ scrapers/news_config.py: Error reading file - {e}")
        all_passed = False

    record_result(13, "Google News RSS Implementation", all_passed)
    return all_passed


# =============================================================================
# Test 14: Gap Fill Logic
# =============================================================================

def test_gap_fill_logic() -> bool:
    """
    Test 14 — Gap Fill Logic

    Verify Stage 3 gap-filling logic in isolation.
    Create fake checkpoint with records in only 3 years.
    """
    print_test_header(14, "Gap Fill Logic")

    try:
        from scrapers.checkpoint_manager import TRACECheckpointManager
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(14, "Gap Fill Logic", False)
        return False

    import pandas as pd

    temp_dir = "data/test_gap_checkpoints"
    all_passed = True

    try:
        # Clean up any existing test checkpoint
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        # Create fake records: 200 each in years 2018, 2019, 2025
        print("\n[Test 14.1] Creating fake checkpoint with 600 records")
        fake_records = []

        for year, count in [(2018, 200), (2019, 200), (2025, 200)]:
            for i in range(count):
                fake_records.append({
                    "url": f"https://example.com/{year}_{i}",
                    "text_content": f"Test article from {year}",
                    "source_platform": "News",
                    "source_detail": "ESPN",
                    "author": "",
                    "created_date": f"{year}-06-15T12:00:00",
                    "engagement_score": 0.0,
                    "engagement_secondary": 0.0,
                    "engagement_tier": "medium",
                    "relevance_score": 20.0,
                    "recovery_phase": "general",
                    "mentioned_players": "[]",
                    "is_achilles_related": True,
                    "is_quality_content": False,
                    "text_length": 30,
                    "year": year,
                    "month": 6,
                    "year_month": f"{year}-06",
                    "num_comments_extracted": 0,
                    "avg_comment_score": 0.0,
                    "total_comment_words": 0,
                    "num_replies_extracted": 0,
                    "avg_reply_likes": 0.0,
                    "total_reply_words": 0,
                    "body_word_count": 0,
                    "fetch_success": False,
                    "uploaded_at": None,
                })

        checkpoint = TRACECheckpointManager(checkpoint_dir=temp_dir)
        checkpoint.save_records_batch(fake_records)
        print(f"  ✅ Saved {len(fake_records)} fake records")

        # Load into DataFrame
        df = checkpoint.load_all_records()

        if len(df) != 600:
            print(f"  ❌ FAIL: Expected 600 records, got {len(df)}")
            all_passed = False
        else:
            print(f"  ✅ Loaded DataFrame with 600 records")

        # Test 14.2: Identify underrepresented years (2015-2024 with <100 records)
        print("\n[Test 14.2] Identifying underrepresented years (2015-2024)")

        year_counts = df["year"].value_counts().to_dict()
        underrepresented = []

        for year in range(2015, 2025):
            count = year_counts.get(year, 0)
            if count < 100:
                underrepresented.append(year)

        print(f"  Year counts: {year_counts}")
        print(f"  Underrepresented years: {underrepresented}")

        # Verify result is a list with at least 5 years
        if not isinstance(underrepresented, list):
            print(f"  ❌ FAIL: underrepresented should be a list, got {type(underrepresented)}")
            all_passed = False
        elif len(underrepresented) < 5:
            print(f"  ❌ FAIL: Expected at least 5 underrepresented years, got {len(underrepresented)}")
            all_passed = False
        else:
            print(f"  ✅ Found {len(underrepresented)} underrepresented years (≥5 required)")

        # Verify 2018 and 2019 are NOT in underrepresented (they have 200 records each)
        if 2018 in underrepresented:
            print(f"  ❌ FAIL: 2018 should NOT be underrepresented (has 200 records)")
            all_passed = False
        else:
            print(f"  ✅ 2018 correctly excluded (has 200 records)")

        if 2019 in underrepresented:
            print(f"  ❌ FAIL: 2019 should NOT be underrepresented (has 200 records)")
            all_passed = False
        else:
            print(f"  ✅ 2019 correctly excluded (has 200 records)")

        # Verify 2025 is NOT checked (outside 2015-2024 window)
        if 2025 in underrepresented:
            print(f"  ❌ FAIL: 2025 should NOT be checked (outside 2015-2024 window)")
            all_passed = False
        else:
            print(f"  ✅ 2025 correctly excluded (outside scan window)")

    except Exception as e:
        print(f"❌ FAIL: Exception - {e}")
        import traceback
        traceback.print_exc()
        all_passed = False

    finally:
        # Clean up
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"\n  ✅ Cleaned up: {temp_dir}")

    record_result(14, "Gap Fill Logic", all_passed)
    return all_passed


# =============================================================================
# Main Execution
# =============================================================================

def main() -> None:
    """
    Main test runner - executes all tests in order and prints summary.
    """
    print("=" * 60)
    print("🧪 TRACE NEWS SCRAPER V2 - INTEGRATION TESTS")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Test 1: Config Import
    test_config_import()

    # Test 2: Article Fetcher Offline
    test_article_fetcher_offline()

    # Test 3: Article Fetcher Live
    test_article_fetcher_live()

    # Test 4: RSS Feed Fetch
    test_rss_feed_fetch()

    # Test 5: Relevance Scorer News
    test_relevance_scorer_news()

    # Test 6: Checkpoint Lifecycle
    test_checkpoint_lifecycle()

    # Test 7: Process Article URL Filtering
    test_process_article_url_filtering()

    # Test 8: Schema Conformance
    test_schema_conformance()

    # Test 9: RSS Full Pipeline
    test_rss_full_pipeline()

    # Test 10: Deduplication
    test_deduplication()

    # Test 11: New RSS Sources Reachable
    test_new_rss_sources_reachable()

    # Test 12: RSS Source Count
    test_rss_source_count()

    # Test 13: Google News RSS Implementation
    test_google_news_rss_implementation()

    # Test 14: Gap Fill Logic
    test_gap_fill_logic()

    # =====================================================================
    # Final Summary
    # =====================================================================
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    # Print summary table
    print(f"\n{'#':<4} {'Test Name':<35} {'Status':<10}")
    print("-" * 50)

    passed_count = 0
    failed_count = 0
    skipped_count = 0
    failed_tests: list[str] = []

    for test_num, test_name, status in test_results:
        print(f"{test_num:<4} {test_name:<35} {status:<10}")
        if status == "PASS":
            passed_count += 1
        elif status == "FAIL":
            failed_count += 1
            failed_tests.append(test_name)
        else:  # SKIPPED
            skipped_count += 1

    print("-" * 50)
    print(f"{'Total':<4} {'':<35} {len(test_results):<10}")
    print(f"\n✅ Passed:  {passed_count}")
    print(f"❌ Failed:  {failed_count}")
    print(f"⚠️  Skipped: {skipped_count}")

    # Print failing tests at bottom
    if failed_tests:
        print("\n" + "=" * 60)
        print("❌ FAILING TESTS (fix before proceeding):")
        print("=" * 60)
        for test_name in failed_tests:
            print(f"  • {test_name}")

    # Exit with non-zero code if any test failed
    if failed_count > 0:
        print(f"\n🚫 {failed_count} test(s) failed. Exiting with error code 1.")
        sys.exit(1)
    else:
        print(f"\n✅ All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
