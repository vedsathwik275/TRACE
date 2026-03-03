#!/usr/bin/env python3
"""
TRACE Reddit Scraper V2 - End-to-End Integration Tests

Run with: python tests/test_reddit.py
Requires valid Reddit API credentials in .env file for Tests 4-6.
Uses no external testing framework — plain Python with pass/fail summary.
"""

import os
import sys
import json
import shutil
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

    Verify that reddit_config.py imports without error and that all seven
    expected constants exist and have the correct types.
    """
    print_test_header(1, "Config Import")

    try:
        from scrapers.reddit_config import (
            TARGET_PLAYERS,
            ACHILLES_SEARCH_QUERIES,
            SUBREDDITS_PRIMARY,
            SUBREDDITS_TEAM,
            KEYWORD_WEIGHTS,
            HYPER_RELEVANCE_THRESHOLD,
            DATE_RANGES,
        )
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(1, "Config Import", False)
        return False

    all_passed = True

    # Check TARGET_PLAYERS: non-empty dict
    if not isinstance(TARGET_PLAYERS, dict) or len(TARGET_PLAYERS) == 0:
        print("❌ TARGET_PLAYERS: Expected non-empty dict")
        all_passed = False
    else:
        print(f"✅ TARGET_PLAYERS: dict with {len(TARGET_PLAYERS)} players")

    # Check ACHILLES_SEARCH_QUERIES: non-empty list
    if not isinstance(ACHILLES_SEARCH_QUERIES, list) or len(ACHILLES_SEARCH_QUERIES) == 0:
        print("❌ ACHILLES_SEARCH_QUERIES: Expected non-empty list")
        all_passed = False
    else:
        print(f"✅ ACHILLES_SEARCH_QUERIES: list with {len(ACHILLES_SEARCH_QUERIES)} queries")

    # Check SUBREDDITS_PRIMARY: list with exactly 2 entries
    if not isinstance(SUBREDDITS_PRIMARY, list) or len(SUBREDDITS_PRIMARY) != 2:
        print(f"❌ SUBREDDITS_PRIMARY: Expected list with 2 entries, got {len(SUBREDDITS_PRIMARY) if isinstance(SUBREDDITS_PRIMARY, list) else 'wrong type'}")
        all_passed = False
    else:
        print(f"✅ SUBREDDITS_PRIMARY: list with {len(SUBREDDITS_PRIMARY)} entries ({SUBREDDITS_PRIMARY})")

    # Check SUBREDDITS_TEAM: dict with exactly 30 entries
    if not isinstance(SUBREDDITS_TEAM, dict) or len(SUBREDDITS_TEAM) != 30:
        print(f"❌ SUBREDDITS_TEAM: Expected dict with 30 entries, got {len(SUBREDDITS_TEAM) if isinstance(SUBREDDITS_TEAM, dict) else 'wrong type'}")
        all_passed = False
    else:
        print(f"✅ SUBREDDITS_TEAM: dict with {len(SUBREDDITS_TEAM)} teams")

    # Check KEYWORD_WEIGHTS: dict with exactly 4 category keys
    if not isinstance(KEYWORD_WEIGHTS, dict) or len(KEYWORD_WEIGHTS) != 4:
        print(f"❌ KEYWORD_WEIGHTS: Expected dict with 4 keys, got {len(KEYWORD_WEIGHTS) if isinstance(KEYWORD_WEIGHTS, dict) else 'wrong type'}")
        all_passed = False
    else:
        print(f"✅ KEYWORD_WEIGHTS: dict with {len(KEYWORD_WEIGHTS)} categories ({list(KEYWORD_WEIGHTS.keys())})")

    # Check HYPER_RELEVANCE_THRESHOLD: float
    if not isinstance(HYPER_RELEVANCE_THRESHOLD, (int, float)):
        print(f"❌ HYPER_RELEVANCE_THRESHOLD: Expected float, got {type(HYPER_RELEVANCE_THRESHOLD).__name__}")
        all_passed = False
    else:
        print(f"✅ HYPER_RELEVANCE_THRESHOLD: float ({HYPER_RELEVANCE_THRESHOLD})")

    # Check DATE_RANGES: list of tuples
    if not isinstance(DATE_RANGES, list):
        print(f"❌ DATE_RANGES: Expected list, got {type(DATE_RANGES).__name__}")
        all_passed = False
    elif not all(isinstance(r, tuple) for r in DATE_RANGES):
        print("❌ DATE_RANGES: Expected list of tuples")
        all_passed = False
    else:
        print(f"✅ DATE_RANGES: list of {len(DATE_RANGES)} tuples")

    record_result(1, "Config Import", all_passed)
    return all_passed


# =============================================================================
# Test 2: Relevance Scorer Behavior
# =============================================================================

def test_relevance_scorer_behavior() -> bool:
    """
    Test 2 — Relevance Scorer Behavior

    Instantiate TRACERelevanceScorer and run four assertions without network calls.
    """
    print_test_header(2, "Relevance Scorer Behavior")

    try:
        from scrapers.relevance_scorer import TRACERelevanceScorer
        from scrapers.reddit_config import HYPER_RELEVANCE_THRESHOLD
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(2, "Relevance Scorer Behavior", False)
        return False

    scorer = TRACERelevanceScorer()
    all_passed = True

    # Test 2.1: High-relevance text with "achilles rupture" + "Kevin Durant"
    print("\n[Test 2.1] High-relevance text (achilles rupture + Kevin Durant)")
    high_text = "Kevin Durant suffered an achilles rupture during the game. The achilles tendon was torn."
    score1, keywords1 = scorer.compute_score(high_text, high_text)
    is_hyper1 = scorer.is_hyper_relevant(high_text, high_text)

    print(f"  Score: {score1}")
    print(f"  Matched keywords: {keywords1}")
    print(f"  is_hyper_relevant: {is_hyper1}")

    if score1 >= HYPER_RELEVANCE_THRESHOLD and is_hyper1:
        print("  ✅ PASS: Score exceeds threshold")
    else:
        print(f"  ❌ FAIL: Expected score >= {HYPER_RELEVANCE_THRESHOLD} and is_hyper_relevant=True")
        all_passed = False

    # Test 2.2: Unrelated basketball text
    print("\n[Test 2.2] Unrelated basketball text (no injury terms)")
    low_text = "The Lakers won the championship. LeBron James scored 40 points in an amazing performance."
    is_hyper2 = scorer.is_hyper_relevant(low_text, low_text)

    print(f"  is_hyper_relevant: {is_hyper2}")

    if not is_hyper2:
        print("  ✅ PASS: Correctly identified as not hyper-relevant")
    else:
        print("  ❌ FAIL: Should return False for unrelated text")
        all_passed = False

    # Test 2.3: Alias lookup - "KD" should return "Kevin Durant"
    print("\n[Test 2.3] Alias lookup (KD → Kevin Durant)")
    alias_text = "KD is the best player in the league. Kevin Durant is amazing."
    players = scorer.extract_players(alias_text)

    print(f"  Extracted players: {players}")

    if "Kevin Durant" in players:
        print("  ✅ PASS: Alias correctly resolved to Kevin Durant")
    else:
        print("  ❌ FAIL: Expected 'Kevin Durant' in extracted players")
        all_passed = False

    # Test 2.4: Recovery phase detection - "cleared to play"
    print("\n[Test 2.4] Recovery phase detection (cleared to play → return_anticipation)")
    phase_text = "The player is cleared to play and will return to court this weekend."
    phase = scorer.detect_recovery_phase(phase_text)

    print(f"  Detected phase: {phase}")

    if phase == "return_anticipation":
        print("  ✅ PASS: Correctly identified return_anticipation phase")
    else:
        print(f"  ❌ FAIL: Expected 'return_anticipation', got '{phase}'")
        all_passed = False

    record_result(2, "Relevance Scorer Behavior", all_passed)
    return all_passed


# =============================================================================
# Test 3: Checkpoint Manager Lifecycle
# =============================================================================

def test_checkpoint_manager_lifecycle() -> bool:
    """
    Test 3 — Checkpoint Manager Lifecycle

    Test the full checkpoint lifecycle with a temporary directory.
    """
    print_test_header(3, "Checkpoint Manager Lifecycle")

    try:
        from scrapers.checkpoint_manager import TRACECheckpointManager
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(3, "Checkpoint Manager Lifecycle", False)
        return False

    import pandas as pd

    temp_dir = "data/test_checkpoints"
    all_passed = True

    try:
        # Clean up any existing test checkpoint
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        # Instantiate with temp directory
        checkpoint = TRACECheckpointManager(checkpoint_dir=temp_dir)

        # Verify directory was created
        if os.path.exists(temp_dir):
            print(f"✅ Directory created: {temp_dir}")
        else:
            print(f"❌ FAIL: Directory not created: {temp_dir}")
            all_passed = False

        # Verify load_completed_queries returns empty set
        completed = checkpoint.load_completed_queries()
        if isinstance(completed, set) and len(completed) == 0:
            print("✅ load_completed_queries returns empty set")
        else:
            print(f"❌ FAIL: Expected empty set, got {completed}")
            all_passed = False

        # Mark a query as complete
        fake_key = "test_subreddit_test_query_2020-01-01_2020-12-31"
        checkpoint.mark_query_complete(fake_key)

        # Verify key appears in completed queries
        completed = checkpoint.load_completed_queries()
        if fake_key in completed:
            print(f"✅ mark_query_complete works - key appears in set")
        else:
            print(f"❌ FAIL: Key not found in completed queries")
            all_passed = False

        # Save 3 fake records
        fake_records = [
            {"url": "https://example.com/1", "text_content": "Test record 1", "source_platform": "Reddit"},
            {"url": "https://example.com/2", "text_content": "Test record 2", "source_platform": "Reddit"},
            {"url": "https://example.com/3", "text_content": "Test record 3", "source_platform": "Reddit"},
        ]
        checkpoint.save_records_batch(fake_records)

        # Verify get_record_count returns 3
        count = checkpoint.get_record_count()
        if count == 3:
            print(f"✅ get_record_count returns 3")
        else:
            print(f"❌ FAIL: Expected 3 records, got {count}")
            all_passed = False

        # Verify load_all_records returns DataFrame with 3 rows
        df = checkpoint.load_all_records()
        if isinstance(df, pd.DataFrame) and len(df) == 3:
            print(f"✅ load_all_records returns DataFrame with 3 rows")
        else:
            print(f"❌ FAIL: Expected DataFrame with 3 rows, got {type(df)} with {len(df) if hasattr(df, '__len__') else 'N/A'} rows")
            all_passed = False

        # Clear checkpoint
        print("  Clearing checkpoint...")
        checkpoint.clear_checkpoint()

        # Verify files are deleted and count returns 0
        count_after = checkpoint.get_record_count()
        if count_after == 0:
            print(f"✅ clear_checkpoint works - record count is 0")
        else:
            print(f"❌ FAIL: Expected 0 records after clear, got {count_after}")
            all_passed = False

    except Exception as e:
        print(f"❌ FAIL: Exception during test - {e}")
        all_passed = False

    finally:
        # Clean up temp directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"✅ Cleaned up temp directory: {temp_dir}")

    record_result(3, "Checkpoint Manager Lifecycle", all_passed)
    return all_passed


# =============================================================================
# Test 4: Reddit Connection
# =============================================================================

def test_reddit_connection() -> tuple[bool, bool]:
    """
    Test 4 — Reddit Connection

    Load credentials from .env and verify setup_connection works.
    Returns (passed, skipped) tuple.
    """
    print_test_header(4, "Reddit Connection")

    from dotenv import load_dotenv
    load_dotenv()

    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")

    if not client_id or not client_secret:
        print("⚠️  SKIP: REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET not found in .env")
        print("   Add credentials to .env to run Reddit API tests")
        record_result(4, "Reddit Connection", False, skipped=True)
        return False, True

    try:
        from scrapers.reddit_scraper_v2 import TRACERedditScraperV2
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(4, "Reddit Connection", False)
        return False, False

    scraper = TRACERedditScraperV2()
    success = scraper.setup_connection(client_id, client_secret)

    if success and scraper.reddit is not None:
        print(f"✅ PASS: Connected to Reddit API successfully")
        print(f"   Reddit instance type: {type(scraper.reddit).__name__}")
        record_result(4, "Reddit Connection", True)
        return True, False
    else:
        print(f"❌ FAIL: setup_connection returned {success} or reddit is None")
        record_result(4, "Reddit Connection", False)
        return False, False


# =============================================================================
# Test 5: Single Submission Processing
# =============================================================================

def test_single_submission_processing(scraper) -> bool:
    """
    Test 5 — Single Submission Processing

    Fetch 1 hot post from r/nba and process it through _process_submission.
    """
    print_test_header(5, "Single Submission Processing")

    if scraper is None:
        print("⚠️  SKIP: Requires Reddit connection (Test 4 was skipped)")
        record_result(5, "Single Submission Processing", False, skipped=True)
        return False

    try:
        # Fetch exactly 1 hot post from r/nba
        subreddit = scraper.reddit.subreddit("nba")
        hot_posts = list(subreddit.hot(limit=1))

        if not hot_posts:
            print("❌ FAIL: No hot posts found in r/nba")
            record_result(5, "Single Submission Processing", False)
            return False

        submission = hot_posts[0]
        print(f"Processing post: {submission.title[:60]}...")

        # Process through _process_submission
        record = scraper._process_submission(submission, "nba")

        if record is None:
            print("✅ PASS (conditional): Post did not meet relevance threshold (expected behavior)")
            record_result(5, "Single Submission Processing", True)
            return True

        # Verify record structure
        all_passed = True

        # Check all 27 expected columns
        expected_columns = {
            "source_platform", "source_detail", "author", "url", "text_content",
            "created_date", "engagement_score", "engagement_secondary", "engagement_tier",
            "relevance_score", "recovery_phase", "mentioned_players", "is_achilles_related",
            "is_quality_content", "text_length", "year", "month", "year_month",
            "num_comments_extracted", "avg_comment_score", "total_comment_words",
            "num_replies_extracted", "avg_reply_likes", "total_reply_words",
            "body_word_count", "fetch_success", "uploaded_at",
        }

        missing_columns = expected_columns - set(record.keys())
        if missing_columns:
            print(f"❌ Missing columns: {missing_columns}")
            all_passed = False
        else:
            print(f"✅ All 27 schema columns present")

        # Check text_content is non-empty string
        if not isinstance(record.get("text_content"), str) or len(record["text_content"]) == 0:
            print("❌ text_content is not a non-empty string")
            all_passed = False
        else:
            print(f"✅ text_content is non-empty string ({len(record['text_content'])} chars)")

        # Check created_date is parseable ISO datetime
        try:
            datetime.fromisoformat(record["created_date"].replace("Z", "+00:00"))
            print(f"✅ created_date is valid ISO format: {record['created_date']}")
        except (ValueError, KeyError, TypeError):
            print(f"❌ created_date is not valid ISO format: {record.get('created_date')}")
            all_passed = False

        # Check engagement_score is float/int
        if not isinstance(record.get("engagement_score"), (int, float)):
            print(f"❌ engagement_score is not numeric: {type(record.get('engagement_score'))}")
            all_passed = False
        else:
            print(f"✅ engagement_score is numeric: {record['engagement_score']}")

        # Check mentioned_players is valid JSON string
        try:
            players = json.loads(record["mentioned_players"])
            if isinstance(players, list):
                print(f"✅ mentioned_players is valid JSON list: {players}")
            else:
                print(f"❌ mentioned_players is not a list: {type(players)}")
                all_passed = False
        except (json.JSONDecodeError, KeyError, TypeError):
            print(f"❌ mentioned_players is not valid JSON: {record.get('mentioned_players')}")
            all_passed = False

        # Check source_platform equals "Reddit"
        if record.get("source_platform") != "Reddit":
            print(f"❌ source_platform is not 'Reddit': {record.get('source_platform')}")
            all_passed = False
        else:
            print(f"✅ source_platform is 'Reddit'")

        # Print record details for visual inspection
        print(f"\n📋 Record details:")
        print(f"   Relevance score: {record.get('relevance_score', 'N/A')}")
        print(f"   Recovery phase: {record.get('recovery_phase', 'N/A')}")
        print(f"   Text preview: {record.get('text_content', '')[:80]}...")

        record_result(5, "Single Submission Processing", all_passed)
        return all_passed

    except Exception as e:
        print(f"❌ FAIL: Exception during test - {e}")
        import traceback
        traceback.print_exc()
        record_result(5, "Single Submission Processing", False)
        return False


# =============================================================================
# Test 6: Search with Minimal Scraping
# =============================================================================

def test_search_with_minimal_scraping(scraper) -> bool:
    """
    Test 6 — Search with Minimal Scraping

    Call search_subreddit_for_query and verify checkpoint behavior.
    """
    print_test_header(6, "Search with Minimal Scraping")

    if scraper is None:
        print("⚠️  SKIP: Requires Reddit connection (Test 4 was skipped)")
        record_result(6, "Search with Minimal Scraping", False, skipped=True)
        return False

    from scrapers.reddit_config import HYPER_RELEVANCE_THRESHOLD
    from scrapers.checkpoint_manager import generate_query_key

    subreddit = "nba"
    query = "Kevin Durant achilles"
    start_date = "2019-01-01"
    end_date = "2020-12-31"

    try:
        # First call - should actually search
        print(f"Searching r/{subreddit} for '{query}' ({start_date} to {end_date})...")
        results = scraper.search_subreddit_for_query(subreddit, query, start_date, end_date)

        if not isinstance(results, list):
            print(f"❌ FAIL: Expected list, got {type(results)}")
            record_result(6, "Search with Minimal Scraping", False)
            return False

        print(f"✅ Returned list with {len(results)} records")

        if results:
            # Verify all records meet relevance threshold
            below_threshold = [r for r in results if r.get("relevance_score", 0) < HYPER_RELEVANCE_THRESHOLD]
            if below_threshold:
                print(f"❌ {len(below_threshold)} records below relevance threshold")
                record_result(6, "Search with Minimal Scraping", False)
                return False
            print(f"✅ All records meet relevance threshold (≥{HYPER_RELEVANCE_THRESHOLD})")

            # Verify all records fall within date range
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            out_of_range = []
            for r in results:
                try:
                    record_dt = datetime.fromisoformat(r["created_date"].replace("Z", "+00:00"))
                    if not (start_dt <= record_dt <= end_dt):
                        out_of_range.append(r["created_date"])
                except (ValueError, KeyError):
                    out_of_range.append(r.get("created_date", "N/A"))

            if out_of_range:
                print(f"❌ {len(out_of_range)} records outside date range: {out_of_range[:3]}")
                record_result(6, "Search with Minimal Scraping", False)
                return False
            print(f"✅ All records within date range")

            # Verify no duplicate URLs
            urls = [r.get("url") for r in results]
            if len(urls) != len(set(urls)):
                dupes = len(urls) - len(set(urls))
                print(f"❌ {dupes} duplicate URLs found")
                record_result(6, "Search with Minimal Scraping", False)
                return False
            print(f"✅ No duplicate URLs")

            # Print top relevance score
            top_score = max(r.get("relevance_score", 0) for r in results)
            print(f"📊 Top relevance score: {top_score}")

        # Second call - should return empty list (checkpointed)
        print("\nCalling again with same arguments (should skip due to checkpoint)...")
        results2 = scraper.search_subreddit_for_query(subreddit, query, start_date, end_date)

        if results2 == []:
            print(f"✅ PASS: Correctly returned empty list (checkpoint skip worked)")
        else:
            print(f"❌ FAIL: Expected empty list, got {len(results2)} records")
            record_result(6, "Search with Minimal Scraping", False)
            return False

        # Clean up checkpoint entry for this test query
        query_key = generate_query_key(subreddit, query, (start_date, end_date))
        completed = scraper.checkpoint.load_completed_queries()
        if query_key in completed:
            completed.remove(query_key)
            # Write back without the test key
            temp_path = scraper.checkpoint.completed_queries_path.with_suffix(".json.tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(sorted(completed), f, indent=2)
            temp_path.replace(scraper.checkpoint.completed_queries_path)
            print(f"✅ Cleaned up test checkpoint entry")

        record_result(6, "Search with Minimal Scraping", True)
        return True

    except Exception as e:
        print(f"❌ FAIL: Exception during test - {e}")
        import traceback
        traceback.print_exc()
        record_result(6, "Search with Minimal Scraping", False)
        return False


# =============================================================================
# Test 7: Output Schema Conformance
# =============================================================================

def test_output_schema_conformance() -> bool:
    """
    Test 7 — Output Schema Conformance

    Create 10 fake records and verify schema conformance.
    """
    print_test_header(7, "Output Schema Conformance")

    try:
        from scrapers.checkpoint_manager import TRACECheckpointManager
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(7, "Output Schema Conformance", False)
        return False

    import pandas as pd

    temp_dir = "data/test_schema_checkpoints"
    all_passed = True

    try:
        # Clean up any existing test checkpoint
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        checkpoint = TRACECheckpointManager(checkpoint_dir=temp_dir)

        # Create 10 fake records matching _process_submission output
        fake_records = [
            {
                "source_platform": "Reddit",
                "source_detail": "nba",
                "author": f"user{i}",
                "url": f"https://reddit.com/r/nba/comments/test{i}",
                "text_content": f"Test content for record {i} about achilles injury",
                "created_date": "2020-06-15T14:30:00",
                "engagement_score": 100 + i * 10,
                "engagement_secondary": 20 + i,
                "engagement_tier": "high" if i % 3 == 0 else "medium",
                "relevance_score": 15.0 + i * 2,
                "recovery_phase": ["immediate_post_injury", "surgery_treatment", "rehabilitation", "return_anticipation", "general"][i % 5],
                "mentioned_players": json.dumps(["Kevin Durant", "Klay Thompson"] if i % 2 == 0 else []),
                "is_achilles_related": i % 2 == 0,
                "is_quality_content": True,
                "text_length": 50 + i * 5,
                "year": 2020 + (i % 3),
                "month": (i % 12) + 1,
                "year_month": "2020-01",
                "num_comments_extracted": i * 2,
                "avg_comment_score": 5.0 + i,
                "total_comment_words": 100 + i * 10,
                "num_replies_extracted": 0,
                "avg_reply_likes": 0.0,
                "total_reply_words": 0,
                "body_word_count": 50,
                "fetch_success": True,
                "uploaded_at": None,
            }
            for i in range(10)
        ]

        # Save records
        checkpoint.save_records_batch(fake_records)
        print(f"✅ Saved 10 fake records")

        # Load and verify
        df = checkpoint.load_all_records()

        # Verify 10 rows
        if len(df) != 10:
            print(f"❌ Expected 10 rows, got {len(df)}")
            all_passed = False
        else:
            print(f"✅ DataFrame has 10 rows")

        # Verify all 27 columns
        expected_columns = {
            "source_platform", "source_detail", "author", "url", "text_content",
            "created_date", "engagement_score", "engagement_secondary", "engagement_tier",
            "relevance_score", "recovery_phase", "mentioned_players", "is_achilles_related",
            "is_quality_content", "text_length", "year", "month", "year_month",
            "num_comments_extracted", "avg_comment_score", "total_comment_words",
            "num_replies_extracted", "avg_reply_likes", "total_reply_words",
            "body_word_count", "fetch_success", "uploaded_at",
        }

        missing = expected_columns - set(df.columns)
        if missing:
            print(f"❌ Missing columns: {missing}")
            all_passed = False
        else:
            print(f"✅ All 27 columns present")

        # Verify no nulls in required fields
        required_fields = ["text_content", "created_date", "source_platform", "url"]
        for field in required_fields:
            null_count = df[field].isna().sum()
            if null_count > 0:
                print(f"❌ {field} has {null_count} null values")
                all_passed = False
            else:
                print(f"✅ {field} has no null values")

        # Verify is_achilles_related contains only booleans
        achilles_values = df["is_achilles_related"].unique()
        if not all(isinstance(v, (bool, type(True), type(False))) for v in achilles_values if pd.notna(v)):
            print(f"❌ is_achilles_related contains non-boolean values: {achilles_values}")
            all_passed = False
        else:
            print(f"✅ is_achilles_related contains only boolean values")

    except Exception as e:
        print(f"❌ FAIL: Exception during test - {e}")
        import traceback
        traceback.print_exc()
        all_passed = False

    finally:
        # Clean up temp directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"✅ Cleaned up temp directory: {temp_dir}")

    record_result(7, "Output Schema Conformance", all_passed)
    return all_passed


# =============================================================================
# Main Execution
# =============================================================================

def main() -> None:
    """
    Main test runner - executes all tests in order and prints summary.
    """
    print("=" * 60)
    print("🧪 TRACE REDDIT SCRAPER V2 - INTEGRATION TESTS")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Test 1: Config Import
    test_config_import()

    # Test 2: Relevance Scorer Behavior
    test_relevance_scorer_behavior()

    # Test 3: Checkpoint Manager Lifecycle
    test_checkpoint_manager_lifecycle()

    # Test 4: Reddit Connection
    reddit_passed, reddit_skipped = test_reddit_connection()

    # Initialize scraper for Tests 5-6 if connection succeeded
    scraper = None
    if reddit_passed and not reddit_skipped:
        from scrapers.reddit_scraper_v2 import TRACERedditScraperV2
        from dotenv import load_dotenv
        load_dotenv()
        scraper = TRACERedditScraperV2()
        scraper.setup_connection(os.getenv("REDDIT_CLIENT_ID"), os.getenv("REDDIT_CLIENT_SECRET"))

    # Test 5: Single Submission Processing
    test_single_submission_processing(scraper)

    # Test 6: Search with Minimal Scraping
    test_search_with_minimal_scraping(scraper)

    # Test 7: Output Schema Conformance
    test_output_schema_conformance()

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
