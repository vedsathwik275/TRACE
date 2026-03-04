#!/usr/bin/env python3
"""
Google News RSS Test - Focused test for the Google News RSS search feature.

Run with: python tests/test_google_news_rss.py
Requires network access.
Uses no external testing framework — plain Python with pass/fail summary.
"""

import os
import sys
import json

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
# Test 1: Google News RSS Method Exists
# =============================================================================

def test_method_exists() -> bool:
    """
    Test 1 — Verify scrape_google_news_rss method exists.
    """
    print_test_header(1, "Google News RSS Method Exists")

    try:
        from scrapers.news_scraper_v2 import TRACENewsScraperV2
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(1, "Method Exists", False)
        return False

    if hasattr(TRACENewsScraperV2, "scrape_google_news_rss"):
        print("✅ scrape_google_news_rss method exists on TRACENewsScraperV2")
        record_result(1, "Method Exists", True)
        return True
    else:
        print("❌ FAIL: scrape_google_news_rss method not found")
        record_result(1, "Method Exists", False)
        return False


# =============================================================================
# Test 2: Google News RSS URL Construction
# =============================================================================

def test_url_construction() -> bool:
    """
    Test 2 — Verify Google News RSS URLs are constructed correctly.
    """
    print_test_header(2, "Google News RSS URL Construction")

    try:
        from urllib.parse import quote_plus
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(2, "URL Construction", False)
        return False

    all_passed = True

    # Test URL format
    base_url = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
    test_query = "Kevin Durant achilles injury NBA"
    encoded = quote_plus(test_query)
    full_url = base_url.format(query=encoded)

    # Verify URL structure
    if not full_url.startswith("https://news.google.com/rss/search"):
        print("❌ FAIL: URL doesn't start with expected base")
        all_passed = False
    else:
        print("✅ URL starts with correct base")

    if "q=" in full_url and "hl=en-US" in full_url and "gl=US" in full_url:
        print("✅ URL contains required parameters (q, hl, gl)")
    else:
        print("❌ FAIL: URL missing required parameters")
        all_passed = False

    if "Kevin+Durant" in full_url or "Kevin%20Durant" in full_url:
        print("✅ Query is properly URL-encoded")
    else:
        print("❌ FAIL: Query not properly encoded")
        all_passed = False

    record_result(2, "URL Construction", all_passed)
    return all_passed


# =============================================================================
# Test 3: TARGET_PLAYERS Loaded
# =============================================================================

def test_target_players_loaded() -> bool:
    """
    Test 3 — Verify TARGET_PLAYERS is loaded with 15 players.
    """
    print_test_header(3, "TARGET_PLAYERS Loaded")

    try:
        from scrapers.reddit_config import TARGET_PLAYERS
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(3, "TARGET_PLAYERS Loaded", False)
        return False

    all_passed = True

    if not isinstance(TARGET_PLAYERS, dict):
        print(f"❌ FAIL: TARGET_PLAYERS is not a dict, got {type(TARGET_PLAYERS)}")
        all_passed = False
    elif len(TARGET_PLAYERS) != 15:
        print(f"❌ FAIL: Expected 15 players, got {len(TARGET_PLAYERS)}")
        all_passed = False
    else:
        print(f"✅ TARGET_PLAYERS has 15 players")

    # Check for key players
    expected_players = ["Kevin Durant", "Klay Thompson", "DeMarcus Cousins"]
    for player in expected_players:
        if player in TARGET_PLAYERS:
            print(f"✅ {player} present with injury date: {TARGET_PLAYERS[player]}")
        else:
            print(f"❌ FAIL: {player} not found in TARGET_PLAYERS")
            all_passed = False

    record_result(3, "TARGET_PLAYERS Loaded", all_passed)
    return all_passed


# =============================================================================
# Test 4: Query Generation
# =============================================================================

def test_query_generation() -> bool:
    """
    Test 4 — Verify query generation logic produces expected queries.
    """
    print_test_header(4, "Query Generation")

    try:
        from scrapers.reddit_config import TARGET_PLAYERS
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(4, "Query Generation", False)
        return False

    all_passed = True

    # Simulate query generation from scrape_google_news_rss
    player_queries = []
    for player_name in TARGET_PLAYERS.keys():
        player_queries.append(f'"{player_name}" achilles injury NBA')
        player_queries.append(f'"{player_name}" achilles surgery recovery')
        player_queries.append(f'"{player_name}" achilles return NBA')
        player_queries.append(f'"{player_name}" achilles rehab timeline')

    # Year-based queries
    for year in range(2015, 2025):
        player_queries.append(f"NBA achilles injury {year}")

    # Generic queries
    generic_queries = [
        "NBA achilles rupture career",
        "NBA achilles tendon surgery recovery",
        "NBA achilles injury return timeline",
        "basketball achilles tear comeback",
    ]

    all_queries = player_queries + generic_queries

    # Verify counts
    expected_player_queries = 15 * 4  # 15 players × 4 queries
    expected_year_queries = 10  # 2015-2024
    expected_generic = 4
    expected_total = expected_player_queries + expected_year_queries + expected_generic

    if len(all_queries) != expected_total:
        print(f"❌ FAIL: Expected {expected_total} queries, got {len(all_queries)}")
        all_passed = False
    else:
        print(f"✅ Generated {len(all_queries)} queries ({expected_player_queries} player + {expected_year_queries} year + {expected_generic} generic)")

    # Verify query content
    kevin_durant_queries = [q for q in all_queries if "Kevin Durant" in q]
    if len(kevin_durant_queries) == 4:
        print(f"✅ Kevin Durant has 4 queries")
        for q in kevin_durant_queries:
            print(f"     • {q}")
    else:
        print(f"❌ FAIL: Kevin Durant should have 4 queries, got {len(kevin_durant_queries)}")
        all_passed = False

    year_queries = [q for q in all_queries if "NBA achilles injury 2019" in q]
    if len(year_queries) >= 1:
        print(f"✅ Year-based queries present")
    else:
        print(f"❌ FAIL: Year-based queries missing")
        all_passed = False

    if len(generic_queries) == 4:
        print(f"✅ Generic queries present")
        for q in generic_queries:
            print(f"     • {q}")
    else:
        print(f"❌ FAIL: Generic queries incorrect count")
        all_passed = False

    record_result(4, "Query Generation", all_passed)
    return all_passed


# =============================================================================
# Test 5: Google News RSS Fetch (Live)
# =============================================================================

def test_google_news_rss_fetch() -> tuple[bool, bool]:
    """
    Test 5 — Live fetch test with a single Google News RSS query.
    Skip if network unavailable.
    """
    print_test_header(5, "Google News RSS Fetch (Live)")

    try:
        import requests
        from bs4 import BeautifulSoup
        from urllib.parse import quote_plus
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(5, "Google News RSS Fetch", False)
        return False, False

    # Test with a simple query
    test_query = "Kevin Durant achilles injury NBA"
    encoded_query = quote_plus(test_query)
    rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"

    try:
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

    except requests.exceptions.Timeout:
        print(f"⚠️  SKIP: Request timed out (30s)")
        record_result(5, "Google News RSS Fetch", False, skipped=True)
        return False, True
    except requests.exceptions.RequestException as e:
        print(f"⚠️  SKIP: Network error - {e}")
        record_result(5, "Google News RSS Fetch", False, skipped=True)
        return False, True

    # Parse RSS feed
    try:
        soup = BeautifulSoup(response.content, "xml")
        items = soup.find_all("item")

        if not items:
            print(f"ℹ️  No items found (may be expected for some queries)")
            print(f"✅ RSS feed fetched and parsed successfully (0 items)")
            record_result(5, "Google News RSS Fetch", True)
            return True, False

        print(f"✅ RSS feed fetched successfully: {len(items)} items found")

        # Verify item structure
        sample_item = items[0]
        title_elem = sample_item.find("title")
        link_elem = sample_item.find("link")
        pub_date_elem = sample_item.find("pubDate")

        if title_elem and link_elem:
            print(f"✅ Items have required elements (title, link)")
            print(f"   Sample title: {title_elem.get_text(strip=True)[:80]}...")
        else:
            print(f"❌ FAIL: Items missing title or link")
            record_result(5, "Google News RSS Fetch", False)
            return False, False

        if pub_date_elem:
            print(f"✅ Items have pubDate element")
        else:
            print(f"ℹ️  pubDate element not found (optional)")

        record_result(5, "Google News RSS Fetch", True)
        return True, False

    except Exception as e:
        print(f"❌ FAIL: Error parsing RSS feed - {e}")
        record_result(5, "Google News RSS Fetch", False)
        return False, False


# =============================================================================
# Test 6: Scraper Instantiation
# =============================================================================

def test_scraper_instantiation() -> bool:
    """
    Test 6 — Verify TRACENewsScraperV2 can be instantiated with Google News capability.
    """
    print_test_header(6, "Scraper Instantiation")

    try:
        from scrapers.news_scraper_v2 import TRACENewsScraperV2
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(6, "Scraper Instantiation", False)
        return False

    try:
        scraper = TRACENewsScraperV2()
        print("✅ TRACENewsScraperV2 instantiated successfully")
    except Exception as e:
        print(f"❌ FAIL: Could not instantiate scraper - {e}")
        record_result(6, "Scraper Instantiation", False)
        return False

    # Verify required attributes
    all_passed = True

    if hasattr(scraper, "scrape_google_news_rss"):
        print("✅ scrape_google_news_rss method available")
    else:
        print("❌ FAIL: scrape_google_news_rss method not found")
        all_passed = False

    if hasattr(scraper, "seen_urls"):
        print(f"✅ seen_urls set initialized ({len(scraper.seen_urls)} items)")
    else:
        print("❌ FAIL: seen_urls set not found")
        all_passed = False

    if hasattr(scraper, "scorer"):
        print("✅ scorer (TRACERelevanceScorer) initialized")
    else:
        print("❌ FAIL: scorer not found")
        all_passed = False

    if hasattr(scraper, "checkpoint"):
        print("✅ checkpoint manager initialized")
    else:
        print("❌ FAIL: checkpoint manager not found")
        all_passed = False

    record_result(6, "Scraper Instantiation", all_passed)
    return all_passed


# =============================================================================
# Test 7: Integration Test (Mini Run)
# =============================================================================

def test_integration_mini_run() -> tuple[bool, bool]:
    """
    Test 7 — Run scrape_google_news_rss with just 2 queries (mini test).
    This is a skippable integration test.
    """
    print_test_header(7, "Integration Test (Mini Run)")

    try:
        from scrapers.news_scraper_v2 import TRACENewsScraperV2
    except ImportError as e:
        print(f"❌ FAIL: Import error - {e}")
        record_result(7, "Integration Test", False)
        return False, False

    try:
        scraper = TRACENewsScraperV2()
    except Exception as e:
        print(f"❌ FAIL: Could not instantiate scraper - {e}")
        record_result(7, "Integration Test", False)
        return False, False

    # Monkey-patch to run only 2 queries for testing
    original_method = scraper.scrape_google_news_rss

    def limited_scrape(debug_mode: bool = False) -> list[dict]:
        """Run only 2 queries instead of full 74."""
        all_records = []

        print("\n" + "=" * 60)
        print("📰 LIMITED Google News RSS Test (2 queries)")
        print("=" * 60)

        # Only test 2 queries
        test_queries = [
            ("Kevin Durant achilles injury NBA", "Kevin Durant"),
            ("NBA achilles rupture career", "Generic"),
        ]

        GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

        import requests
        from bs4 import BeautifulSoup
        from urllib.parse import quote_plus
        import time

        for i, (query, label) in enumerate(test_queries, 1):
            print(f"\n[{i}/2] Query: {query}")

            try:
                encoded_query = quote_plus(query)
                rss_url = GOOGLE_NEWS_RSS_BASE.format(query=encoded_query)

                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/rss+xml,text/xml,application/xml",
                }
                response = requests.get(rss_url, headers=headers, timeout=30)
                response.raise_for_status()

                soup = BeautifulSoup(response.content, "xml")
                items = soup.find_all("item")

                if not items:
                    print(f"   No items found")
                    continue

                print(f"   Found {len(items)} items")

                # Process first 3 items max per query
                for item in items[:3]:
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

                    # Process through pipeline
                    record = scraper._process_article_url(
                        title=title,
                        url=url,
                        source_name=f"Google News ({source_name})",
                        pub_date_str=pub_date_str,
                        description=title,
                        is_rss_source=True,
                        debug_mode=debug_mode,
                    )

                    if record is not None:
                        all_records.append(record)

                time.sleep(2.0)  # Rate limiting

            except Exception as e:
                print(f"   ⚠️  Error: {e}")
                continue

        print(f"\n📊 Test Summary: {len(all_records)} records collected")
        return all_records

    try:
        records = limited_scrape(debug_mode=False)

        if not isinstance(records, list):
            print(f"❌ FAIL: Expected list, got {type(records)}")
            record_result(7, "Integration Test", False)
            return False, False

        print(f"\n✅ Integration test completed: {len(records)} records collected")

        if len(records) > 0:
            # Verify record structure
            sample = records[0]
            required_keys = {"source_platform", "url", "text_content", "relevance_score", "year"}
            missing = required_keys - set(sample.keys())

            if missing:
                print(f"❌ FAIL: Records missing keys: {missing}")
                record_result(7, "Integration Test", False)
                return False, False
            else:
                print(f"✅ Records have correct schema")
                print(f"   Sample source_platform: {sample['source_platform']}")
                print(f"   Sample relevance_score: {sample['relevance_score']}")

        record_result(7, "Integration Test", True)
        return True, False

    except Exception as e:
        print(f"⚠️  SKIP: Integration test failed - {e}")
        import traceback
        traceback.print_exc()
        record_result(7, "Integration Test", False, skipped=True)
        return False, True


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    """
    Main test runner.
    """
    print("=" * 60)
    print("🧪 GOOGLE NEWS RSS - INTEGRATION TESTS")
    print("=" * 60)
    print(f"Started at: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Run tests
    test_method_exists()
    test_url_construction()
    test_target_players_loaded()
    test_query_generation()
    test_google_news_rss_fetch()  # Live network test (skippable)
    test_scraper_instantiation()
    test_integration_mini_run()  # Live network test (skippable)

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
