# validate_news_v2.py
"""
Quality assurance validation script for TRACE News Scraper V2 output.

Runs 10 named validation checks on the most recent V2 news CSV file.
Mirrors the structure of the Reddit validator (validate_reddit_v2.py).
"""

import os
import sys
import json
import glob
from datetime import datetime

import pandas as pd

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.news_config import MIN_ARTICLE_WORD_COUNT


# Valid recovery phases from TRACERelevanceScorer
VALID_RECOVERY_PHASES = {
    "immediate_post_injury",
    "surgery_treatment",
    "rehabilitation",
    "return_anticipation",
    "general",
}

# Expected 27 columns from unified schema
EXPECTED_COLUMNS = {
    "source_platform",
    "source_detail",
    "author",
    "url",
    "text_content",
    "created_date",
    "engagement_score",
    "engagement_secondary",
    "engagement_tier",
    "relevance_score",
    "recovery_phase",
    "mentioned_players",
    "is_achilles_related",
    "is_quality_content",
    "text_length",
    "year",
    "month",
    "year_month",
    "num_comments_extracted",
    "avg_comment_score",
    "total_comment_words",
    "num_replies_extracted",
    "avg_reply_likes",
    "total_reply_words",
    "body_word_count",
    "fetch_success",
    "uploaded_at",
}


def find_latest_v2_csv() -> str | None:
    """
    Find the most recent CSV file in data/ starting with 'trace_news_v2'.

    Returns:
        Full path to the file, or None if not found.
    """
    pattern = "data/trace_news_v2*.csv"
    files = glob.glob(pattern)

    if not files:
        return None

    # Sort by modification time, most recent first
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def check_record_count(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 1: Verify at least 300 total rows.
    Warn (but not fail) if under 1000.
    """
    count = len(df)
    if count >= 300:
        if count < 1000:
            return True, f"PASS ({count} rows - meets minimum, consider collecting more for better coverage)"
        return True, f"PASS ({count} rows)"
    return False, f"FAIL ({count} rows - minimum 300 required)"


def check_achilles_rate(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 2: Verify at least 35% of records have is_achilles_related == True.
    News sources tend to have slightly lower rates than Reddit due to broader coverage.
    """
    if "is_achilles_related" not in df.columns:
        return False, "FAIL (column missing)"

    achilles_count = int(df["is_achilles_related"].sum())
    rate = (achilles_count / len(df)) * 100 if len(df) > 0 else 0

    if rate >= 35:
        return True, f"PASS ({rate:.1f}% achilles-related)"
    return False, f"FAIL ({rate:.1f}% achilles-related - need ≥35%)"


def check_relevance_threshold(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 3: Verify every row has relevance_score >= 15.0.
    Show count of failing rows and 3 worst offenders if any fail.
    """
    if "relevance_score" not in df.columns:
        return False, "FAIL (column missing)"

    failing = df[df["relevance_score"] < 15.0]
    failing_count = len(failing)

    if failing_count == 0:
        min_score = df["relevance_score"].min()
        return True, f"PASS (all scores ≥ 15.0, min = {min_score:.2f})"

    # Get 3 worst offenders
    worst = failing.nsmallest(3, "relevance_score")
    worst_info = []
    for _, row in worst.iterrows():
        worst_info.append(f"score={row['relevance_score']:.2f}, url={row['url'][:50]}...")

    return False, f"FAIL ({failing_count} rows below threshold). Worst: {'; '.join(worst_info)}"


def check_temporal_coverage(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 4: Verify records span at least 5 distinct calendar years.
    """
    if "year" not in df.columns:
        return False, "FAIL (column missing)"

    unique_years = df["year"].nunique()

    if unique_years >= 5:
        years_list = sorted(df["year"].unique())
        return True, f"PASS ({unique_years} years: {years_list})"
    return False, f"FAIL (only {unique_years} distinct years - need ≥5)"


def check_player_coverage(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 5: Parse mentioned_players JSON, verify at least 8 distinct players.
    News coverage should have broader player representation than Reddit.
    """
    if "mentioned_players" not in df.columns:
        return False, "FAIL (column missing)"

    all_players: set[str] = set()
    for players_json in df["mentioned_players"]:
        try:
            players = json.loads(players_json)
            if isinstance(players, list):
                all_players.update(players)
        except (json.JSONDecodeError, TypeError):
            pass

    unique_count = len(all_players)

    if unique_count >= 8:
        return True, f"PASS ({unique_count} unique players)"
    return False, f"FAIL (only {unique_count} unique players - need ≥8)"


def check_no_nulls(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 6: Verify text_content has zero null or empty string values.
    """
    if "text_content" not in df.columns:
        return False, "FAIL (column missing)"

    null_count = df["text_content"].isna().sum()
    empty_count = (df["text_content"] == "").sum()
    total_issues = int(null_count + empty_count)

    if total_issues == 0:
        return True, "PASS (no null or empty values)"
    return False, f"FAIL ({total_issues} issues: {null_count} null, {empty_count} empty)"


def check_no_dupes(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 7: Verify url column has zero duplicate values.
    """
    if "url" not in df.columns:
        return False, "FAIL (column missing)"

    total_urls = len(df)
    unique_urls = df["url"].nunique()
    dupe_count = total_urls - unique_urls

    if dupe_count == 0:
        return True, f"PASS ({unique_urls} unique URLs)"
    return False, f"FAIL ({dupe_count} duplicate URLs found)"


def check_schema_complete(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 8: Verify all 27 columns from unified schema are present.
    """
    actual_columns = set(df.columns)
    missing = EXPECTED_COLUMNS - actual_columns

    if not missing:
        return True, f"PASS (all {len(EXPECTED_COLUMNS)} columns present)"
    return False, f"FAIL (missing: {', '.join(sorted(missing))})"


def check_fetch_quality(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 9: Verify at least 30% of records have fetch_success True
    AND body_word_count above MIN_ARTICLE_WORD_COUNT.
    Print the actual fetch success rate.
    """
    if "fetch_success" not in df.columns or "body_word_count" not in df.columns:
        return False, "FAIL (columns missing)"

    # Count records with successful fetch AND adequate word count
    quality_records = df[
        (df["fetch_success"] == True) &
        (df["body_word_count"] >= MIN_ARTICLE_WORD_COUNT)
    ]

    quality_count = len(quality_records)
    quality_rate = (quality_count / len(df)) * 100 if len(df) > 0 else 0

    if quality_rate >= 30:
        return True, f"PASS ({quality_rate:.1f}% full-article quality)"
    return False, f"FAIL ({quality_rate:.1f}% full-article quality - need ≥30%)"


def check_source_diversity(df: pd.DataFrame) -> tuple[bool, str]:
    """
    Check 10: Verify at least 4 distinct values in source_detail.
    """
    if "source_detail" not in df.columns:
        return False, "FAIL (column missing)"

    unique_sources = df["source_detail"].nunique()

    if unique_sources >= 4:
        sources_list = sorted(df["source_detail"].unique())
        return True, f"PASS ({unique_sources} sources: {sources_list})"
    return False, f"FAIL (only {unique_sources} sources - need ≥4)"


def main() -> None:
    """
    Main validation function.
    """
    print("=" * 60)
    print("🔍 TRACE NEWS SCRAPER V2 - DATA VALIDATION")
    print("=" * 60)

    # Find latest V2 CSV
    csv_path = find_latest_v2_csv()

    if csv_path is None:
        print("\n❌ ERROR: No CSV file found matching 'data/trace_news_v2*.csv'")
        print("   Please run runners/ns_run_v2.py first to generate data.")
        sys.exit(1)

    print(f"\n📁 Validating: {csv_path}")
    print(f"📅 File modified: {datetime.fromtimestamp(os.path.getmtime(csv_path)).strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"\n❌ ERROR: Failed to read CSV file: {e}")
        sys.exit(1)

    print(f"📊 Loaded {len(df)} records, {len(df.columns)} columns")

    # Run validation checks
    print("\n" + "=" * 60)
    print("📋 RUNNING VALIDATION CHECKS")
    print("=" * 60)

    checks = [
        ("RECORD_COUNT", check_record_count),
        ("ACHILLES_RATE", check_achilles_rate),
        ("RELEVANCE_THRESHOLD", check_relevance_threshold),
        ("TEMPORAL_COVERAGE", check_temporal_coverage),
        ("PLAYER_COVERAGE", check_player_coverage),
        ("NO_NULLS", check_no_nulls),
        ("NO_DUPES", check_no_dupes),
        ("SCHEMA_COMPLETE", check_schema_complete),
        ("FETCH_QUALITY", check_fetch_quality),
        ("SOURCE_DIVERSITY", check_source_diversity),
    ]

    passed = 0
    failed = []

    for i, (name, check_fn) in enumerate(checks, 1):
        success, message = check_fn(df)
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"\nCheck {i}: {name}")
        print(f"  {status}: {message}")

        if success:
            passed += 1
        else:
            failed.append(name)

    # Final summary
    print("\n" + "=" * 60)
    print(f"📊 FINAL RESULT: {passed}/10 checks passed")
    print("=" * 60)

    if failed:
        print("\n⚠️  FAILED CHECKS (fix in priority order before Phase 2):")
        priority_order = [
            "SCHEMA_COMPLETE",  # Must have correct schema
            "NO_NULLS",  # Data integrity
            "NO_DUPES",  # Data integrity
            "RELEVANCE_THRESHOLD",  # Core filtering logic
            "FETCH_QUALITY",  # Article extraction quality
            "SOURCE_DIVERSITY",  # Collection breadth
            "ACHILLES_RATE",  # Collection quality
            "TEMPORAL_COVERAGE",  # Collection scope
            "PLAYER_COVERAGE",  # Collection scope
            "RECORD_COUNT",  # May just need more collection
        ]

        # Sort failed checks by priority
        sorted_failed = sorted(failed, key=lambda x: priority_order.index(x) if x in priority_order else 999)

        for i, check_name in enumerate(sorted_failed, 1):
            print(f"  {i}. {check_name}")

        print("\n💡 Fix these issues before proceeding to Phase 2 (Supabase upload).")
    else:
        print("\n✅ ALL CHECKS PASSED - Data is ready for Phase 2!")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
