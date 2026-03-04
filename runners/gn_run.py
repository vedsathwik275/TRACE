#!/usr/bin/env python3
"""
Runner script for Google News RSS scraper only.

Executes the Google News RSS historical search to collect NBA Achilles injury
articles from 2015-2026. Uses checkpointing for resumability.

Usage:
    python runners/gn_run.py              # Normal mode
    python runners/gn_run.py --debug      # Debug mode (print filtered items)
"""

import os
import sys
import json
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.news_scraper_v2 import TRACENewsScraperV2


def main() -> None:
    """
    Main execution function for Google News RSS scraper.
    """
    # Load environment variables
    load_dotenv()

    # Check for debug mode flag
    debug_mode = "--debug" in sys.argv

    if debug_mode:
        print("🔍 DEBUG MODE ENABLED: Will print all filtered items\n")

    # Initialize scraper
    print("🚀 Initializing TRACE Google News RSS Scraper...")
    scraper = TRACENewsScraperV2()

    # Run Google News RSS collection
    print("\n" + "=" * 60)
    print("🔍 Starting Google News RSS historical search...")
    print("=" * 60)

    records = scraper.scrape_google_news_rss(debug_mode=debug_mode)

    # Convert list to DataFrame
    if records:
        df = pd.DataFrame(records)
    else:
        df = pd.DataFrame()

    # Check if we have results
    if df is None or df.empty:
        print("\n⚠️  No records collected. Exiting without saving.")
        sys.exit(0)

    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/trace_news_gnews_{timestamp}.csv"

    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)

    # Save to CSV
    print(f"\n💾 Saving {len(df)} records to {output_path}...")
    df.to_csv(output_path, index=False)
    print(f"✅ CSV saved successfully")

    # =====================================================================
    # SUMMARY REPORT
    # =====================================================================
    print("\n" + "=" * 60)
    print("📊 GOOGLE NEWS RSS SCRAPER - SUMMARY REPORT")
    print("=" * 60)

    # Total record count
    print(f"\n📈 TOTAL RECORDS: {len(df)}")

    # Breakdown by source_detail (Google News sources)
    print("\n📰 RECORDS BY SOURCE:")
    source_counts = df["source_detail"].value_counts().sort_values(ascending=False)
    for source, count in source_counts.items():
        print(f"   • {source}: {count}")

    # Breakdown by year (chronological) with historical emphasis
    print("\n📅 RECORDS BY YEAR:")
    year_counts = df["year"].value_counts().sort_index()
    historical_count = 0
    recent_count = 0
    for year, count in year_counts.items():
        marker = ""
        if 2015 <= year <= 2024:
            historical_count += count
            marker = " (historical)"
        else:
            recent_count += count
        print(f"   • {year}: {count}{marker}")
    print(f"\n   Historical (2015-2024): {historical_count} records")
    print(f"   Recent (2025-2026): {recent_count} records")

    # Top 10 mentioned players
    print("\n🏀 TOP 10 MENTIONED PLAYERS:")
    all_players: list[str] = []
    for players_json in df["mentioned_players"]:
        try:
            players = json.loads(players_json)
            if isinstance(players, list):
                all_players.extend(players)
        except (json.JSONDecodeError, TypeError):
            pass

    from collections import Counter
    player_counts = Counter(all_players)
    top_10 = player_counts.most_common(10)

    if top_10:
        for i, (player, count) in enumerate(top_10, 1):
            print(f"   {i:2d}. {player}: {count} mentions")
        print(f"\n   Total unique players: {len(player_counts)}")
    else:
        print("   (No players mentioned)")

    # Achilles-related count and percentage
    achilles_count = int(df["is_achilles_related"].sum())
    achilles_pct = (achilles_count / len(df)) * 100 if len(df) > 0 else 0
    print(f"\n🏥 ACHILLES-RELATED RECORDS: {achilles_count} ({achilles_pct:.1f}%)")

    # Full article fetch success rate
    fetch_success_count = int(df["fetch_success"].sum())
    fetch_success_rate = (fetch_success_count / len(df)) * 100 if len(df) > 0 else 0
    print(f"\n📄 FULL ARTICLE FETCH SUCCESS: {fetch_success_count} ({fetch_success_rate:.1f}%)")

    # Records with relevance_score > 20
    high_relevance_count = int((df["relevance_score"] > 20).sum())
    print(f"\n⭐ HIGH RELEVANCE RECORDS (score > 20): {high_relevance_count}")

    # Records by month (to show distribution)
    print("\n📅 RECORDS BY MONTH (most recent 12):")
    df["year_month"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    month_counts = df["year_month"].value_counts().sort_index(ascending=False).head(12)
    for month, count in month_counts.items():
        print(f"   • {month}: {count}")

    # =====================================================================
    # CHECKPOINT MESSAGE
    # =====================================================================
    print("\n" + "=" * 60)
    print("💾 CHECKPOINT STATUS")
    print("=" * 60)
    print(f"✅ Checkpoint data stored in: data/news_checkpoints/")
    print("✅ This run can be safely resumed if interrupted")
    print("✅ Simply run the script again to continue from where it left off")
    print("\n" + "=" * 60)
    print("✅ GOOGLE NEWS RSS COLLECTION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
