# ns_run_v2.py
"""
Runner script for TRACE News Scraper V2.

Executes the improved historical news scraper with Google News search,
full article fetching, and checkpointing.
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
    Main execution function for News scraper V2.
    
    Usage:
        python runners/ns_run_v2.py              # Normal mode
        python runners/ns_run_v2.py --debug      # Debug mode (print filtered items)
    """
    # Load environment variables
    load_dotenv()

    # Check for debug mode flag
    debug_mode = "--debug" in sys.argv

    if debug_mode:
        print("🔍 DEBUG MODE ENABLED: Will print all filtered RSS items\n")

    # Initialize scraper
    print("🚀 Initializing TRACE News Scraper V2...")
    scraper = TRACENewsScraperV2()

    # Run Phase 1 collection
    print("\n" + "=" * 60)
    print("🔍 Starting Phase 1 news collection...")
    print("=" * 60)

    df = scraper.run_phase1_collection(debug_mode=debug_mode)

    # Check if we have results
    if df is None or df.empty:
        print("\n⚠️  No records collected. Exiting without saving.")
        sys.exit(0)

    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/trace_news_v2_data_{timestamp}.csv"

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
    print("📊 TRACE NEWS SCRAPER V2 - SUMMARY REPORT")
    print("=" * 60)

    # Total record count
    print(f"\n📈 TOTAL RECORDS: {len(df)}")

    # Breakdown by source_detail
    print("\n📰 RECORDS BY SOURCE:")
    source_counts = df["source_detail"].value_counts().sort_values(ascending=False)
    for source, count in source_counts.items():
        print(f"   • {source}: {count}")

    # Breakdown by year (chronological)
    print("\n📅 RECORDS BY YEAR:")
    year_counts = df["year"].value_counts().sort_index()
    for year, count in year_counts.items():
        print(f"   • {year}: {count}")

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
    print("✅ PHASE 1 NEWS COLLECTION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
