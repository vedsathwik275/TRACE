# rs_run_v2.py
"""
Runner script for TRACE Reddit Scraper V2.

Executes the improved historical scraper with checkpointing and relevance scoring.
"""

import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.reddit_scraper_v2 import TRACERedditScraperV2


def main() -> None:
    """
    Main execution function for Reddit scraper V2.
    """
    # Load environment variables
    load_dotenv()

    # Retrieve Reddit credentials
    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")

    if not client_id:
        print("❌ Error: REDDIT_CLIENT_ID not found in .env file")
        sys.exit(1)

    if not client_secret:
        print("❌ Error: REDDIT_CLIENT_SECRET not found in .env file")
        sys.exit(1)

    # Initialize scraper
    print("🚀 Initializing TRACE Reddit Scraper V2...")
    scraper = TRACERedditScraperV2()

    # Set up connection
    print("\n" + "=" * 60)
    if not scraper.setup_connection(client_id, client_secret):
        print("❌ Failed to establish Reddit connection. Exiting.")
        sys.exit(1)

    # Run Phase 1 collection
    print("\n" + "=" * 60)
    print("🔍 Starting Phase 1 collection...")
    print("=" * 60)

    df = scraper.run_phase1_collection()

    # Check if we have results
    if df is None or df.empty:
        print("\n⚠️  No records collected. Exiting without saving.")
        sys.exit(0)

    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/trace_reddit_v2_data_{timestamp}.csv"

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
    print("📊 TRACE REDDIT SCRAPER V2 - SUMMARY REPORT")
    print("=" * 60)

    # Total record count
    print(f"\n📈 TOTAL RECORDS: {len(df)}")

    # Records by recovery phase
    print("\n📋 RECORDS BY RECOVERY PHASE:")
    phase_counts = df["recovery_phase"].value_counts().sort_index()
    for phase, count in phase_counts.items():
        print(f"   • {phase}: {count}")

    # Records by year (chronological)
    print("\n📅 RECORDS BY YEAR:")
    year_counts = df["year"].value_counts().sort_index()
    for year, count in year_counts.items():
        print(f"   • {year}: {count}")

    # Top 10 mentioned players
    print("\n🏀 TOP 10 MENTIONED PLAYERS:")
    all_players: list[str] = []
    for players_json in df["mentioned_players"]:
        try:
            import json
            players = json.loads(players_json)
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

    # Records with relevance_score > 20
    high_relevance_count = int((df["relevance_score"] > 20).sum())
    print(f"\n⭐ HIGH RELEVANCE RECORDS (score > 20): {high_relevance_count}")

    # Confirm all records have source_platform == "Reddit"
    all_reddit = (df["source_platform"] == "Reddit").all()
    print(f"\n📱 SOURCE PLATFORM: {'✅ All records are Reddit' if all_reddit else '⚠️ Mixed sources detected'}")

    # =====================================================================
    # CHECKPOINT MESSAGE
    # =====================================================================
    print("\n" + "=" * 60)
    print("💾 CHECKPOINT STATUS")
    print("=" * 60)
    print("✅ Checkpoint data is preserved in data/checkpoints/")
    print("✅ This run can be safely resumed if interrupted")
    print("✅ Simply run the script again to continue from where it left off")
    print("\n" + "=" * 60)
    print("✅ PHASE 1 COLLECTION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
