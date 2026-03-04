# bs_run_v2.py
"""
Runner script for TRACE Bluesky Scraper V2.

Executes the improved historical scraper with checkpointing and relevance scoring.
"""

import os
import sys
import json
from datetime import datetime
from collections import Counter

import pandas as pd
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.bluesky_scraper_v2 import TRACEBlueskyScraperV2


def main() -> None:
    """
    Main execution function for Bluesky scraper V2.
    """
    # Load environment variables
    load_dotenv()

    # Retrieve Bluesky credentials
    handle = os.getenv("BLUESKY_HANDLE")
    app_password = os.getenv("BLUESKY_APP_PASSWORD")

    if not handle:
        print("❌ Error: BLUESKY_HANDLE not found in .env file")
        sys.exit(1)

    if not app_password:
        print("❌ Error: BLUESKY_APP_PASSWORD not found in .env file")
        sys.exit(1)

    # Initialize scraper
    print("🚀 Initializing TRACE Bluesky Scraper V2...")
    scraper = TRACEBlueskyScraperV2()

    # Set up connection
    print("\n" + "=" * 60)
    if not scraper.login(handle, app_password):
        print("❌ Failed to establish Bluesky connection. Exiting.")
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
    output_path = f"data/trace_bluesky_v2_data_{timestamp}.csv"

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
    print("📊 TRACE BLUESKY SCRAPER V2 - SUMMARY REPORT")
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
            players = json.loads(players_json)
            all_players.extend(players)
        except (json.JSONDecodeError, TypeError):
            pass

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

    # Confirm all records have source_platform == "Bluesky"
    all_bluesky = (df["source_platform"] == "Bluesky").all()
    print(f"\n📱 SOURCE PLATFORM: {'✅ All records are Bluesky' if all_bluesky else '⚠️ Mixed sources detected'}")

    # =====================================================================
    # CHECKPOINT MESSAGE
    # =====================================================================
    print("\n" + "=" * 60)
    print("💾 CHECKPOINT STATUS")
    print("=" * 60)
    print("✅ Checkpoint data is preserved in data/bluesky_checkpoints/")
    print("✅ This run can be safely resumed if interrupted")
    print("✅ Simply run the script again to continue from where it left off")
    print("\n" + "=" * 60)
    print("✅ PHASE 1 COLLECTION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
