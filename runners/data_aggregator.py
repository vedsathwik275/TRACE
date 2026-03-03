# data_aggregator.py
import pandas as pd
import os
from datetime import datetime
import numpy as np

def load_and_aggregate_data(data_folder_path: str = 'data/'):
    """
    Loads data from separate CSV files in the data folder and aggregates them into a unified format.
    """
    print("🧩 Starting Data Aggregation...")
    print("-" * 40)

    all_dataframes = []
    filenames_found = []

    # Look for data files from each source
    for filename in os.listdir(data_folder_path):
        filepath = os.path.join(data_folder_path, filename)

        if filename.startswith('trace_reddit_data_') and filename.endswith('.csv'):
            print(f"📂 Loading Reddit data: {filename}")
            df_source = pd.read_csv(filepath)
            # --- Map Reddit-specific columns to the unified schema ---
            df_mapped = pd.DataFrame()
            df_mapped['source_platform'] = 'Reddit'
            df_mapped['source_detail'] = df_source['subreddit']
            df_mapped['author'] = df_source['author']
            df_mapped['url'] = df_source['permalink']
            df_mapped['text_content'] = df_source['combined_text'] # Using combined text including comments
            df_mapped['created_date'] = pd.to_datetime(df_source['created_date'], errors='coerce')
            df_mapped['engagement_score'] = df_source['score']
            df_mapped['engagement_secondary'] = df_source['num_comments']
            df_mapped['relevance_score'] = df_source['total_relevance_score']
            df_mapped['recovery_phase'] = df_source['recovery_phase']
            df_mapped['mentioned_players'] = df_source['mentioned_players'].apply(lambda x: x if isinstance(x, list) else []) # Keep as list initially
            df_mapped['is_achilles_related'] = df_source['text'].str.contains('achilles|achillies', case=False, na=False) | \
                                               df_source['title'].str.contains('achilles|achillies', case=False, na=False)
            df_mapped['is_quality_content'] = True # Assume quality for now
            df_mapped['scraped_date'] = datetime.now()
            df_mapped['text_length'] = df_mapped['text_content'].apply(lambda x: len(str(x)) if pd.notna(x) else 0)
            df_mapped['year'] = df_mapped['created_date'].dt.year
            df_mapped['month'] = df_mapped['created_date'].dt.month
            df_mapped['year_month'] = df_mapped['created_date'].dt.to_period('M')
            # Handle engagement tiers (example logic, adjust as needed)
            median_engagement = df_mapped['engagement_score'].median()
            df_mapped['engagement_tier'] = df_mapped['engagement_score'].apply(
                lambda x: 'high' if x > median_engagement * 1.5 else ('medium' if x > median_engagement else 'low')
            )
            all_dataframes.append(df_mapped)
            filenames_found.append(filename)

        elif filename.startswith('trace_news_data_') and filename.endswith('.csv'):
            print(f"📂 Loading News data: {filename}")
            df_source = pd.read_csv(filepath)
            # --- Map News-specific columns to the unified schema ---
            df_mapped = pd.DataFrame()
            df_mapped['source_platform'] = df_source['source']
            df_mapped['source_detail'] = 'RSS Feed' # Or specific feed name if available
            df_mapped['author'] = 'N/A' # News articles often don't have author in RSS
            df_mapped['url'] = df_source['url']
            df_mapped['text_content'] = df_source['body'] # Use the full body if available, otherwise title
            df_mapped['text_content'] = df_mapped['text_content'].fillna(df_source['title']) # Fallback to title
            df_mapped['created_date'] = pd.to_datetime(df_source['pub_date'], errors='coerce')
            df_mapped['engagement_score'] = df_source['relevance_score'] # Could be views, shares later
            df_mapped['engagement_secondary'] = 0 # Usually not in RSS
            df_mapped['relevance_score'] = df_source['relevance_score']
            df_mapped['recovery_score'] = 'news_general' # Placeholder
            df_mapped['mentioned_players'] = df_source['mentioned_players'].apply(lambda x: eval(x) if isinstance(x, str) and x.startswith('[') else []) # Convert string representation back to list
            df_mapped['is_achilles_related'] = df_source['title'].str.contains('achilles|achillies', case=False, na=False) | \
                                               df_source['body'].str.contains('achilles|achillies', case=False, na=False)
            df_mapped['is_quality_content'] = True
            df_mapped['scraped_date'] = pd.to_datetime(df_source['scraped_date'], errors='coerce')
            df_mapped['text_length'] = df_mapped['text_content'].apply(lambda x: len(str(x)) if pd.notna(x) else 0)
            df_mapped['year'] = df_mapped['created_date'].dt.year
            df_mapped['month'] = df_mapped['created_date'].dt.month
            df_mapped['year_month'] = df_mapped['created_date'].dt.to_period('M')
            # Example engagement tier logic for news
            median_relevance = df_mapped['relevance_score'].median()
            df_mapped['engagement_tier'] = df_mapped['relevance_score'].apply(
                lambda x: 'high' if x > median_relevance * 1.5 else ('medium' if x > median_relevance else 'low')
            )
            all_dataframes.append(df_mapped)
            filenames_found.append(filename)

        elif filename.startswith('trace_bluesky_data_') and filename.endswith('.csv'):
            print(f"🦋 Loading Bluesky data: {filename}")
            df_source = pd.read_csv(filepath)
            # --- Map Bluesky-specific columns to the unified schema ---
            df_mapped = pd.DataFrame()
            df_mapped['source_platform'] = 'Bluesky'
            df_mapped['source_detail'] = 'Post Search' # Or specific hashtag if known
            df_mapped['author'] = df_source['author_handle']
            df_mapped['url'] = df_source['url']
            df_mapped['text_content'] = df_source['combined_text'] # Using combined text including replies
            df_mapped['created_date'] = pd.to_datetime(df_source['created_at'], errors='coerce')
            df_mapped['engagement_score'] = df_source['like_count']
            df_mapped['engagement_secondary'] = df_source['repost_count']
            df_mapped['relevance_score'] = 0 # Might calculate based on search query match later
            df_mapped['recovery_phase'] = 'fan_discussion' # Placeholder
            df_mapped['mentioned_players'] = '' # Bluesky scraper didn't capture players, leave empty string or list
            df_mapped['is_achilles_related'] = df_source['text'].str.contains('achilles|achillies', case=False, na=False)
            df_mapped['is_quality_content'] = True
            df_mapped['scraped_date'] = datetime.now() # Or add scraped_date column to bluesky scraper
            df_mapped['text_length'] = df_mapped['text_content'].apply(lambda x: len(str(x)) if pd.notna(x) else 0)
            df_mapped['year'] = df_mapped['created_date'].dt.year
            df_mapped['month'] = df_mapped['created_date'].dt.month
            df_mapped['year_month'] = df_mapped['created_date'].dt.to_period('M')
            # Example engagement tier logic for Bluesky
            median_engagement = df_mapped['engagement_score'].median()
            df_mapped['engagement_tier'] = df_mapped['engagement_score'].apply(
                lambda x: 'high' if x > median_engagement * 1.5 else ('medium' if x > median_engagement else 'low')
            )
            all_dataframes.append(df_mapped)
            filenames_found.append(filename)

    if not all_dataframes:
        print("❌ No source data files found in the 'data' folder to aggregate.")
        print("    Expected files like: trace_reddit_data_*.csv, trace_news_data_*.csv, trace_bluesky_data_*.csv")
        return pd.DataFrame()

    print(f"\n📋 Found data from {len(filenames_found)} files: {filenames_found}")
    print("🔗 Combining dataframes...")

    # Concatenate all mapped dataframes
    unified_df = pd.concat(all_dataframes, ignore_index=True)

    print(f"✅ Combined dataframe has {len(unified_df)} rows.")

    # --- Post-Processing & Enhancements for Unified Schema ---
    print("\n🛠️ Post-processing unified data...")
    # 1. Ensure created_date is datetime
    unified_df['created_date'] = pd.to_datetime(unified_df['created_date'], errors='coerce')
    # 2. Handle mentioned_players as JSON string (for Supabase compatibility)
    unified_df['mentioned_players'] = unified_df['mentioned_players'].apply(
        lambda x: str(x) if isinstance(x, list) else x # Convert list to string representation
    )
    # 3. Fill any remaining NaN in critical columns
    unified_df = unified_df.fillna({
        'author': 'Unknown',
        'source_detail': 'Unknown',
        'url': '',
        'recovery_phase': 'general',
        'engagement_tier': 'none',
        'text_content': '',
        'mentioned_players': '[]' # Default to empty JSON array string
    })

    # 4. Ensure boolean columns are correctly typed
    unified_df['is_achilles_related'] = unified_df['is_achilles_related'].astype(bool)
    unified_df['is_quality_content'] = unified_df['is_quality_content'].astype(bool)

    print("✅ Post-processing complete.")

    # Save the unified dataframe
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f'{data_folder_path}trace_unified_data_{timestamp}.csv'
    unified_df.to_csv(output_filename, index=False)
    print(f"\n💾 Unified data saved to: {output_filename}")
    print(f"📊 Final unified dataframe shape: {unified_df.shape}")

    return unified_df


if __name__ == "__main__":
    aggregated_df = load_and_aggregate_data()
    if not aggregated_df.empty:
        print("\n🎉 Data Aggregation Complete!")
        print(aggregated_df.head())
    else:
        print("\n❌ Aggregation failed or no data found.")