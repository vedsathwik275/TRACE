# data_aggregator.py
import pandas as pd
import os
from datetime import datetime
import numpy as np

def load_and_aggregate_data(data_folder_path: str = 'data/'):
    """
    Loads data from separate CSV files in the data folder and aggregates them.
    
    All scrapers now output data in the standardized Supabase schema format,
    so aggregation is simply concatenating the dataframes.
    """
    print("🧩 Starting Data Aggregation...")
    print("-" * 40)

    all_dataframes = []
    filenames_found = []

    # Ensure path has trailing slash
    if not data_folder_path.endswith('/'):
        data_folder_path += '/'

    # Look for data files from each source
    for filename in os.listdir(data_folder_path):
        filepath = os.path.join(data_folder_path, filename)

        if filename.startswith('trace_reddit_data_') and filename.endswith('.csv'):
            print(f"📂 Loading Reddit data: {filename}")
            df_source = pd.read_csv(filepath)
            all_dataframes.append(df_source)
            filenames_found.append(filename)

        elif filename.startswith('trace_news_data_') and filename.endswith('.csv'):
            print(f"📂 Loading News data: {filename}")
            df_source = pd.read_csv(filepath)
            all_dataframes.append(df_source)
            filenames_found.append(filename)

        elif filename.startswith('trace_bluesky_data_') and filename.endswith('.csv'):
            print(f"🦋 Loading Bluesky data: {filename}")
            df_source = pd.read_csv(filepath)
            all_dataframes.append(df_source)
            filenames_found.append(filename)

    if not all_dataframes:
        print("❌ No source data files found in the 'data' folder to aggregate.")
        print("    Expected files like: trace_reddit_data_*.csv, trace_news_data_*.csv, trace_bluesky_data_*.csv")
        return pd.DataFrame()

    print(f"\n📋 Found data from {len(filenames_found)} files: {filenames_found}")
    print("🔗 Combining dataframes...")

    # Concatenate all dataframes (all have standardized schema)
    unified_df = pd.concat(all_dataframes, ignore_index=True)

    print(f"✅ Combined dataframe has {len(unified_df)} rows.")

    # Post-processing: Ensure no NaN values remain
    print("\n🛠️ Post-processing unified data...")
    
    # Fill any remaining NaN in string columns
    string_columns = unified_df.select_dtypes(include=['object']).columns
    for col in string_columns:
        unified_df[col] = unified_df[col].fillna('')
    
    # Fill NaN in numeric columns with 0
    numeric_columns = unified_df.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        unified_df[col] = unified_df[col].fillna(0)
    
    # Ensure boolean columns are properly typed
    boolean_columns = ['is_achilles_related', 'is_quality_content', 'fetch_success']
    for col in boolean_columns:
        if col in unified_df.columns:
            unified_df[col] = unified_df[col].fillna(False).astype(bool)

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
