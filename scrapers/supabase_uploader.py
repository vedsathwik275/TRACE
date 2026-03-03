# supabase_uploader.py
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import json
import numpy as np

class TRACESupabaseUploader:
    """
    Upload TRACE unified dataframe to Supabase
    """
    def __init__(self, supabase_url: str, supabase_key: str):
        """Initialize Supabase client."""
        print("🔌 Initializing Supabase client...")
        self.client: Client = create_client(supabase_url, supabase_key)
        print("✅ Supabase client initialized!")

    def prepare_dataframe_for_upload(self, df: pd.DataFrame):
        """Prepare dataframe for Supabase upload."""
        import numpy as np
        print("\n🛠️ Preparing dataframe for Supabase...")
        df_upload = df.copy()

        # =========================================================================
        # STEP 1: Clean Infinity values (keep NaN for now)
        # =========================================================================
        print("🧹 Cleaning Infinity values...")
        df_upload = df_upload.replace([np.inf, -np.inf], np.nan)

        # =========================================================================
        # STEP 2: Handle complex objects (lists, dicts) - Convert to JSON strings
        # =========================================================================
        print("📝 Converting complex objects to JSON strings...")
        for col in df_upload.columns:
            if df_upload[col].dtype == 'object':
                # Check if the column contains lists or dicts
                sample_val = df_upload[col].dropna().iloc[0] if not df_upload[col].dropna().empty else None
                if isinstance(sample_val, (list, dict)):
                    print(f"  Converting column '{col}' to JSON string...")
                    df_upload[col] = df_upload[col].apply(lambda x: json.dumps(x) if pd.notnull(x) else None)

        # =========================================================================
        # STEP 3: Ensure integer columns are proper integers (handle NaN -> 0)
        # =========================================================================
        integer_like_columns = [
            'score', 'num_comments', 'like_count', 'repost_count', 'reply_count',
            'total_relevance_score', 'comments_extracted', 'total_comment_words'
        ]
        print("🔢 Converting integer-like columns...")
        for col in integer_like_columns:
            if col in df_upload.columns:
                # First, ensure it's numeric, coercing errors to NaN
                df_upload[col] = pd.to_numeric(df_upload[col], errors='coerce')
                # Fill NaN with 0
                df_upload[col] = df_upload[col].fillna(0)
                # Convert to proper Python int
                df_upload[col] = df_upload[col].astype(int)

        # =========================================================================
        # STEP 4: Clean remaining NaN in numeric columns (floats)
        # =========================================================================
        numeric_columns = df_upload.select_dtypes(include=[np.number]).columns
        df_upload[numeric_columns] = df_upload[numeric_columns].fillna(0)

        # =========================================================================
        # STEP 5: Clean object columns (strings) - Replace NaN with empty string or None
        # =========================================================================
        object_columns = df_upload.select_dtypes(include=['object']).columns
        # For strings, replacing NaN with empty string is often fine, but None might be preferred for DB
        df_upload[object_columns] = df_upload[object_columns].fillna('')
        # Or replace with None if the database column allows it and you prefer NULL
        # df_upload[object_columns] = df_upload[object_columns].where(pd.notnull(df_upload[object_columns]), None)

        print("✅ Data preparation complete.")
        return df_upload

    def upload_to_supabase(self, df: pd.DataFrame, table_name: str = 'trace_sentiment_data', batch_size: int = 100):
        """Upload dataframe to Supabase table."""
        print(f"\n📤 Uploading to Supabase table: {table_name}")
        print("=" * 60)

        # Prepare dataframe
        df_upload = self.prepare_dataframe_for_upload(df)

        # Convert to list of dictionaries
        records = df_upload.to_dict('records')
        total_records = len(records)
        print(f"📊 Total records to upload: {total_records}")
        print(f"📦 Batch size: {batch_size}")

        uploaded_count = 0
        failed_count = 0

        # Upload in batches
        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            print(f"  Uploading batch {batch_num} ({len(batch)} records)...")

            try:
                response = self.client.table(table_name).insert(batch).execute()
                uploaded_count += len(batch)
                print(f"    ✅ Batch {batch_num} uploaded successfully.")
            except Exception as e:
                print(f"    ❌ Error uploading batch {batch_num}: {e}")
                print(f"       Attempting single record uploads for this batch...")
                # Fallback: try uploading records one by one
                for record in batch:
                     try:
                         self.client.table(table_name).insert([record]).execute()
                         uploaded_count += 1
                     except Exception as single_e:
                         print(f"      ❌ Failed single record: {single_e}")
                         failed_count += 1
                # End fallback loop

        print(f"\n✅ Upload process finished!")
        print(f"   Uploaded: {uploaded_count}")
        print(f"   Failed: {failed_count}")
        print(f"   Check your Supabase dashboard for final results.")

    def create_trace_table(self):
        """Print the SQL command to create the trace_sentiment_data table in Supabase."""
        sql_schema = """
-- Create the trace_sentiment_data table
CREATE TABLE IF NOT EXISTS trace_sentiment_data (
    id SERIAL PRIMARY KEY,
    source_platform TEXT NOT NULL,
    source_detail TEXT,
    text_content TEXT,
    author TEXT,
    created_date TIMESTAMP WITH TIME ZONE,
    engagement_score INTEGER DEFAULT 0,
    engagement_secondary INTEGER DEFAULT 0,
    url TEXT,
    relevance_score DOUBLE PRECISION,
    recovery_phase TEXT,
    mentioned_players TEXT, -- Stored as JSON string
    is_injury_related BOOLEAN DEFAULT FALSE,
    num_comments_extracted INTEGER DEFAULT 0,
    avg_comment_score DOUBLE PRECISION DEFAULT 0.0,
    total_comment_words INTEGER DEFAULT 0,
    sentiment_label TEXT,
    sentiment_score DOUBLE PRECISION,
    is_achilles_related BOOLEAN DEFAULT FALSE,
    is_quality_content BOOLEAN DEFAULT TRUE,
    scraped_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    model_version TEXT DEFAULT 'FinBERT_v1',
    finbert_positive DOUBLE PRECISION,
    finbert_negative DOUBLE PRECISION,
    finbert_neutral DOUBLE PRECISION
);
"""
        print("📋 SQL Schema for Supabase Table 'trace_sentiment_data':")
        print(sql_schema)
        print("⚠️  IMPORTANT: Run this SQL in your Supabase SQL Editor first!")


# Example usage function
def upload_from_data_folder(supabase_url: str, supabase_key: str, data_folder_path: str = 'data/'):
    """Load unified data from the 'data' folder and upload to Supabase."""
    import os
    # Look for the latest unified CSV file in the data folder
    csv_files = [f for f in os.listdir(data_folder_path) if f.startswith('trace_unified_data_') and f.endswith('.csv')]
    if not csv_files:
        print(f"❌ No unified data file found in {data_folder_path}")
        return
    
    latest_file = sorted(csv_files)[-1] # Get the most recent one
    file_path = os.path.join(data_folder_path, latest_file)
    
    print(f"📂 Loading unified data from: {file_path}")
    unified_df = pd.read_csv(file_path)
    
    print(f"📊 Loaded {len(unified_df)} rows for upload.")
    
    # Initialize uploader
    uploader = TRACESupabaseUploader(supabase_url, supabase_key)
    
    # Upload the data
    uploader.upload_to_supabase(unified_df, table_name='trace_sentiment_data')
    
    print("\n✅ Supabase upload process complete!")