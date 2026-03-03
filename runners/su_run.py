import os
from dotenv import load_dotenv
from scrapers.supabase_uploader import TRACESupabaseUploader # Import the uploader class
import pandas as pd

def run_supabase_upload(supabase_url: str, supabase_key: str, data_folder_path: str = 'data/'):
    """Load the unified/modelled data and upload it to Supabase."""
    print("🚀 Starting Supabase Upload Runner...")
    print("-" * 40)

    # --- Step 1: Find and Load the Unified/Modelled Data ---
    # Look for the latest unified or modelled CSV file in the data folder
    csv_files = [
        f for f in os.listdir(data_folder_path) 
        if f.startswith(('trace_unified_data_', 'trace_modelled_data_')) and f.endswith('.csv')
    ]
    
    if not csv_files:
        print(f"❌ No unified or modelled data file found in {data_folder_path}")
        print("    Expected files starting with 'trace_unified_data_' or 'trace_modelled_data_'.csv")
        return

    latest_file = sorted(csv_files)[-1] # Get the most recent one
    file_path = os.path.join(data_folder_path, latest_file)

    print(f"📂 Loading data from: {file_path}")
    try:
        unified_df = pd.read_csv(file_path)
        print(f"📊 Loaded {len(unified_df)} rows for upload.")
    except Exception as e:
        print(f"❌ Error loading data from {file_path}: {e}")
        return

    # --- Step 2: Initialize the Supabase Uploader ---
    print("\n🔌 Initializing Supabase Uploader...")
    try:
        uploader = TRACESupabaseUploader(supabase_url, supabase_key)
        print("✅ Supabase Uploader initialized.")
    except Exception as e:
        print(f"❌ Error initializing Supabase uploader: {e}")
        return

    # --- Step 3: Upload the Data ---
    print("\n📤 Starting upload to Supabase...")
    try:
        # Use the uploader's method to handle the upload
        uploader.upload_to_supabase(
            df=unified_df,
            table_name='trace_sentiment_data', # Ensure this matches your Supabase table name
            batch_size=100 # Adjust batch size as needed
        )
        print("\n🎉 Supabase upload process completed successfully!")
    except Exception as e:
        print(f"❌ Error during Supabase upload: {e}")

if __name__ == "__main__":
    # .env file
    load_dotenv()

    # Retrieve Supabase credentials from environment variables
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")

    # Validate that credentials are present
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Supabase URL or Key not found in environment variables (.env file).")
        print("    Please ensure 'SUPABASE_URL' and 'SUPABASE_KEY' are set in your .env file.")
        exit() # Exit the script if credentials are missing

    # Run the upload process
    run_supabase_upload(SUPABASE_URL, SUPABASE_KEY)
