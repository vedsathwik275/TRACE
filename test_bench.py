# test_bench.py
import os
import sys
import traceback
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- 1. LOAD ENVIRONMENT VARIABLES AND CHECK DEPENDENCIES ---
print("🔍 Test Bench: Initializing...")
print("=" * 60)

# Load environment variables
load_dotenv()

# Required packages dictionary (name -> import name)
required_packages = {
    "os": "os", # Built-in
    "sys": "sys", # Built-in
    "dotenv": "dotenv",
    "pandas": "pandas",
    "requests": "requests",
    "bs4": "bs4",
    "praw": "praw",
    "atproto": "atproto",
    "supabase": "supabase",
    "transformers": "transformers",
    "torch": "torch",
    "numpy": "numpy",
    "sklearn": "sklearn",
    "matplotlib": "matplotlib",
    "seaborn": "seaborn",
}

print("\n📋 Checking required dependencies...")
missing_packages = []
for display_name, import_name in required_packages.items():
    try:
        if display_name in ["os", "sys"]: # Skip built-ins
            continue
        __import__(import_name)
        print(f"  ✅ {display_name}")
    except ImportError:
        print(f"  ❌ {display_name}")
        missing_packages.append(import_name)

if missing_packages:
    print(f"\n❌ Missing required packages: {', '.join(missing_packages)}")
    print("   Please install them using pip install <package_name>.")
    sys.exit(1) # Stop execution if dependencies are missing

print("\n✅ All required dependencies are available.")

# --- 2. VERIFY CREDENTIALS ---
print("\n🔐 Verifying credentials from .env...")
credentials_ok = True

# Reddit
reddit_cid = os.getenv("REDDIT_CLIENT_ID")
reddit_cs = os.getenv("REDDIT_CLIENT_SECRET")
if not reddit_cid or not reddit_cs:
    print("  ❌ REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET not found.")
    credentials_ok = False
else:
    print("  ✅ Reddit credentials found.")

# Bluesky
bsky_handle = os.getenv("BLUESKY_HANDLE")
bsky_pass = os.getenv("BLUESKY_APP_PASSWORD")
if not bsky_handle or not bsky_pass:
    print("  ❌ BLUESKY_HANDLE or BLUESKY_APP_PASSWORD not found.")
    credentials_ok = False
else:
    print("  ✅ Bluesky credentials found.")

# Supabase
sb_url = os.getenv("SUPABASE_URL")
sb_key = os.getenv("SUPABASE_KEY")
if not sb_url or not sb_key:
    print("  ❌ SUPABASE_URL or SUPABASE_KEY not found.")
    credentials_ok = False
else:
    print("  ✅ Supabase credentials found.")

if not credentials_ok:
    print("\n❌ One or more credentials are missing in .env.")
    sys.exit(1) # Stop execution if credentials are missing

print("\n✅ All credentials verified.")

# --- 3. CHECK DATA FOLDER ---
data_folder = "data"
if not os.path.exists(data_folder) or not os.path.isdir(data_folder):
    print(f"\n❌ Data folder '{data_folder}' does not exist or is not a directory.")
    sys.exit(1)
print(f"\n✅ Data folder '{data_folder}' exists.")

# --- 4. TEST EACH COMPONENT ---
test_results = {
    "praw_setup": False,
    "reddit_scrape": False,
    "news_scrape": False,
    "bluesky_scrape": False,
    "aggregate_data": False,
    "upload_to_supabase": False,
    "fetch_from_supabase": False,
    "run_finbert": False,
    "analyze_results": False,
    "upload_model_results": False,
}

def print_test_result(test_name, success, details=""):
    status = "✅ PASS" if success else "❌ FAIL"
    print(f"  {status} {test_name} - {details}")

try:
    # --- 4a. TEST REDDIT SCRAPER SETUP ---
    print("\n--- Testing Reddit Scraper Setup ---")
    try:
        from scrapers.reddit_scraper import TRACEPrawScraper
        scraper = TRACEPrawScraper()
        success = scraper.setup_reddit_connection(reddit_cid, reddit_cs)
        test_results["praw_setup"] = success
        print_test_result("PRAW Setup", success, "Connected to Reddit API" if success else "Failed to connect")
    except Exception as e:
        print_test_result("PRAW Setup", False, f"Exception: {e}")
        traceback.print_exc()

    # --- 4b. TEST REDDIT SCRAPING (Small Test) ---
    if test_results["praw_setup"]:
        print("\n--- Testing Reddit Scraping (Small Test) ---")
        try:
            # Run a very small scrape to test functionality
            small_df = scraper.scrape_subreddit('nba', 'hot', limit=5, include_comments=False)
            test_results["reddit_scrape"] = len(small_df) > 0
            print_test_result("Reddit Scrape", test_results["reddit_scrape"], f"Retrieved {len(small_df)} posts")
        except Exception as e:
            print_test_result("Reddit Scrape", False, f"Exception: {e}")
            traceback.print_exc()

    # --- 4c. TEST NEWS SCRAPER ---
    print("\n--- Testing News Scraper ---")
    try:
        from scrapers.news_scraper import TRACENewsScraper
        news_scraper = TRACENewsScraper()
        # Run a small test scrape
        articles = news_scraper.run_trace_scrape(fetch_full_articles=False) # Fetch full bodies is slow
        test_results["news_scrape"] = len(articles) > 0
        print_test_result("News Scrape", test_results["news_scrape"], f"Retrieved {len(articles)} articles")
    except Exception as e:
        print_test_result("News Scrape", False, f"Exception: {e}")
        traceback.print_exc()

    # --- 4d. TEST BLUESKY SCRAPER ---
    print("\n--- Testing Bluesky Scraper ---")
    try:
        from scrapers.bluesky_scraper import TRACEBlueskyScraper
        bluesky_scraper = TRACEBlueskyScraper()
        bluesky_scraper.login(bsky_handle, bsky_pass)
        # Run a small test collection
        df_bluesky_test = bluesky_scraper.run_comprehensive_collection(posts_per_query=5, fetch_replies=False)
        test_results["bluesky_scrape"] = not df_bluesky_test.empty
        print_test_result("Bluesky Scrape", test_results["bluesky_scrape"], f"Retrieved {len(df_bluesky_test)} posts")
    except Exception as e:
        print_test_result("Bluesky Scrape", False, f"Exception: {e}")
        traceback.print_exc()

    # --- 4e. TEST AGGREGATION ---
    # This assumes you have run the individual scrapers and generated files like trace_reddit_data_*.csv
    print("\n--- Testing Data Aggregation ---")
    try:
        from runners.data_aggregator import load_and_aggregate_data
        # This will fail if no source files are found
        aggregated_df = load_and_aggregate_data(data_folder_path=data_folder)
        test_results["aggregate_data"] = not aggregated_df.empty
        print_test_result("Data Aggregation", test_results["aggregate_data"], f"Created unified DF with {len(aggregated_df)} rows")
    except FileNotFoundError:
        print_test_result("Data Aggregation", False, f"No source files found in '{data_folder}' to aggregate.")
        print("     ⚠️  Did you run the individual scrapers first?")
    except Exception as e:
        print_test_result("Data Aggregation", False, f"Exception: {e}")
        traceback.print_exc()

    # --- 4f. TEST UPLOAD TO SUPABASE ---
    # This requires a unified CSV file to exist
    if test_results["aggregate_data"]:
        print("\n--- Testing Upload to Supabase ---")
        try:
            from scrapers.supabase_uploader import TRACESupabaseUploader
            uploader = TRACESupabaseUploader(sb_url, sb_key)
            # Use the aggregated_df from the previous step
            # The uploader handles the upload process
            uploader.upload_to_supabase(aggregated_df, table_name='trace_sentiment_data', batch_size=50)
            # If we reach here, the upload process started without immediate error
            test_results["upload_to_supabase"] = True # Assume success if no exception in upload call
            print_test_result("Upload to Supabase", test_results["upload_to_supabase"], "Upload process initiated")
        except Exception as e:
            print_test_result("Upload to Supabase", False, f"Exception: {e}")
            traceback.print_exc()

    # --- 4g. TEST FETCH FROM SUPABASE ---
    if test_results["upload_to_supabase"]:
        print("\n--- Testing Fetch from Supabase ---")
        try:
            from supabase import create_client, Client
            sb_client = create_client(sb_url, sb_key)
            # Fetch recently added records (within last hour for this test)
            one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
            response = sb_client.table('trace_sentiment_data').select('*').gte('scraped_date', one_hour_ago).execute()
            fetched_df = pd.DataFrame(response.data)
            test_results["fetch_from_supabase"] = not fetched_df.empty
            print_test_result("Fetch from Supabase", test_results["fetch_from_supabase"], f"Fetched {len(fetched_df)} recent records")
        except Exception as e:
            print_test_result("Fetch from Supabase", False, f"Exception: {e}")
            traceback.print_exc()

    # --- 4h. TEST FINBERT MODEL RUNNER ---
    # This assumes data exists in Supabase and fetches it
    if test_results["fetch_from_supabase"]:
        print("\n--- Testing FinBERT Model Runner (on fetched data) ---")
        try:
            from runners.model_runner import run_finbert_sentiment_analysis, analyze_model_results
            # Use the fetched data from the previous step
            df_with_sentiment = run_finbert_sentiment_analysis(fetched_df)
            test_results["run_finbert"] = 'sentiment_label' in df_with_sentiment.columns
            print_test_result("Run FinBERT", test_results["run_finbert"], "Analysis completed")

            if test_results["run_finbert"]:
                 analyze_model_results(df_with_sentiment)
                 test_results["analyze_results"] = True # Assume analysis step passed if no exception
                 print_test_result("Analyze Results", test_results["analyze_results"], "Analysis displayed")
        except Exception as e:
            print_test_result("Run FinBERT", False, f"Exception: {e}")
            traceback.print_exc()

    # --- 4i. TEST UPLOAD MODEL RESULTS TO SUPABASE ---
    # This requires the model to have run successfully
    if test_results["run_finbert"]:
        print("\n--- Testing Upload Model Results to Supabase ---")
        try:
            from supabase import create_client, Client
            sb_client_results = create_client(sb_url, sb_key)
            # Prepare results for upload (simplified mapping)
            results_to_upload = df_with_sentiment[['id', 'sentiment_label', 'sentiment_score', 'finbert_positive', 'finbert_negative', 'finbert_neutral']].copy()
            results_to_upload.rename(columns={'id': 'trace_data_id'}, inplace=True)
            results_to_upload['finbert_model_version'] = 'ProsusAI/finbert'
            results_to_upload['analyzed_at'] = datetime.now().isoformat()

            # Upload to a results table (ensure this table exists)
            target_table_name = 'trace_sentiment_results' # Make sure this table exists!
            records_to_insert = results_to_upload.to_dict('records')
            # Insert in batches
            batch_size = 50
            total_records = len(records_to_insert)
            for i in range(0, total_records, batch_size):
                 batch = records_to_insert[i:i+batch_size]
                 sb_client_results.table(target_table_name).upsert(
                     batch,
                     on_conflict='trace_data_id' # Assuming trace_data_id is the key
                 ).execute()
            # If we reach here, the upload error
            test_results["upload_model_results"] = True # Assume success if no exception in upload call
            print_test_result("Upload Model Results", test_results["upload_model_results"], "Upload process initiated")
        except Exception as e:
            print_test_result("Upload Model Results", False, f"Exception: {e}")
            traceback.print_exc()


except KeyboardInterrupt:
    print("\n⚠️ Test bench interrupted by user.")
except Exception as e:
    print(f"\n❌ An unexpected error occurred in the test bench: {e}")
    traceback.print_exc()

# --- 5. PRINT FINAL SUMMARY ---
print("\n" + "=" * 60)
print("🏁 Test Bench Summary")
print("=" * 60)
overall_success = all(status for status in test_results.values())

for test, status in test_results.items():
    print_test_result(test.replace('_', ' ').title(), status)

print("\n" + "-" * 60)
if overall_success:
    print("🎉 ALL TESTS PASSED! Your TRACE pipeline seems to be working correctly.")
else:
    print("⚠️ Some tests failed. Please review the logs above.")
print("-" * 60)

print("\n💡 Next Steps:")
if not test_results["praw_setup"] or not test_results["reddit_scrape"]:
    print("  - Check Reddit API credentials and network connectivity.")
if not test_results["news_scrape"]:
    print("  - Verify news source URLs and network connectivity.")
if not test_results["bluesky_scrape"]:
    print("  - Check Bluesky credentials and network connectivity.")
if not test_results["aggregate_data"]:
    print("  - Ensure source data files exist in the 'data' folder before running aggregator.")
if not test_results["upload_to_supabase"]:
    print("  - Verify Supabase URL/key and that the 'trace_sentiment_data' table exists.")
if not test_results["fetch_from_supabase"]:
    print("  - Verify Supabase URL/key and that data was successfully uploaded.")
if not test_results["run_finbert"]:
    print("  - Check if the FinBERT model can be loaded and applied to text data.")
if not test_results["upload_model_results"]:
    print("  - Verify Supabase URL/key and that the 'trace_sentiment_results' table exists.")

print("\nTest bench execution complete.")