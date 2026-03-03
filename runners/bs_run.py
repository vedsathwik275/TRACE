from scrapers.bluesky_scraper import TRACEBlueskyScraper
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize the scraper
scraper = TRACEBlueskyScraper()

# Log in using credentials from .env
try:
    scraper.login(os.getenv("BLUESKY_HANDLE"), os.getenv("BLUESKY_APP_PASSWORD"))
    print("Starting comprehensive Bluesky scrape...")
    # Run the main scraping function
    df_bluesky = scraper.run_comprehensive_collection(
        posts_per_query=50,  # Adjust as needed
        fetch_replies=True,  # Adjust as needed
        max_replies=30       # Adjust as needed
    )

    if not df_bluesky.empty:
        print(f"Bluesky scrape complete. Retrieved {len(df_bluesky)} posts/threads.")
        # Save the data (similarly, you might add a save method to the class)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        df_bluesky.to_csv(f'data/trace_bluesky_data_{timestamp}.csv', index=False)
        print(f"Bluesky data saved to: data/trace_bluesky_data_{timestamp}.csv")
    else:
        print("No data retrieved from Bluesky.")
except Exception as e:
    print(f"Error logging in or scraping Bluesky: {e}")