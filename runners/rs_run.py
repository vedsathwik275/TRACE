from reddit_scraper import TRACEPrawScraper
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Initialize the scraper
scraper = TRACEPrawScraper()

# Connect to Reddit using credentials from .env
success = scraper.setup_reddit_connection(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET")
)

if success:
    print("Starting comprehensive Reddit scrape...")
    # Run the main scraping function
    df_reddit = scraper.comprehensive_scrape(max_posts_per_subreddit=50) # Adjust as needed

    if not df_reddit.empty:
        scraper.analyze_scraped_data(df_reddit)
        # Save the data specifically from this scraper
        # You might need to add a save function similar to the news scraper's save_for_trace
        # For now, let's assume you just want the df_reddit object
        print(f"Reddit scrape complete. Retrieved {len(df_reddit)} posts.")
        # Example save within scraper context (you might add this method):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        df_reddit.to_csv(f'data/trace_reddit_data_{timestamp}.csv', index=False)
        print(f"Reddit data saved to: data/trace_reddit_data_{timestamp}.csv")
    else:
        print("No data retrieved from Reddit.")
else:
    print("Failed to set up Reddit connection.")