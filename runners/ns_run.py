from scrapers.news_scraper import TRACENewsScraper
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize the scraper
scraper = TRACENewsScraper()

print("Starting comprehensive News scrape...")
# Run the main scraping function
articles = scraper.run_trace_scrape(fetch_full_articles=True) # Set fetch_full_articles as desired

if articles:
    # Analyze results using the scraper's built-in method
    scraper.analyze_results()
    # Save the data using the scraper's built-in method
    df_news = scraper.save_for_trace() # This saves and returns the DataFrame
    print(f"News scrape complete. Retrieved {len(df_news)} articles.")
else:
    print("No articles retrieved from News sources.")