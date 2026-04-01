# model_runner_csv.py — CSV-based FinBERT sentiment analysis for TRACE
# Reads from CSV files, aggregates, runs FinBERT, and saves results to CSV

import argparse
import pandas as pd
import numpy as np
from transformers import BertTokenizer, BertForSequenceClassification, pipeline
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

print("🏀 TRACE Project: CSV -> FinBERT -> CSV Analysis")
print("=" * 60)


def parse_arguments():
    """Parse command-line arguments for CSV file paths and options."""
    parser = argparse.ArgumentParser(
        description="Run FinBERT sentiment analysis on TRACE CSV data files."
    )
    parser.add_argument(
        "--reddit",
        type=str,
        required=True,
        help="Path to Reddit CSV data file"
    )
    parser.add_argument(
        "--news",
        type=str,
        required=True,
        help="Path to News CSV data file"
    )
    parser.add_argument(
        "--gnews",
        type=str,
        required=True,
        help="Path to Google News CSV data file"
    )
    parser.add_argument(
        "--bluesky",
        type=str,
        required=True,
        help="Path to Bluesky CSV data file"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV file path (default: runners/data/trace_sentiment_results_[timestamp].csv)"
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=1.0,
        help="Fraction of data to sample for analysis (default: 1.0, use 0.8 for 80%%)"
    )
    return parser.parse_args()


def load_csv_file(file_path: str, source_name: str) -> pd.DataFrame:
    """Load a single CSV file and validate it exists."""
    print(f"\n📂 Loading {source_name} data from: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file_path)
        print(f"✅ Loaded {len(df)} records from {source_name}")
        return df
    except Exception as e:
        print(f"❌ Error loading {file_path}: {e}")
        return pd.DataFrame()


def aggregate_csv_dataframes(
    reddit_df: pd.DataFrame,
    news_df: pd.DataFrame,
    gnews_df: pd.DataFrame,
    bluesky_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregate all source DataFrames into a unified schema.
    
    Adds source_platform column to each and concatenates.
    Ensures all columns from the unified 27-column schema exist.
    """
    print("\n📊 Aggregating data from all sources...")
    
    # Add source_platform column to each dataframe
    if not reddit_df.empty:
        reddit_df = reddit_df.copy()
        reddit_df["source_platform"] = "Reddit"
    
    if not news_df.empty:
        news_df = news_df.copy()
        news_df["source_platform"] = "News"
    
    if not gnews_df.empty:
        gnews_df = gnews_df.copy()
        gnews_df["source_platform"] = "News"
    
    if not bluesky_df.empty:
        bluesky_df = bluesky_df.copy()
        bluesky_df["source_platform"] = "Bluesky"
    
    # Concatenate all dataframes
    dfs_to_concat = [df for df in [reddit_df, news_df, gnews_df, bluesky_df] if not df.empty]
    
    if not dfs_to_concat:
        print("❌ No data to aggregate — all input files were empty or missing")
        return pd.DataFrame()
    
    unified_df = pd.concat(dfs_to_concat, ignore_index=True)
    print(f"✅ Aggregated {len(unified_df)} total records from {len(dfs_to_concat)} sources")
    
    # Ensure required columns exist (fill missing with defaults)
    required_columns = [
        "source_platform", "source_detail", "author", "url", "text_content",
        "created_date", "engagement_score", "engagement_secondary",
        "engagement_tier", "relevance_score", "recovery_phase",
        "mentioned_players", "is_achilles_related", "is_quality_content",
        "text_length", "year", "month", "year_month"
    ]
    
    for col in required_columns:
        if col not in unified_df.columns:
            if col in ["engagement_score", "engagement_secondary", "text_length", "year", "month"]:
                unified_df[col] = 0
            elif col in ["is_achilles_related", "is_quality_content"]:
                unified_df[col] = False
            elif col == "source_platform":
                unified_df[col] = "Unknown"
            else:
                unified_df[col] = ""
    
    # Fill NaN values appropriately
    unified_df = unified_df.fillna({
        "text_content": "",
        "source_platform": "Unknown",
        "is_achilles_related": False,
        "engagement_score": 0,
    })
    
    # Platform distribution summary
    print("\n📈 Platform distribution:")
    print(unified_df["source_platform"].value_counts())
    
    return unified_df


def run_finbert_sentiment_analysis(df: pd.DataFrame):
    """Run FinBERT sentiment analysis on the text content."""
    print("\n🤖 Running FinBERT sentiment analysis...")
    
    # Load FinBERT model
    model_name = "ProsusAI/finbert"
    print(f"🔍 Loading model: {model_name}")
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertForSequenceClassification.from_pretrained(model_name)
    
    # Create a HuggingFace pipeline
    sentiment_pipeline = pipeline(
        "sentiment-analysis",
        model=model,
        tokenizer=tokenizer,
        return_all_scores=True,
        truncation=True,
        max_length=512
    )
    
    # Function to run analysis on a single text
    def analyze_single_text(text):
        if pd.isna(text) or text.strip() == "":
            return {
                "label": "neutral",
                "score": 0.0,
                "positive": 0.0,
                "negative": 0.0,
                "neutral": 1.0
            }
        
        try:
            # Truncate text if too long (FinBERT max is 512 tokens)
            # Approximate: 1 token ≈ 4 characters, so 512 tokens ≈ 2000 chars
            if len(text) > 2000:
                text = text[:2000] + "..."
            
            results = sentiment_pipeline(text)
            
            # Handle different result formats
            if isinstance(results, list) and len(results) > 0:
                result_item = results[0]
                if isinstance(result_item, list):
                    results = result_item
                elif isinstance(result_item, dict):
                    results = [result_item]
            
            pos_score = next((r["score"] for r in results if r["label"].lower() == "positive"), 0.0)
            neg_score = next((r["score"] for r in results if r["label"].lower() == "negative"), 0.0)
            neu_score = next((r["score"] for r in results if r["label"].lower() == "neutral"), 0.0)
            
            max_score = max(pos_score, neg_score, neu_score)
            if pos_score == max_score:
                overall_label = "positive"
            elif neg_score == max_score:
                overall_label = "negative"
            else:
                overall_label = "neutral"
            
            return {
                "label": overall_label,
                "score": max_score,
                "positive": pos_score,
                "negative": neg_score,
                "neutral": neu_score
            }
        except Exception as e:
            print(f"⚠️ Error analyzing text: {e}")
            return {
                "label": "neutral",
                "score": 0.0,
                "positive": 0.0,
                "negative": 0.0,
                "neutral": 1.0
            }
    
    # Apply the analysis
    print(f"📝 Analyzing {len(df)} records...")
    sentiment_results = df["text_content"].apply(analyze_single_text)
    
    # Expand the results back into the dataframe
    df["sentiment_label"] = sentiment_results.apply(lambda x: x["label"])
    df["sentiment_score"] = sentiment_results.apply(lambda x: x["score"])
    df["sentiment_positive"] = sentiment_results.apply(lambda x: x["positive"])
    df["sentiment_negative"] = sentiment_results.apply(lambda x: x["negative"])
    df["sentiment_neutral"] = sentiment_results.apply(lambda x: x["neutral"])
    df["finbert_model_version"] = "ProsusAI/finbert"
    df["analyzed_at"] = datetime.now().isoformat()
    
    print("✅ FinBERT analysis complete.")
    return df


def analyze_model_results(df: pd.DataFrame):
    """Analyze the results of the sentiment model."""
    print("\n📊 Analyzing Model Results...")
    print("=" * 40)
    
    print(f"Total records analyzed: {len(df)}")
    print(f"\nSentiment distribution:")
    print(df["sentiment_label"].value_counts())
    print(f"\nAverage sentiment confidence: {df['sentiment_score'].mean():.3f}")
    
    print(f"\nPlatform-wise Sentiment:")
    platform_sentiment = df.groupby("source_platform")["sentiment_label"].value_counts().unstack(fill_value=0)
    print(platform_sentiment)
    
    if "is_achilles_related" in df.columns:
        print(f"\nAchilles-related Sentiment:")
        achilles_df = df[df["is_achilles_related"] == True]
        if len(achilles_df) > 0:
            print(achilles_df["sentiment_label"].value_counts())
        else:
            print("No Achilles-related records found")
    
    print("\n✅ Analysis complete!")


def save_results_to_csv(df: pd.DataFrame, output_path: str):
    """Save the analyzed results to a CSV file."""
    print(f"\n💾 Saving results to: {output_path}")
    
    try:
        df.to_csv(output_path, index=False)
        print(f"✅ Saved {len(df)} records to {output_path}")
    except Exception as e:
        print(f"❌ Error saving CSV: {e}")


def run_csv_pipeline(args):
    """Run the full CSV-based pipeline."""
    print("🚀 Starting CSV-based Modelling Pipeline...")
    print("-" * 40)
    
    # 1. Load all CSV files
    reddit_df = load_csv_file(args.reddit, "Reddit")
    news_df = load_csv_file(args.news, "News")
    gnews_df = load_csv_file(args.gnews, "Google News")
    bluesky_df = load_csv_file(args.bluesky, "Bluesky")
    
    # Check if any data was loaded
    if all(df.empty for df in [reddit_df, news_df, gnews_df, bluesky_df]):
        print("❌ Cannot proceed — no data loaded from any source")
        return
    
    # 2. Aggregate data
    unified_df = aggregate_csv_dataframes(reddit_df, news_df, gnews_df, bluesky_df)
    
    if unified_df.empty:
        print("❌ Cannot proceed — aggregated data is empty")
        return
    
    # 3. Sample if requested
    if args.sample_fraction < 1.0:
        sample_size = int(len(unified_df) * args.sample_fraction)
        unified_df = unified_df.sample(n=sample_size, random_state=42)
        print(f"\n📊 Sampled {len(unified_df)} records ({args.sample_fraction*100:.1f}%) for analysis")
    
    # 4. Run FinBERT analysis
    df_with_sentiment = run_finbert_sentiment_analysis(unified_df)
    
    # 5. Analyze results
    analyze_model_results(df_with_sentiment)
    
    # 6. Save results to CSV
    output_path = args.output
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"runners/data/trace_sentiment_results_{timestamp}.csv"
    
    save_results_to_csv(df_with_sentiment, output_path)
    
    print("\n🎉 CSV-based Modelling Pipeline Complete!")


def main():
    """Main entry point."""
    args = parse_arguments()
    run_csv_pipeline(args)


if __name__ == "__main__":
    main()
