# model_runner.py (Updated to fetch from Supabase, run model, upload results to Supabase)
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import torch
from transformers import BertTokenizer, BertForSequenceClassification, pipeline
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from supabase import create_client, Client
import os
from dotenv import load_dotenv # Import for loading .env
import warnings
warnings.filterwarnings('ignore')

print("🏀 TRACE Project: Supabase -> FinBERT -> Supabase Analysis")
print("=" * 60)

# --- Load Supabase Credentials ---
# If using Kaggle secrets, uncomment the block below and remove the dotenv block
# secrets = UserSecretsClient()
# SUPABASE_URL = secrets.get_secret("SUPABASE_URL")
# SUPABASE_KEY = secrets.get_secret("SUPABASE_KEY")

# If using environment variables (e.g., .env file), uncomment the block below
load_dotenv() # Load from .env file
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Supabase URL or Key not found in environment variables (.env file) or Kaggle secrets.")
    exit()

print("✅ Credentials loaded.")


def connect_to_supabase(url, key):
    """Connect to Supabase client."""
    print("\n🔐 Connecting to Supabase...")
    try:
        client = create_client(url, key)
        print("✅ Connected to Supabase!")
        return client
    except Exception as e:
        print(f"❌ Failed to connect to Supabase: {e}")
        return None

def fetch_data_from_supabase(supabase_client: Client, table_name: str = 'trace_sentiment_data', sample_fraction: float = 0.8):
    """Fetch data from Supabase table."""
    print(f"\n📊 Fetching data from Supabase table: {table_name}")
    try:
        # Fetch all data (adjust select/where clauses as needed)
        response = supabase_client.table(table_name).select('*').execute()
        df = pd.DataFrame(response.data)
        print(f"✅ Retrieved {len(df)} records from {table_name}")

        if sample_fraction < 1.0:
            sample_size = int(len(df) * sample_fraction)
            df = df.sample(n=sample_size, random_state=42)
            print(f"   Sampled {len(df)} records ({sample_fraction*100:.1f}%) for analysis.")

        return df
    except Exception as e:
        print(f"❌ Error fetching data from Supabase: {e}")
        return pd.DataFrame()

def run_finbert_sentiment_analysis(df: pd.DataFrame):
    """Run FinBERT sentiment analysis on the text content."""
    print("\n🤖 Running FinBERT sentiment analysis...")
    
    # Load FinBERT model
    model_name = "ProsusAI/finbert"
    tokenizer = BertTokenizer.from_pretrained(model_name)
    model = BertForSequenceClassification.from_pretrained(model_name)

    # Create a HuggingFace pipeline
    sentiment_pipeline = pipeline("sentiment-analysis", 
                                  model=model, 
                                  tokenizer=tokenizer,
                                  return_all_scores=True)

    # Function to run analysis on a single text
    def analyze_single_text(text):
        if pd.isna(text) or text.strip() == "":
            return {"label": "neutral", "score": 0.0, "positive": 0.0, "negative": 0.0, "neutral": 1.0}
        
        try:
            results = sentiment_pipeline(text)[0]
            pos_score = next((r['score'] for r in results if r['label'].lower() == 'positive'), 0.0)
            neg_score = next((r['score'] for r in results if r['label'].lower() == 'negative'), 0.0)
            neu_score = next((r['score'] for r in results if r['label'].lower() == 'neutral'), 0.0)
            
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
            return {"label": "neutral", "score": 0.0, "positive": 0.0, "negative": 0.0, "neutral": 1.0}

    # Apply the analysis
    sentiment_results = df['text_content'].apply(analyze_single_text)
    
    # Expand the results back into the dataframe
    df['sentiment_label'] = sentiment_results.apply(lambda x: x['label'])
    df['sentiment_score'] = sentiment_results.apply(lambda x: x['score'])
    df['finbert_positive'] = sentiment_results.apply(lambda x: x['positive'])
    df['finbert_negative'] = sentiment_results.apply(lambda x: x['negative'])
    df['finbert_neutral'] = sentiment_results.apply(lambda x: x['neutral'])
    
    print("✅ FinBERT analysis complete.")
    return df

def analyze_model_results(df: pd.DataFrame):
    """Analyze the results of the sentiment model."""
    print("\n📊 Analyzing Model Results...")
    print("="*40)
    
    print(f"Total records analyzed: {len(df)}")
    print(f"Sentiment distribution:\n{df['sentiment_label'].value_counts()}")
    print(f"Average sentiment confidence: {df['sentiment_score'].mean():.3f}")
    
    print(f"\nPlatform-wise Sentiment:")
    platform_sentiment = df.groupby('source_platform')['sentiment_label'].value_counts().unstack(fill_value=0)
    print(platform_sentiment)
    
    print(f"\nAchilles-related Sentiment:")
    achilles_sentiment = df[df['is_achilles_related']==True]['sentiment_label'].value_counts()
    print(achilles_sentiment)
    
    # Plotting (optional, requires matplotlib/seaborn)
    # fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    # sns.countplot(data=df, x='sentiment_label', ax=axes[0,0])
    # axes[0,0].set_title('Overall Sentiment Distribution')
    # sns.histplot(data=df, x='sentiment_score', bins=50, ax=axes[0,1])
    # axes[0,1].set_title('Distribution of Sentiment Confidence Scores')
    # sns.countplot(data=df, x='source_platform', hue='sentiment_label', ax=axes[1,0])
    # axes[1,0].set_title('Sentiment by Source Platform')
    # axes[1,0].tick_params(axis='x', rotation=45)
    # achilles_status_df = df.copy()
    # achilles_status_df['achilles_group'] = achilles_status_df['is_achilles_related'].map({True: 'Achilles Related', False: 'Not Achilles Related'})
    # sns.countplot(data=achilles_status_df, x='achilles_group', hue='sentiment_label', ax=axes[1,1])
    # axes[1,1].set_title('Sentiment: Achilles vs Non-Achilles Related')
    # plt.tight_layout()
    # plt.show()
    
    print("\n✅ Analysis complete!")

def upload_model_results_to_supabase(supabase_client: Client, df: pd.DataFrame, target_table: str = 'trace_sentiment_results'):
    """Upload the modelled results back to Supabase."""
    print(f"\n📤 Uploading model results to Supabase table: {target_table}")
    # Prepare records for insertion/upsert
    records_to_upload = []
    for _, row in df.iterrows():
        record = {
            'trace_data_id': row.get('id', None), # Assuming 'id' is the primary key from the source table
            'sentiment_label': row['sentiment_label'],
            'sentiment_score': float(row['sentiment_score']),
            'finbert_positive': float(row['finbert_positive']),
            'finbert_negative': float(row['finbert_negative']),
            'finbert_neutral': float(row['finbert_neutral']),
            'finbert_model_version': 'ProsusAI/finbert', # Version used
            'analyzed_at': datetime.now().isoformat() # Timestamp of analysis
        }
        records_to_upload.append(record)

    # Upload in batches
    batch_size = 100
    total_records = len(records_to_upload)
    uploaded_count = 0

    for i in range(0, total_records, batch_size):
        batch = records_to_upload[i:i+batch_size]
        try:
            response = supabase_client.table(target_table).upsert(
                batch,
                on_conflict='trace_data_id' # Upsert based on the foreign key
            ).execute()
            uploaded_count += len(batch)
            print(f"   ✅ Uploaded batch {i//batch_size + 1} ({uploaded_count}/{total_records})")
        except Exception as e:
            print(f"   ❌ Failed to upload batch {i//batch_size + 1}: {e}")

    print(f"\n✅ Upload of {uploaded_count} model results complete!")


def run_full_modelling_pipeline():
    """Run the full pipeline: fetch data -> run model -> analyze -> upload results."""
    print("🚀 Starting Full Modelling Pipeline (Supabase -> Model -> Supabase)...")
    print("-" * 40)
    
    # 1. Connect to Supabase
    supabase_client = connect_to_supabase(SUPABASE_URL, SUPABASE_KEY)
    if not supabase_client:
        print("❌ Cannot proceed without Supabase connection.")
        return

    # 2. Fetch data from Supabase
    df_to_model = fetch_data_from_supabase(supabase_client, table_name='trace_sentiment_data', sample_fraction=0.8) # Adjust table name and fraction
    if df_to_model.empty:
        print("❌ Cannot proceed without data from Supabase.")
        return
    
    # 3. Run FinBERT analysis
    df_with_sentiment = run_finbert_sentiment_analysis(df_to_model)
    
    # 4. Analyze results
    analyze_model_results(df_with_sentiment)
    
    # 5. Upload model results back to Supabase
    upload_model_results_to_supabase(supabase_client, df_with_sentiment, target_table='trace_sentiment_results') # Adjust target table name
    
    print("\n🎉 Full Modelling Pipeline Complete (Data fetched from and results sent to Supabase)!")

# Run the pipeline
if __name__ == "__main__":
    run_full_modelling_pipeline()