# TRACE

**Temporal Recovery Analytics for Career Expectation**

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

A multi-source NBA injury sentiment analysis pipeline that collects data from Reddit, Bluesky, and sports news outlets, performs NLP sentiment analysis using FinBERT, and stores results in Supabase.

---

## Overview

TRACE tracks public sentiment around NBA player injuries by scraping social media and news sources, then applying financial-domain sentiment analysis (FinBERT) to classify posts and articles as positive, negative, or neutral. The system is designed to surface trends in fan and media perception of player recovery timelines.

**What it does:**
- Scrapes Reddit posts and comments from all 30 NBA team subreddits
- Scrapes Bluesky posts using injury-related search queries
- Scrapes RSS feeds from ESPN, CBS Sports, Bleacher Report, Yahoo Sports, NBA.com, and Sporting News
- Aggregates all sources into a unified schema
- Uploads data to Supabase PostgreSQL
- Runs FinBERT sentiment classification on all text content
- Stores and visualizes sentiment results

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA COLLECTION                              │
│                                                                     │
│  Reddit (PRAW)  ──┐                                                 │
│  Bluesky API    ──┼──► Individual CSVs (data/ folder)              │
│  News RSS Feeds ──┘                                                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       DATA AGGREGATION                              │
│                                                                     │
│  data_aggregator.py ──► trace_unified_data_[timestamp].csv         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       DATABASE UPLOAD                               │
│                                                                     │
│  supabase_uploader.py ──► Supabase: trace_sentiment_data           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     SENTIMENT ANALYSIS                              │
│                                                                     │
│  model_runner.py (FinBERT) ──► Supabase: trace_sentiment_results   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Features

- **Multi-source scraping** — Reddit (30 NBA subreddits), Bluesky, 6 major sports news RSS feeds
- **Standardized schema** — 27-column unified data model across all sources
- **Injury keyword tracking** — 300+ injury-related terms for relevance scoring
- **Player mention extraction** — Automatically identifies discussed NBA players
- **Achilles-specific analytics** — Dedicated flag for achilles tendon injury content
- **Engagement metrics** — Captures upvotes, comments, likes, reposts, and engagement tier
- **FinBERT sentiment analysis** — Positive / Negative / Neutral classification with confidence scores
- **Supabase integration** — Cloud PostgreSQL storage with batch upload and fallback handling
- **Full-text article extraction** — Fetches article bodies via newspaper3k for deep content

---

## Project Structure

```
TRACE/
├── README.md                       # This file
├── QWEN.md                         # Python coding standards for this project
├── requirements.txt                # Python dependencies
├── test_bench.py                   # Full pipeline validation test
├── scrapers/                       # Data collection modules
│   ├── reddit_scraper.py           # TRACEPrawScraper — Reddit via PRAW
│   ├── news_scraper.py             # TRACENewsScraper — RSS news feeds
│   ├── bluesky_scraper.py          # TRACEBlueskyScraper — Bluesky AT Protocol
│   └── supabase_uploader.py        # TRACESupabaseUploader — database upload
└── runners/                        # Orchestration and execution scripts
    ├── rs_run.py                   # Run Reddit scraper
    ├── ns_run.py                   # Run News scraper
    ├── bs_run.py                   # Run Bluesky scraper
    ├── su_run.py                   # Upload unified data to Supabase
    ├── data_aggregator.py          # Merge all source CSVs into one
    └── model_runner.py             # FinBERT inference + results upload
```

> **Note:** The `data/` directory (CSV output) and `.env` file are excluded from version control via `.gitignore`.

---

## Prerequisites

- Python 3.11+
- pip
- A Reddit developer account and application ([create here](https://www.reddit.com/prefs/apps))
- A Bluesky account with an app password ([manage here](https://bsky.app/settings/app-passwords))
- A Supabase project with the required tables ([see schema below](#supabase-tables))

---

## Installation

```bash
# 1. Clone the repository
git clone <repository-url>
cd TRACE

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create the data output directory
mkdir data

# 4. Copy and configure environment variables
cp .env.example .env
# Then edit .env with your credentials (see Configuration below)
```

---

## Configuration

Create a `.env` file in the project root with the following variables:

| Variable | Description | Where to get it |
|---|---|---|
| `REDDIT_CLIENT_ID` | Reddit application client ID | [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) |
| `REDDIT_CLIENT_SECRET` | Reddit application client secret | [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) |
| `BLUESKY_HANDLE` | Your Bluesky handle (e.g. `user.bsky.social`) | Your Bluesky profile |
| `BLUESKY_APP_PASSWORD` | Bluesky app-specific password | [bsky.app/settings/app-passwords](https://bsky.app/settings/app-passwords) |
| `SUPABASE_URL` | Supabase project URL | Supabase project settings |
| `SUPABASE_KEY` | Supabase API key (anon or service role) | Supabase project settings |

**Example `.env`:**
```
REDDIT_CLIENT_ID=your_client_id_here
REDDIT_CLIENT_SECRET=your_client_secret_here
BLUESKY_HANDLE=yourhandle.bsky.social
BLUESKY_APP_PASSWORD=your-app-password
SUPABASE_URL=https://yourproject.supabase.co
SUPABASE_KEY=your_supabase_key
```

---

## Usage

Run each step in order for a complete pipeline execution:

```bash
# Step 1: Collect data from all sources (can be run in parallel)
python runners/rs_run.py          # Reddit — scrapes all 30 NBA team subreddits
python runners/ns_run.py          # News   — scrapes 6 sports RSS feeds
python runners/bs_run.py          # Bluesky — searches injury-related queries

# Step 2: Aggregate all source CSVs into one unified file
python runners/data_aggregator.py

# Step 3: Upload unified data to Supabase
python runners/su_run.py

# Step 4: Run FinBERT sentiment analysis and upload results
python runners/model_runner.py
```

**Validate the full pipeline:**
```bash
python test_bench.py
```

### Output Files

Each runner writes timestamped CSV files to the `data/` directory:

| Script | Output file |
|---|---|
| `rs_run.py` | `data/trace_reddit_data_YYYYMMDD_HHMMSS.csv` |
| `ns_run.py` | `data/trace_news_data_YYYYMMDD_HHMMSS.csv` |
| `bs_run.py` | `data/trace_bluesky_data_YYYYMMDD_HHMMSS.csv` |
| `data_aggregator.py` | `data/trace_unified_data_YYYYMMDD_HHMMSS.csv` |

---

## Data Schema

All scrapers output data conforming to this unified 27-column schema:

| Column | Type | Description |
|---|---|---|
| `source_platform` | TEXT | `Reddit`, `Bluesky`, or `News` |
| `source_detail` | TEXT | Subreddit name, news outlet, or Bluesky query |
| `author` | TEXT | Username or handle of the content creator |
| `url` | TEXT | Direct link to the original post or article |
| `text_content` | TEXT | Full text content |
| `created_date` | TIMESTAMP | ISO 8601 creation timestamp |
| `engagement_score` | INT | Primary engagement (upvotes, likes) |
| `engagement_secondary` | INT | Secondary engagement (comments, reposts) |
| `engagement_tier` | TEXT | `high`, `medium`, or `low` |
| `relevance_score` | FLOAT | Count of matched injury-related keywords |
| `recovery_phase` | TEXT | Content category (e.g., `fan_discussion`, `news_general`) |
| `mentioned_players` | TEXT | JSON array of detected player names |
| `is_achilles_related` | BOOL | Whether content mentions achilles injury |
| `is_quality_content` | BOOL | Content quality flag |
| `text_length` | INT | Character count of `text_content` |
| `year` | INT | Year of creation |
| `month` | INT | Month of creation |
| `year_month` | TEXT | Combined year-month (e.g., `2025-01`) |
| `num_comments_extracted` | INT | Reddit: number of comments collected |
| `avg_comment_score` | FLOAT | Reddit: average comment score |
| `total_comment_words` | INT | Reddit: total words across comments |
| `num_replies_extracted` | INT | Bluesky: number of replies collected |
| `avg_reply_likes` | FLOAT | Bluesky: average reply like count |
| `total_reply_words` | INT | Bluesky: total words across replies |
| `body_word_count` | INT | News: article body word count |
| `fetch_success` | BOOL | News: whether full article body was fetched |
| `uploaded_at` | TIMESTAMP | ISO 8601 timestamp of database upload |

---

## Supabase Tables

### `trace_sentiment_data`
Stores the aggregated raw data from all scrapers. Schema matches the unified 27-column model above.

### `trace_sentiment_results`
Stores FinBERT model output, linked to source data:

| Column | Type | Description |
|---|---|---|
| `trace_data_id` | INT | Foreign key to `trace_sentiment_data` |
| `sentiment_label` | TEXT | `positive`, `negative`, or `neutral` |
| `sentiment_score` | FLOAT | Confidence score for the assigned label (0–1) |
| `sentiment_positive` | FLOAT | Raw positive class probability |
| `sentiment_negative` | FLOAT | Raw negative class probability |
| `sentiment_neutral` | FLOAT | Raw neutral class probability |
| `finbert_model_version` | TEXT | Model identifier (`ProsusAI/finbert`) |
| `analyzed_at` | TIMESTAMP | ISO 8601 timestamp of analysis |

---

## Testing

Run the full integration test suite with:

```bash
python test_bench.py
```

The test bench validates:

1. All required Python packages are installed
2. All API credentials are present and loadable
3. The `data/` directory exists
4. Reddit API connection and test scrape (r/nba, 5 posts)
5. News scraper collection (small batch)
6. Bluesky scraper collection (5 posts per query)
7. Data aggregation from multiple source CSVs
8. Supabase upload of test records
9. Supabase data retrieval
10. FinBERT model load and inference
11. Sentiment results upload
12. Cleanup of all test data

A final summary reports pass/fail for each component.

---

## Contributing

1. **Branch naming:** Use descriptive branches (e.g., `feature/add-twitter-scraper`, `fix/supabase-upload-timeout`)
2. **Commit messages:** Use the imperative mood and describe the change clearly (e.g., `"Add ESPN article body extraction"`, not `"changes"`)
3. **Coding standards:** Follow the guidelines in [QWEN.md](./QWEN.md) — this file defines Python coding standards for this project
4. **Tests:** Add test coverage to `test_bench.py` for any new scraper or runner component
5. **Secrets:** Never commit credentials — use `.env` only

---

## Dependencies

See [`requirements.txt`](./requirements.txt) for exact versions. Key libraries:

| Library | Purpose |
|---|---|
| `pandas`, `numpy` | Data manipulation and aggregation |
| `requests`, `beautifulsoup4`, `lxml` | HTTP requests and HTML parsing |
| `newspaper3k` | Full article body extraction |
| `praw` | Reddit API client |
| `atproto` | Bluesky / AT Protocol client |
| `supabase` | Supabase PostgreSQL client |
| `transformers`, `torch` | FinBERT model inference |
| `python-dotenv` | Environment variable loading |
| `matplotlib`, `seaborn` | Visualization and analytics plots |
