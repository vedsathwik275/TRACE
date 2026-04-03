# TRACE

**Temporal Recovery Analytics for Career Expectation**

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

A multi-source NBA injury sentiment analysis pipeline that collects data from Reddit, Bluesky, and sports news outlets, performs NLP sentiment analysis using FinBERT, and applies LLM-based content classification to identify suitable research content for Achilles injury recovery narratives.

---

## Overview

TRACE tracks public sentiment around NBA player injuries by scraping social media and news sources, then applying financial-domain sentiment analysis (FinBERT) to classify posts and articles as positive, negative, or neutral. The system is designed to surface trends in fan and media perception of player recovery timelines.

The project now includes **V2 scrapers** for Reddit, News, and Bluesky — purpose-built for historical Achilles injury data collection across 15 target NBA players. The V2 scrapers add checkpointing (resumable runs), multi-phase collection strategies, and a modular relevance scoring engine.

**What it does:**
- Scrapes Reddit posts and comments from NBA subreddits (V1: all 30 team subreddits; V2: targeted historical Achilles queries)
- Scrapes Bluesky posts using injury-related search queries with cursor-based pagination
- Scrapes RSS feeds from ESPN, CBS Sports, Yahoo Sports, NY Post, RotoWire, and more (V2 adds Google News RSS historical search)
- Aggregates all sources into a unified schema
- Uploads data to Supabase PostgreSQL
- Runs FinBERT sentiment classification on all text content
- Applies Gemini 2.5 Flash LLM classification to identify SUITABLE vs UNSUITABLE posts for research
- Stores and visualizes sentiment and classification results

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
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   LLM CONTENT CLASSIFICATION                        │
│                                                                     │
│  batch_runner.py (Gemini 2.5 Flash) ──► CSV: llm_classifications   │
│  • Text sanitization & validation                                  │
│  • SUITABLE / UNSUITABLE classification                            │
│  • Confidence scoring & reasoning                                  │
│  • Recovery phase refinement                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Features

- **Multi-source scraping** — Reddit (30 NBA subreddits), Bluesky, and 10+ sports news RSS feeds
- **V2 historical scrapers** — Targeted collection for 15 NBA players with confirmed Achilles injuries
- **Checkpointing** — V2 scrapers save progress to disk and resume safely after interruption
- **Multi-phase collection** — V2 scrapers run three distinct search stages per source for maximum coverage
- **Modular relevance scoring** — `TRACERelevanceScorer` with keyword weighting, player mention detection, and recovery phase classification
- **Google News RSS search** — V2 news scraper queries Google News RSS per-player for historical coverage (2015–present)
- **Standardized schema** — 27-column unified data model across all sources
- **Injury keyword tracking** — 300+ injury-related terms for relevance scoring
- **Player mention extraction** — Automatically identifies 15 target NBA players and their known aliases
- **Achilles-specific analytics** — Dedicated flag and recovery phase classifier for Achilles tendon injury content
- **Engagement metrics** — Captures upvotes, comments, likes, reposts, and engagement tier
- **FinBERT sentiment analysis** — Positive / Negative / Neutral classification with confidence scores
- **LLM content classification** — Gemini 2.5 Flash powered SUITABLE/UNSUITABLE classification for research content
- **Text sanitization** — Comprehensive cleaning pipeline removing problematic characters and formatting for reliable LLM processing
- **Batch processing with checkpointing** — Resumable LLM classification with automatic progress saving
- **Stratified sampling** — Platform-balanced sampling for efficient pilot testing and representative analysis
- **Supabase integration** — Cloud PostgreSQL storage with batch upload and fallback handling
- **Full-text article extraction** — Fetches article bodies via newspaper3k for deep content
- **V2 validation scripts** — 10-check QA suite for each V2 scraper output

---

## Project Structure

```
TRACE/
├── README.md                       # This file
├── QWEN.md                         # Python coding standards for this project
├── requirements.txt                # Python dependencies
├── scrapers/                       # Data collection modules
│   ├── reddit_scraper.py           # TRACEPrawScraper — Reddit via PRAW (V1)
│   ├── reddit_scraper_v2.py        # TRACERedditScraperV2 — historical Achilles scraper
│   ├── reddit_config.py            # Config: TARGET_PLAYERS, queries, subreddits, date ranges
│   ├── news_scraper.py             # TRACENewsScraper — RSS news feeds (V1)
│   ├── news_scraper_v2.py          # TRACENewsScraperV2 — RSS + Google News RSS scraper
│   ├── news_config.py              # Config: NEWS_SOURCES, relevance thresholds
│   ├── bluesky_scraper.py          # TRACEBlueskyScraper — Bluesky AT Protocol (V1)
│   ├── bluesky_scraper_v2.py       # TRACEBlueskyScraperV2 — paginated historical scraper
│   ├── supabase_uploader.py        # TRACESupabaseUploader — database upload
│   ├── relevance_scorer.py         # TRACERelevanceScorer — keyword scoring and classification
│   ├── checkpoint_manager.py       # TRACECheckpointManager — progress persistence
│   └── article_fetcher.py          # TRACEArticleFetcher — RSS and full article fetching
├── runners/                        # Orchestration and execution scripts
│   ├── rs_run.py                   # Run Reddit scraper (V1)
│   ├── rs_run_v2.py                # Run Reddit scraper V2
│   ├── ns_run.py                   # Run News scraper (V1)
│   ├── ns_run_v2.py                # Run News scraper V2 (RSS + Google News)
│   ├── bs_run.py                   # Run Bluesky scraper (V1)
│   ├── bs_run_v2.py                # Run Bluesky scraper V2
│   ├── gn_run.py                   # Run Google News RSS scraper only
│   ├── su_run.py                   # Upload unified data to Supabase
│   ├── data_aggregator.py          # Merge all source CSVs into one
│   ├── model_runner.py             # FinBERT inference + results upload
│   ├── text_sanitizer.py           # Text cleaning utilities for LLM processing
│   ├── gemini_classifier.py        # Gemini 2.5 Flash classification core logic
│   ├── test_classifier.py          # Test script for Gemini classifier (5 samples)
│   ├── pilot_runner.py             # Pilot classification run (100 stratified samples)
│   ├── batch_runner.py             # Production batch classifier with checkpointing
│   ├── validate_reddit_v2.py       # 10-check QA validation for Reddit V2 output
│   ├── validate_news_v2.py         # 10-check QA validation for News V2 output
│   └── validate_bluesky_v2.py      # 10-check QA validation for Bluesky V2 output
└── tests/                          # Test suite
    ├── test_bench.py               # Full pipeline validation test
    ├── test_reddit.py              # Reddit scraper unit tests
    ├── test_news.py                # News scraper unit tests
    └── test_google_news_rss.py     # Google News RSS tests
```

> **Note:** The `data/` directory (CSV output and checkpoint files) and `.env` file are excluded from version control via `.gitignore`.

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
| `GEMINI_API_KEY` | Google Gemini API key for LLM classification | [Google AI Studio](https://aistudio.google.com/app/apikey) |

**Example `.env`:**
```
REDDIT_CLIENT_ID=your_client_id_here
REDDIT_CLIENT_SECRET=your_client_secret_here
BLUESKY_HANDLE=yourhandle.bsky.social
BLUESKY_APP_PASSWORD=your-app-password
SUPABASE_URL=https://yourproject.supabase.co
SUPABASE_KEY=your_supabase_key
GEMINI_API_KEY=your_gemini_api_key_here
```

---

## Usage

### V2 Scrapers (Recommended — Historical Achilles Data)

The V2 scrapers perform targeted historical collection for 15 NBA players with confirmed Achilles injuries. They support checkpointing, so long-running collections can be safely interrupted and resumed.

```bash
# Collect historical Reddit data (multi-phase: player queries, achilles queries, hot/top sweeps)
python runners/rs_run_v2.py

# Collect historical news articles (RSS feeds + Google News RSS)
python runners/ns_run_v2.py
python runners/ns_run_v2.py --debug   # Print all filtered items for inspection

# Collect Google News RSS only (standalone historical search)
python runners/gn_run.py
python runners/gn_run.py --debug

# Collect historical Bluesky posts (multi-phase: player queries, achilles queries, broader queries)
python runners/bs_run_v2.py
```

After collection, validate V2 output quality before uploading:

```bash
python runners/validate_reddit_v2.py
python runners/validate_news_v2.py
python runners/validate_bluesky_v2.py
```

Each validation script runs 10 checks covering record count, Achilles rate, relevance thresholds, temporal coverage, player coverage, nulls, duplicates, schema completeness, date format, and recovery phases.

### V1 Scrapers (Standard Pipeline)

Run each step in order for a complete pipeline execution:

```bash
# Step 1: Collect data from all sources (can be run in parallel)
python runners/rs_run.py          # Reddit — scrapes all 30 NBA team subreddits
python runners/ns_run.py          # News   — scrapes sports RSS feeds
python runners/bs_run.py          # Bluesky — searches injury-related queries

# Step 2: Aggregate all source CSVs into one unified file
python runners/data_aggregator.py

# Step 3: Upload unified data to Supabase
python runners/su_run.py

# Step 4: Run FinBERT sentiment analysis and upload results
python runners/model_runner.py
```

### LLM Content Classification

After sentiment analysis, use Gemini 2.5 Flash to classify posts as SUITABLE or UNSUITABLE for Achilles injury recovery research:

**Quick test (5 samples):**
```bash
python runners/test_classifier.py
```

**Pilot run (100 stratified samples):**
```bash
python runners/pilot_runner.py
# Output: data/pilot_results.csv
# Provides comprehensive statistics across platforms and content types
```

**Production batch processing:**
```bash
# Process 10% stratified sample (default)
python runners/batch_runner.py
# Output: data/llm_classifications_sample.csv

# Process full dataset
python runners/batch_runner.py --full
# Output: data/llm_classifications_full.csv
```

**Batch runner features:**
- **Checkpointing** — Automatically resumes from last saved batch if interrupted
- **Stratified sampling** — Balanced sampling across Reddit, News, and Bluesky platforms
- **Memory efficient** — Processes in batches of 50, appending results to CSV after each batch
- **Progress tracking** — Real-time counts of SUITABLE/UNSUITABLE/ERROR classifications
- **Text sanitization** — Automatic cleaning of problematic characters to prevent JSON parsing errors

The batch runner outputs detailed statistics including:
- Total processed, success rate, error rate
- SUITABLE vs UNSUITABLE breakdown with percentages
- Average confidence scores
- Platform-wise SUITABLE rates
- SUITABLE rates for achilles_related=True vs False content
- Processing time and throughput

**Validate the full pipeline:**
```bash
python tests/test_bench.py
```

### Output Files

Each runner writes timestamped CSV files to the `data/` directory:

| Script | Output file |
|---|---|
| `rs_run.py` | `data/trace_reddit_data_YYYYMMDD_HHMMSS.csv` |
| `ns_run.py` | `data/trace_news_data_YYYYMMDD_HHMMSS.csv` |
| `bs_run.py` | `data/trace_bluesky_data_YYYYMMDD_HHMMSS.csv` |
| `rs_run_v2.py` | `data/trace_reddit_v2_data_YYYYMMDD_HHMMSS.csv` |
| `ns_run_v2.py` | `data/trace_news_v2_data_YYYYMMDD_HHMMSS.csv` |
| `bs_run_v2.py` | `data/trace_bluesky_v2_data_YYYYMMDD_HHMMSS.csv` |
| `gn_run.py` | `data/trace_news_gnews_YYYYMMDD_HHMMSS.csv` |
| `data_aggregator.py` | `data/trace_unified_data_YYYYMMDD_HHMMSS.csv` |
| `test_classifier.py` | `data/classifier_test_results.json` |
| `pilot_runner.py` | `data/pilot_results.csv` |
| `batch_runner.py` | `data/llm_classifications_sample.csv` or `data/llm_classifications_full.csv` |

V2 scrapers also write checkpoint files to `data/checkpoints/`, `data/news_checkpoints/`, and `data/bluesky_checkpoints/` for resumability.

---

## V2 Scraper Details

### Target Players

The V2 scrapers collect content specifically related to 15 NBA players with confirmed Achilles injuries:

| Player | Injury Date |
|---|---|
| Kevin Durant | 2019-05-13 |
| Klay Thompson | 2019-06-13 |
| DeMarcus Cousins | 2018-01-26 |
| John Wall | 2019-01-25 |
| Wesley Matthews | 2015-03-05 |
| Rudy Gay | 2017-01-28 |
| Rodney Hood | 2019-04-20 |
| Jeremy Lin | 2017-08-26 |
| Brandon Jennings | 2015-11-24 |
| Danilo Gallinari | 2021-08-07 |
| Bogdan Bogdanovic | 2020-10-19 |
| Chandler Parsons | 2018-01-04 |
| Brook Lopez | 2018-03-29 |
| Marcus Smart | 2022-05-27 |
| Gordon Hayward | 2017-10-17 |

### Collection Phases

Each V2 scraper runs three collection stages:

**Reddit V2 (`TRACERedditScraperV2`):**
- Stage 1: Player-specific historical searches (`"{player} achilles"`) across primary subreddits with date-range filtering
- Stage 2: Achilles-specific queries across all subreddits (primary + team + specialty)
- Stage 3: Hot and top post sweeps from primary subreddits (past year)

**News V2 (`TRACENewsScraperV2`):**
- Stage 1: RSS collection from 10+ configured sports news sources with broad relevance scoring
- Stage 2: Google News RSS historical search — 4 queries per player (injury, surgery, return, rehab) + yearly generic queries (2015–2024)
- Stage 3: Gap-filling analysis — reports years with thin coverage (<100 records)

**Bluesky V2 (`TRACEBlueskyScraperV2`):**
- Stage 1: Player-specific searches (`"{player} achilles"`, `"{player} achilles injury"`) with cursor pagination
- Stage 2: Achilles-specific queries from the shared query list
- Stage 3: Broader NBA injury queries (`"NBA injury"`, `"NBA sidelined"`, `"NBA injury report"`, etc.)

### Relevance Scoring

`TRACERelevanceScorer` computes a numeric relevance score for each item using:
- **Achilles terms** (highest weight): `achilles`, `achilles tear`, `achilles rupture`, etc.
- **Player name / alias matches**: +5.0 per matched player
- **Player + injury word combo bonus**: +3.0 when both are present
- **Broad injury terms**: +0.5 each
- **NBA context terms**: +0.25 each

RSS-specific scoring (`compute_score_rss`) uses simplified weights tuned for short titles and descriptions. Items below the relevance threshold are filtered before any full-article fetching occurs.

Recovery phases detected: `immediate_post_injury`, `surgery_treatment`, `rehabilitation`, `return_anticipation`, `general`.

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
| `recovery_phase` | TEXT | Content category (e.g., `rehabilitation`, `return_anticipation`) |
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

## LLM Content Classification

### Overview

After FinBERT sentiment analysis, TRACE applies an LLM-powered classification layer using **Gemini 2.5 Flash** to identify posts that are SUITABLE vs UNSUITABLE for Achilles injury recovery research. This step filters out noise, off-topic content, and low-quality posts to create a curated dataset of high-value recovery narratives.

### Classification Criteria

**SUITABLE posts include:**
- Personal recovery experiences and timelines
- Detailed injury descriptions and rehabilitation progress
- Medical updates or treatment discussions
- Player comeback stories and performance analysis
- Community discussions about recovery expectations
- Comparative analysis of different recovery trajectories

**UNSUITABLE posts include:**
- Generic injury news without recovery context
- Trade rumors or contract discussions
- Off-topic sports commentary
- Duplicate or spam content
- Low-quality or uninformative posts
- Posts primarily about other injuries

### Text Sanitization Pipeline

Before LLM processing, all text content passes through `text_sanitizer.py` with these steps:

1. **UTF-8 encoding normalization** — Strip invalid unicode
2. **Whitespace normalization** — Replace newlines, tabs, carriage returns with spaces
3. **Character replacement** — Convert quotes, backslashes, braces to safer alternatives
4. **Control character removal** — Remove null bytes and characters with `ord < 32`
5. **Deduplication** — Collapse multiple consecutive spaces
6. **Truncation** — Limit to 500 characters for efficient LLM processing

This pipeline prevents JSON parsing errors and ensures reliable classification results.

### Classification Workflow

1. **Load sentiment results** from Supabase or CSV
2. **Sanitize text content** using the cleaning pipeline
3. **Sample or process full dataset** (stratified across platforms if sampling)
4. **Classify each record** with Gemini 2.5 Flash via `gemini_classifier.py`:
   - Send: `text_content`, `source_platform`, `recovery_phase`, `mentioned_players`, `is_achilles_related`, `engagement_score`, `created_date`
   - Receive: JSON response with `classification`, `confidence`, `reasoning`, `recovery_phase`, `key_entities`
5. **Checkpoint progress** after each batch of 50 records
6. **Output results** to CSV with full classification metadata

### Output Schema

Classification results are saved to CSV with these columns:

| Column | Type | Description |
|---|---|---|
| `row_index` | INT | Original row index from sentiment_results |
| `source_platform` | TEXT | Reddit, News, or Bluesky |
| `is_achilles_related` | BOOL | Original Achilles detection flag |
| `text_preview` | TEXT | First 80 characters of sanitized text |
| `classification` | TEXT | `SUITABLE`, `UNSUITABLE`, or `ERROR` |
| `confidence` | FLOAT | Model confidence score (0.0–1.0) |
| `reasoning` | TEXT | LLM explanation of classification decision |
| `recovery_phase` | TEXT | Refined recovery phase classification |
| `key_entities` | TEXT | Comma-separated list of detected players, teams, concepts |
| `error` | TEXT | Error message if classification failed |
| `processed_at` | TIMESTAMP | ISO 8601 timestamp of processing |

### Classification Modules

**`text_sanitizer.py`** — Text cleaning utilities
- `sanitize_text(text)` — Clean individual text strings
- `sanitize_dataframe(df, column='text_content')` — Batch clean entire columns with statistics

**`gemini_classifier.py`** — Core classification logic
- `classify_record(row)` — Classify a single pandas Series
- Uses Gemini 2.5 Flash with system instructions for consistent classification
- Includes retry logic (3 attempts) with exponential backoff
- Returns structured JSON with classification, confidence, reasoning, phase, entities

**`test_classifier.py`** — Quick validation script
- Tests classifier on 5 diverse samples
- Validates API connectivity and output format
- Useful for debugging and development

**`pilot_runner.py`** — Statistical pilot run
- Processes 100 stratified samples (balanced across platforms)
- Generates comprehensive statistics and breakdowns
- Output: `data/pilot_results.csv`

**`batch_runner.py`** — Production-ready batch processor
- **Checkpointing** — Reads existing output CSV and skips already-processed rows
- **Sampling modes** — Default 10% stratified sample or `--full` for entire dataset
- **Memory efficient** — Processes in batches of 50, appends to CSV immediately
- **Progress tracking** — Real-time counts and elapsed time after each batch
- **Resumable** — Can be safely interrupted and restarted
- Output: `data/llm_classifications_sample.csv` or `data/llm_classifications_full.csv`

### Performance and Cost Considerations

- **Gemini 2.5 Flash** — Fast, cost-effective model optimized for classification tasks
- **Rate limiting** — 0.5 second delay between API calls to respect quotas
- **Batch checkpointing** — Prevents data loss and allows resumption after interruptions
- **Stratified sampling** — Test on 10% representative sample before full-dataset runs
- **Text truncation** — 500 character limit reduces token usage while preserving context

### Example Statistics

Typical pilot run (100 samples) output:
```
Total processed: 100
  SUITABLE: 45 (45.0%)
  UNSUITABLE: 53 (53.0%)
  ERROR: 2 (2.0%)

Average Confidence: 0.847

SUITABLE Rate by Platform:
  Reddit: 24/48 (50.0%)
  News: 14/32 (43.8%)
  Bluesky: 7/20 (35.0%)

SUITABLE Rate by is_achilles_related:
  is_achilles_related=True: 38/60 (63.3%)
  is_achilles_related=False: 7/40 (17.5%)
```

---

## Testing

Run the full integration test suite with:

```bash
python tests/test_bench.py
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
4. **Tests:** Add test coverage to `tests/test_bench.py` for any new scraper or runner component
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
| `google-generativeai` | Gemini 2.5 Flash LLM classification |
| `python-dotenv` | Environment variable loading |
| `matplotlib`, `seaborn` | Visualization and analytics plots |
