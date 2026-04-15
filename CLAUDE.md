# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment

All commands must be run with the TRACE virtual environment activated:

```bash
source trace/bin/activate   # macOS/Linux
```

Install dependencies: `pip install -r requirements.txt`

## Running the Pipeline

### V2 Scrapers (primary — historical Achilles data for 15 target players)

```bash
python runners/rs_run_v2.py          # Reddit
python runners/ns_run_v2.py          # News (RSS + Google News)
python runners/gn_run.py             # Google News only
python runners/bs_run_v2.py          # Bluesky
```

### Validation and aggregation

```bash
python runners/validate_reddit_v2.py
python runners/validate_news_v2.py
python runners/validate_bluesky_v2.py
python runners/data_aggregator.py    # merge CSVs → trace_unified_data_*.csv
python runners/su_run.py             # upload to Supabase
```

### Sentiment and classification

```bash
python runners/model_runner.py          # FinBERT inference → Supabase
python runners/test_classifier.py       # smoke test: 5 samples
python runners/pilot_runner.py          # 100 stratified samples → data/pilot_results.csv
python runners/batch_runner.py          # 10% sample → data/llm_classifications_sample.csv
python runners/batch_runner.py --full   # full → data/llm_classifications_full.csv
```

### Temporal transformer (Phase 4)

```bash
python runners/temporal_transformer.py --train
python runners/temporal_transformer.py --predict --player "Player Name" --injury_date 2025-05-10
python runners/temporal_transformer.py --show_format   # print player_outcomes.csv template
```

Key flags for `--train`: `--sentiment_csv`, `--outcomes_csv`, `--classifications_csv` (default: `data/llm_classifications_full.csv`, filters to SUITABLE rows with confidence ≥ 0.5).

### Tests

```bash
python tests/test_bench.py   # full integration test (requires live APIs)
```

## Architecture

Two-layer design:

- **`scrapers/`** — Class-based collection modules. Each exposes one primary class with the `TRACE` prefix (`TRACEPrawScraper`, `TRACENewsScraperV2`, etc.). `__init__` sets state only — no network I/O. `scrapers/relevance_scorer.py` (`TRACERelevanceScorer`) and `scrapers/checkpoint_manager.py` (`TRACECheckpointManager`) are shared utilities used by all V2 scrapers.
- **`runners/`** — Thin orchestration scripts that call scraper methods and write output to `data/`. Classification logic lives here: `text_sanitizer.py` → `gemini_classifier.py` → `batch_runner.py`.

### Data flow

```
scrapers/ → data/*.csv → data_aggregator → Supabase → model_runner (FinBERT)
                                                             ↓
                                                     batch_runner (Gemini) → llm_classifications_full.csv
                                                             ↓
                                                   temporal_transformer.py (PyTorch)
```

### Unified schema

All scrapers output a 27-column DataFrame. Key columns relevant to downstream work:
- `text_content`, `source_platform`, `created_date`, `engagement_score`
- `mentioned_players` (JSON array string), `is_achilles_related` (bool)
- `recovery_phase`, `relevance_score`

The FinBERT results table (`trace_sentiment_results`) adds `sentiment_positive`, `sentiment_neutral`, `sentiment_negative` floats and links back via `trace_data_id`.

The Gemini classification CSV (`llm_classifications_full.csv`) adds `row_index`, `classification` (SUITABLE/UNSUITABLE), `confidence`, `reasoning`, `recovery_phase`, `key_entities`.

### Temporal transformer inputs

`temporal_transformer.py` reads two files:
- `sentiment_results.csv` — exported from `trace_sentiment_results` in Supabase. Requires `created_at`, `sentiment_positive/neutral/negative`, `engagement_score`, and either `player_name` or `mentioned_players`.
- `player_outcomes.csv` — hand-curated labels per player (see `--show_format`).
- Optionally filtered by `llm_classifications_full.csv` via `--classifications_csv`.

It bins records into 52 weekly slots post-injury and trains a 6-layer Transformer encoder to predict `P(career_success)`.

## Coding Standards (from QWEN.md)

- Python 3.11+; f-strings, `str | None` union types, `list[str]` generics
- All public methods require type hints and Google-style docstrings
- Imports ordered: stdlib → third-party → local
- All external calls (API, HTTP, DB, file I/O) wrapped in try-except with specific exceptions; never `except: pass`
- Console output uses emoji indicators: `✅` success, `❌` error, `⚠️` warning, `🔍` fetching, `💾` saving
- Constants defined at module level (never inside functions)
- `__init__` performs no network I/O
- Supabase uploads in batches of 100 with single-record fallback
- Before upload: replace `inf`/`-inf` with NaN, fill NaN, JSON-stringify lists, ISO-format datetimes

## Key Config Files

- `scrapers/reddit_config.py` — `TARGET_PLAYERS` list (15 players + injury dates + aliases), subreddit lists, date ranges, search queries
- `scrapers/news_config.py` — `NEWS_SOURCES` RSS feed list, relevance thresholds
- `.env` — credentials for Reddit, Bluesky, Supabase, Gemini (`GEMINI_API_KEY`), Firecrawl (`FIRECRAWL_API_KEY`)

## Data Directory

`data/` is gitignored. V2 scrapers write checkpoints to `data/checkpoints/`, `data/news_checkpoints/`, `data/bluesky_checkpoints/` for resumability. The batch runner also checkpoints after every 50 records.
