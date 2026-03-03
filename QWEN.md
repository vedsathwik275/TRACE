# QWEN.md — Python Coding Standards for TRACE

This document defines Python coding standards and best practices for the TRACE project. It is intended for AI coding assistants (such as Qwen, Claude, or Copilot) and human contributors alike. When generating or reviewing code for this repository, follow all guidelines below.

---

## 1. Python Version

**Require Python 3.11+.** Use modern language features where appropriate:

- f-strings for all string interpolation (never `%` formatting or `.format()`)
- Walrus operator (`:=`) for combined assignment and comparison in loops/conditions
- `match` statements for exhaustive case handling (Python 3.10+)
- `|` union syntax for type hints (Python 3.10+): `str | None` instead of `Optional[str]`

```python
# Good
label = f"Player: {player_name}"
if chunk := data_queue.get_nowait():
    process(chunk)

# Bad
label = "Player: %s" % player_name
label = "Player: {}".format(player_name)
```

---

## 2. Project Architecture Conventions

TRACE follows a two-layer architecture:

- **`scrapers/`** — Contains class-based modules responsible for data collection and upload. Each module exposes one primary class.
- **`runners/`** — Contains thin orchestration scripts that import from `scrapers/`, configure parameters, and write output to `data/`.

**Rules:**
- All scraper and uploader classes use the `TRACE` prefix: `TRACEPrawScraper`, `TRACENewsScraper`, `TRACEBlueskyScraper`, `TRACESupabaseUploader`
- Runner scripts (`rs_run.py`, `ns_run.py`, etc.) contain minimal logic — they call scraper methods and save output; they do not implement scraping logic themselves
- New data sources must be added as a new file in `scrapers/` with a corresponding runner in `runners/`
- All scraper output **must** conform to the [unified Supabase schema](./README.md#data-schema)

---

## 3. Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Classes | `PascalCase` with `TRACE` prefix | `TRACEPrawScraper` |
| Functions / Methods | `snake_case` | `fetch_article_body()` |
| Constants | `UPPER_SNAKE_CASE` | `SUPABASE_URL`, `MAX_POSTS` |
| Module files | `snake_case.py` | `reddit_scraper.py` |
| Variables | `snake_case`, descriptive | `engagement_score`, `article_df` |
| Loop indices | Single-letter only in short loops | `for i in range(n):` |

**Do not use:**
- Abbreviations that obscure meaning (`get_art()` → use `fetch_article_body()`)
- Single-letter variable names outside loop indices (`x`, `d`, `s`)
- Trailing underscores unless avoiding a Python keyword conflict (`type_`)

---

## 4. Import Organization

Organize imports in three groups, separated by a blank line, in this order:

1. **Standard library** — `os`, `sys`, `time`, `datetime`, `json`, `re`
2. **Third-party packages** — `pandas`, `requests`, `praw`, `transformers`, etc.
3. **Local modules** — imports from within this project

```python
# Good — three groups, alphabetically sorted within each group
import json
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import praw
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from scrapers.reddit_scraper import TRACEPrawScraper
from runners.data_aggregator import load_and_aggregate_data

# Bad
from scrapers.reddit_scraper import *   # wildcard import — never use
import pandas, requests, os            # multiple on one line — avoid
```

**Rules:**
- Never use wildcard imports (`from module import *`)
- Never import unused modules
- Always use explicit imports (prefer `from transformers import BertTokenizer` over importing the whole module)

---

## 5. Type Hints

Type hints are **required on all public method signatures**. Use them on private methods too wherever the types are non-obvious.

```python
# Good
from typing import Optional

def setup_reddit_connection(self, client_id: str, client_secret: str) -> bool:
    ...

def fetch_data_from_supabase(
    self,
    table_name: str = "trace_sentiment_data",
    sample_fraction: float = 0.8,
) -> Optional[pd.DataFrame]:
    ...

# Bad — missing type hints
def setup_reddit_connection(self, client_id, client_secret):
    ...
```

**Type hint guidelines:**
- Use `from typing import List, Dict, Optional, Tuple` for complex types (Python < 3.10 compat)
- For Python 3.10+, use built-in generics: `list[str]`, `dict[str, int]`, `str | None`
- Always annotate return types, including `-> None` for functions that return nothing
- Use `pd.DataFrame` for DataFrame parameters/returns (not just `object`)
- Use `Optional[X]` (or `X | None`) for values that can be `None`

---

## 6. Docstrings

**Required on:** all classes, all public methods, and any standalone function over 5 lines.

Use **Google-style docstrings**:

```python
class TRACEPrawScraper:
    """
    PRAW-based scraper for NBA injury content from Reddit.

    Collects posts and comments from all 30 NBA team subreddits,
    scores relevance against an injury keyword list, and outputs
    a standardized DataFrame conforming to the Supabase schema.

    Attributes:
        reddit: Authenticated PRAW Reddit instance.
        articles: List of collected post records.
    """

    def scrape_subreddit(
        self,
        subreddit_name: str,
        post_limit: int = 50,
    ) -> pd.DataFrame:
        """
        Scrape posts and comments from a single subreddit.

        Args:
            subreddit_name: The subreddit to scrape (without 'r/' prefix).
            post_limit: Maximum number of posts to retrieve. Defaults to 50.

        Returns:
            DataFrame with columns matching the unified Supabase schema.
            Returns an empty DataFrame if the subreddit is inaccessible.

        Raises:
            praw.exceptions.PRAWException: If the Reddit API returns an error.
        """
        ...
```

**Rules:**
- First line: one-sentence summary ending with a period
- Blank line before `Args`, `Returns`, `Raises` sections
- Document every parameter with its type and default if applicable
- Document return value including shape/schema for DataFrames
- Document exceptions only if the function can raise them

---

## 7. Error Handling

All external interactions (API calls, HTTP requests, file I/O, database operations) must be wrapped in try-except.

```python
# Good — specific error handling with meaningful context
try:
    response = requests.get(url, headers=self.headers, timeout=10)
    response.raise_for_status()
    return response.json()
except requests.exceptions.Timeout:
    print(f"⚠️ Request timed out for URL: {url}")
    return None
except requests.exceptions.HTTPError as e:
    print(f"❌ HTTP error {e.response.status_code} for URL: {url}")
    return None
except Exception as e:
    print(f"❌ Unexpected error fetching {url}: {e}")
    return None

# Bad — swallowing the exception silently
try:
    response = requests.get(url)
    return response.json()
except:
    pass
```

**Rules:**
- Never use bare `except:` — always catch at minimum `Exception as e`
- Never swallow exceptions silently with `pass` alone
- Always include the exception message (`{e}`) in the error output
- Use graceful degradation: return `None`, `[]`, or an empty `pd.DataFrame()` on failure rather than re-raising (unless the error is fatal)
- Use `response.raise_for_status()` after HTTP requests to catch 4xx/5xx codes
- Always specify `timeout=` on `requests.get()` and `requests.post()` calls

### Console Status Indicators

Use emoji indicators for console output, consistent with the codebase:

| Symbol | Meaning |
|---|---|
| `✅` | Success |
| `❌` | Error / Failure |
| `⚠️` | Warning |
| `🔍` | Searching / Fetching |
| `📊` | Data processing / stats |
| `🚀` | Starting / Launching |
| `💾` | Saving / Uploading |

```python
print(f"✅ Connected to Reddit API successfully")
print(f"❌ Failed to fetch article from {url}: {e}")
print(f"⚠️ Rate limit reached, sleeping for {sleep_time}s")
print(f"🔍 Scraping r/{subreddit_name} ({post_limit} posts)")
print(f"💾 Uploading {len(df)} records to {table_name}")
```

---

## 8. Environment Variables

**Never hardcode credentials or secrets.** Always load from `.env` using `python-dotenv`.

```python
# Good — validate at startup, fail with a clear message
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError(
        "Missing required environment variables: SUPABASE_URL and/or SUPABASE_KEY. "
        "Check your .env file."
    )

# Bad — hardcoded credentials
SUPABASE_URL = "https://myproject.supabase.co"
SUPABASE_KEY = "my-secret-key-123"
```

**Rules:**
- Load `.env` at the top of every entry-point script (runners), not in `scrapers/` modules
- Validate that required variables are set before any API calls
- Use `os.getenv("VAR", "default")` only if a sensible default exists
- Document all required variables in README.md and `.env.example`

---

## 9. Data Handling

### Unified Schema Compliance

All scrapers must produce a DataFrame with columns matching the [unified Supabase schema](./README.md#data-schema). Use `create_standardized_article()` or the equivalent method to ensure conformance.

### DataFrame Standards

```python
# Good — explicit dtypes and null handling
df["engagement_score"] = pd.to_numeric(df["engagement_score"], errors="coerce").fillna(0).astype(int)
df["created_date"] = pd.to_datetime(df["created_date"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
df["mentioned_players"] = df["mentioned_players"].apply(json.dumps)

# Bad — leaving raw types that will fail on upload
df["engagement_score"] = some_raw_value  # could be float('inf'), None, or a list
```

### Data Cleaning Before Upload

Before uploading to Supabase, always:

1. Replace infinity values: `df.replace([np.inf, -np.inf], np.nan, inplace=True)`
2. Fill NaN appropriately: strings → `""`, numbers → `0`, booleans → `False`
3. Convert lists/dicts to JSON strings: `json.dumps(value)`
4. Convert datetimes to ISO format strings: `.strftime("%Y-%m-%dT%H:%M:%S")`
5. Ensure integer columns are cast to `int` (not `float64` with NaN)

### Batch Processing

Always upload in batches. Default batch size is 100 records. Implement single-record fallback:

```python
BATCH_SIZE = 100

for i in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[i:i + BATCH_SIZE].to_dict("records")
    try:
        supabase.table(table_name).insert(batch).execute()
        print(f"✅ Uploaded batch {i // BATCH_SIZE + 1}")
    except Exception as e:
        print(f"⚠️ Batch upload failed, falling back to single-record mode: {e}")
        for record in batch:
            try:
                supabase.table(table_name).insert([record]).execute()
            except Exception as inner_e:
                print(f"❌ Failed to upload record: {inner_e}")
```

---

## 10. Code Structure

### Functions

- **Single responsibility:** Each function does one thing
- **Length:** Aim for under 50 lines per function; refactor if longer
- **Parameters:** Avoid functions with more than 5 parameters; use a config dict or dataclass if needed
- **Mutable defaults:** Never use mutable objects as default arguments

```python
# Bad — mutable default argument (shared across all calls)
def process_results(data, results=[]):
    results.append(data)
    return results

# Good
def process_results(data: dict, results: list | None = None) -> list:
    if results is None:
        results = []
    results.append(data)
    return results
```

### Classes

- `__init__` must only set state (assign attributes). Do not make API calls or perform I/O in `__init__`
- Keep scraper-specific setup logic in a dedicated method (e.g., `setup_reddit_connection()`)
- Keep data processing separate from I/O: one method fetches, another transforms

```python
# Good — __init__ is pure state setup
class TRACENewsScraper:
    def __init__(self) -> None:
        self.headers = {"User-Agent": "TRACE/1.0"}
        self.articles: list[dict] = []
        self.session = requests.Session()

    def run_trace_scrape(self) -> pd.DataFrame:
        """Orchestrates scraping from all news sources."""
        ...

# Bad — __init__ performs network I/O
class TRACENewsScraper:
    def __init__(self) -> None:
        self.articles = self._fetch_all_articles()  # network call in __init__
```

---

## 11. Constants and Configuration

Define all static data as module-level constants, not inside functions. This includes keyword lists, team names, API endpoints, and numeric thresholds.

```python
# Good — at the top of the module, after imports
NBA_TEAMS = [
    "nba", "lakers", "celtics", "warriors", "heat",
    # ... all 30 teams
]

INJURY_KEYWORDS = [
    "acl", "mcl", "achilles", "hamstring", "meniscus",
    # ... all injury terms
]

MAX_POSTS_PER_SUBREDDIT = 50
BATCH_SIZE = 100
REQUEST_TIMEOUT = 10

# Bad — buried inside a function
def comprehensive_scrape(self):
    teams = ["nba", "lakers", ...]  # should be a constant
    for team in teams:
        ...
```

---

## 12. Testing

- All new scrapers or runners must have corresponding validation steps in `test_bench.py`
- Tests must use small, bounded data (e.g., `post_limit=5`) to avoid slow CI or rate-limit issues
- Tests must clean up any files or database records they create
- Test failures must be reported clearly and not silently ignored

```python
# Good — bounded, descriptive, with cleanup
def test_reddit_scrape():
    scraper = TRACEPrawScraper()
    df = scraper.scrape_subreddit("nba", post_limit=5)
    success = len(df) > 0 and "text_content" in df.columns
    print_test_result("Reddit scrape", success, f"{len(df)} posts collected")
    return success
```

---

## 13. Git and Version Control Practices

- **Never commit `.env`** — it is gitignored; do not force-add it
- **Never commit `data/`** — CSV output is gitignored; it is runtime-generated
- **Commit messages:** Use imperative mood, describe the change clearly
  - Good: `"Add Bluesky reply thread extraction"`, `"Fix NaN handling in Supabase uploader"`
  - Bad: `"updates"`, `"fix stuff"`, `"WIP"`
- **One logical change per commit** — avoid bundling unrelated changes

---

## 14. Anti-Patterns to Avoid

The following patterns are **forbidden** in this codebase:

| Anti-pattern | Why | Fix |
|---|---|---|
| `from module import *` | Pollutes namespace, hides dependencies | Use explicit imports |
| `except: pass` or `except Exception: pass` | Silently hides bugs | Log the error and return gracefully |
| Hardcoded secrets | Security risk | Use `.env` + `python-dotenv` |
| `print("debug:", x)` left in production | Clutters output | Remove before committing |
| Mutable default arguments (`def f(x=[])`) | Shared state bug | Use `None` default and initialize inside |
| God functions > 100 lines doing multiple things | Hard to test and maintain | Split into single-responsibility functions |
| Inline magic numbers | Unreadable and hard to change | Define as named constants |
| Missing `timeout=` on requests | Hangs indefinitely on failure | Always set `timeout=10` or similar |
| `df.append()` in a loop | Deprecated and slow in pandas | Collect records as a list, then `pd.DataFrame(records)` |
| Catching specific exceptions and re-raising as generic `Exception` | Loses error context | Let the original exception propagate or log it |

---

## 15. Quick Checklist

Before submitting code to this repository, verify:

- [ ] All public methods have type hints on parameters and return types
- [ ] All classes and public methods have Google-style docstrings
- [ ] All external API calls are wrapped in try-except with meaningful error messages
- [ ] No secrets or credentials are hardcoded
- [ ] All constants are defined at module level, not inside functions
- [ ] f-strings used for all string formatting
- [ ] Imports are organized (stdlib → third-party → local)
- [ ] No wildcard imports
- [ ] `__init__` does not perform network I/O
- [ ] New scraper/runner added to `test_bench.py`
- [ ] Scraper output conforms to the unified Supabase schema
- [ ] No debug `print` statements left in code
