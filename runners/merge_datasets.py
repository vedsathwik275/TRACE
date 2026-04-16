"""
merge_datasets.py

Merges llm_classifications_full.csv and sentiment_results.csv on their shared
positional row index, then filters to records that are SUITABLE (LLM) or
is_achilles_related (FinBERT/scraper flag), and saves the result.
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
LLM_CSV = DATA_DIR / "llm_classifications_full.csv"
SENTIMENT_CSV = DATA_DIR / "sentiment_results.csv"
OUTPUT_CSV = DATA_DIR / "trace_filtered_dataset.csv"


def main() -> None:
    # ── Load ──────────────────────────────────────────────────────────────────
    llm = pd.read_csv(LLM_CSV)
    sent = pd.read_csv(SENTIMENT_CSV)

    print("=== llm_classifications_full.csv ===")
    print(f"Shape: {llm.shape}")
    print(f"Columns: {list(llm.columns)}")
    print()
    print(llm.head(2).to_string())
    print()

    print("=== sentiment_results.csv ===")
    print(f"Shape: {sent.shape}")
    print(f"Columns: {list(sent.columns)}")
    print()
    print(sent.head(2)[["source_platform", "text_content", "is_achilles_related",
                         "sentiment_label", "sentiment_positive", "sentiment_negative",
                         "sentiment_neutral", "engagement_score"]].to_string())
    print()

    # ── Join key inspection ───────────────────────────────────────────────────
    # llm_classifications uses `row_index` (0-based integer) as a positional
    # pointer into sentiment_results, which has no explicit ID column.
    # We reset the sentiment index to make it explicit and merge on it.
    print("✅ Join key: llm['row_index']  ↔  sent.reset_index()['index']")
    print(f"   llm row_index range  : {llm['row_index'].min()} – {llm['row_index'].max()}")
    print(f"   sent positional range: 0 – {len(sent) - 1}")
    print()

    # ── Merge ─────────────────────────────────────────────────────────────────
    sent_indexed = sent.reset_index().rename(columns={"index": "row_index"})
    merged = pd.merge(llm, sent_indexed, on="row_index", how="inner",
                      suffixes=("_llm", "_sent"))
    print(f"Shape after inner merge: {merged.shape}")
    print()

    # ── Normalise filter columns ──────────────────────────────────────────────
    # classification may be 'SUITABLE', 'UNSUITABLE', 'ERROR'
    # is_achilles_related comes from the sentiment file (suffixed _sent if dup)
    achilles_col = "is_achilles_related_sent" if "is_achilles_related_sent" in merged.columns \
        else "is_achilles_related"

    is_suitable = merged["classification"].str.strip().str.upper() == "SUITABLE"
    is_achilles = merged[achilles_col].astype(str).str.strip().str.lower().isin(
        {"true", "1", "yes"}
    )

    # ── Filter ────────────────────────────────────────────────────────────────
    filtered = merged[is_suitable | is_achilles].drop_duplicates()
    print(f"Shape after filter + dedup: {filtered.shape}")
    print()

    # ── Breakdown ─────────────────────────────────────────────────────────────
    both       = (is_suitable & is_achilles).sum()
    suit_only  = (is_suitable & ~is_achilles).sum()
    ach_only   = (~is_suitable & is_achilles).sum()

    print("=== Filter breakdown (pre-dedup row counts) ===")
    print(f"  SUITABLE only            : {suit_only:>6,}")
    print(f"  is_achilles_related only : {ach_only:>6,}")
    print(f"  Both conditions          : {both:>6,}")
    print(f"  Total passing filter     : {(is_suitable | is_achilles).sum():>6,}")
    print()

    # ── Save ──────────────────────────────────────────────────────────────────
    filtered.to_csv(OUTPUT_CSV, index=False)
    print(f"💾 Saved → {OUTPUT_CSV}  ({len(filtered):,} rows, {len(filtered.columns)} cols)")


if __name__ == "__main__":
    main()
