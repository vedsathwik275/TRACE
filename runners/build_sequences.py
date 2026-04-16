import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
EPISODES_PATH = DATA_DIR / "injury_episodes.csv"
OUTCOMES_PATH = DATA_DIR / "player_outcomes.csv"
OUTPUT_PATH = DATA_DIR / "temporal_sequences.csv"

NUM_BINS = 52  # one per week over 365 days


def load_and_preview(path: Path, label: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    print(f"\n{'=' * 60}")
    print(f"  {label}  ({path.name})")
    print(f"{'=' * 60}")
    print(f"Columns ({len(df.columns)}):")
    print("  " + ", ".join(df.columns.tolist()))
    print(f"\nFirst 2 rows:")
    print(df.head(2).to_string(index=False))
    return df


def build_sequences(episodes: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    # --- post-injury window only (days 0 – 365) ---
    post = episodes[
        (episodes["days_from_injury"] >= 0) & (episodes["days_from_injury"] <= 365)
    ].copy()

    post["bin_index"] = (post["days_from_injury"] // 7).clip(upper=NUM_BINS - 1).astype(int)

    # --- aggregate per (player, bin) ---
    agg = (
        post.groupby(["player_name", "bin_index"], as_index=False)
        .agg(
            avg_sentiment_positive=("sentiment_positive", "mean"),
            avg_sentiment_neutral=("sentiment_neutral", "mean"),
            avg_sentiment_negative=("sentiment_negative", "mean"),
            total_engagement=("engagement_score", "sum"),
            record_count=("sentiment_positive", "count"),
        )
    )

    # --- build complete 52-bin grid for every player ---
    all_players = post["player_name"].unique()
    all_bins = pd.RangeIndex(NUM_BINS)

    full_index = pd.MultiIndex.from_product(
        [all_players, all_bins], names=["player_name", "bin_index"]
    )
    template = pd.DataFrame(index=full_index).reset_index()

    merged = template.merge(agg, on=["player_name", "bin_index"], how="left")
    fill_cols = [
        "avg_sentiment_positive",
        "avg_sentiment_neutral",
        "avg_sentiment_negative",
        "total_engagement",
        "record_count",
    ]
    merged[fill_cols] = merged[fill_cols].fillna(0)

    # --- attach outcome labels ---
    outcomes_clean = outcomes[["player", "outcome"]].rename(columns={"player": "player_name"})
    merged = merged.merge(outcomes_clean, on="player_name", how="left")

    missing_outcome = merged[merged["outcome"].isna()]["player_name"].unique()
    if len(missing_outcome):
        print(f"\nWARNING: no outcome found for {len(missing_outcome)} player(s): "
              f"{missing_outcome.tolist()}")

    merged["outcome"] = merged["outcome"].astype("Int64")  # nullable int
    merged = merged.sort_values(["player_name", "bin_index"]).reset_index(drop=True)

    return merged


def print_summary(sequences: pd.DataFrame) -> None:
    print(f"\n{'=' * 60}")
    print("  Per-player bin coverage summary")
    print(f"{'=' * 60}")
    print(f"{'Player':<30} {'Non-zero bins':>13} {'Total records':>14}")
    print("-" * 60)

    for player, grp in sequences.groupby("player_name"):
        non_zero = (grp["record_count"] > 0).sum()
        total_records = grp["record_count"].sum()
        print(f"{player:<30} {non_zero:>13} {total_records:>14.0f}")

    total_players = sequences["player_name"].nunique()
    total_bins_with_data = (sequences["record_count"] > 0).sum()
    total_records = sequences["record_count"].sum()
    print("-" * 60)
    print(f"{'TOTAL':<30} {total_bins_with_data:>13} {total_records:>14.0f}")
    print(f"\nPlayers: {total_players} | Rows in output: {len(sequences)}")


def main() -> None:
    episodes = load_and_preview(EPISODES_PATH, "injury_episodes.csv")
    outcomes = load_and_preview(OUTCOMES_PATH, "player_outcomes.csv")

    sequences = build_sequences(episodes, outcomes)
    print_summary(sequences)

    output_cols = [
        "player_name",
        "bin_index",
        "avg_sentiment_positive",
        "avg_sentiment_neutral",
        "avg_sentiment_negative",
        "total_engagement",
        "record_count",
        "outcome",
    ]
    sequences[output_cols].to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved  →  {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
