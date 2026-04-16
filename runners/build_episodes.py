"""
build_episodes.py

For each player in player_outcomes.csv, extracts a ±365-day window of records
from trace_filtered_dataset.csv where the text mentions the player by name,
annotates each record with player_name and days_from_injury, and saves all
valid episodes (≥ 10 records) to injury_episodes.csv.
"""

import re
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
FILTERED_CSV = DATA_DIR / "trace_filtered_dataset.csv"
OUTCOMES_CSV = DATA_DIR / "player_outcomes.csv"
OUTPUT_CSV = DATA_DIR / "injury_episodes.csv"
WINDOW_DAYS = 365   # ±365 days around injury_date
MIN_RECORDS = 10


def build_name_pattern(player: str) -> str:
    """Build a regex that matches any name token from the player's full name.

    Args:
        player: Full player name, e.g. "Kevin Durant".

    Returns:
        Regex pattern string matching first or last name as a word boundary.
    """
    tokens = [re.escape(t) for t in player.replace("_", " ").strip().split() if len(t) > 1]
    return r"\b(" + "|".join(tokens) + r")\b"


def build_episode(
    filtered: pd.DataFrame,
    player: str,
    injury_date: pd.Timestamp,
) -> pd.DataFrame:
    """Filter records to a player's injury window and annotate them.

    Args:
        filtered: Full trace_filtered_dataset DataFrame with a UTC-aware
            ``created_date`` column.
        player: Player display name from player_outcomes.
        injury_date: UTC-aware injury timestamp.

    Returns:
        Subset of ``filtered`` within ±WINDOW_DAYS of injury_date that
        contain the player's name, with ``player_name`` and
        ``days_from_injury`` columns added.
    """
    window_start = injury_date - pd.Timedelta(days=WINDOW_DAYS)
    window_end = injury_date + pd.Timedelta(days=WINDOW_DAYS)

    pattern = build_name_pattern(player)

    in_window = (filtered["created_date"] >= window_start) & (
        filtered["created_date"] <= window_end
    )
    mentions_player = filtered["text_content"].str.contains(
        pattern, case=False, na=False, regex=True
    )

    episode = filtered.loc[in_window & mentions_player].copy()
    episode["player_name"] = player
    episode["days_from_injury"] = (
        episode["created_date"] - injury_date
    ).dt.days.astype(int)

    return episode


def main() -> None:
    # ── Load ──────────────────────────────────────────────────────────────────
    filtered = pd.read_csv(FILTERED_CSV, low_memory=False)
    outcomes = pd.read_csv(OUTCOMES_CSV)

    filtered.columns = filtered.columns.str.lower().str.strip()
    outcomes.columns = outcomes.columns.str.lower().str.strip()

    print(f"🔍 Loaded trace_filtered_dataset: {filtered.shape}")
    print(f"🔍 Loaded player_outcomes:        {outcomes.shape}")
    print()

    # ── Parse dates ───────────────────────────────────────────────────────────
    filtered["created_date"] = pd.to_datetime(
        filtered["created_date"], utc=True, errors="coerce"
    )
    outcomes["injury_date"] = pd.to_datetime(
        outcomes["injury_date"], utc=True, errors="coerce"
    )

    bad_dates = filtered["created_date"].isna().sum()
    if bad_dates:
        print(f"⚠️  Dropped {bad_dates} rows with unparseable created_date")
        filtered = filtered.dropna(subset=["created_date"])
        print()

    # ── Diagnostics ───────────────────────────────────────────────────────────
    print("=== Date range diagnostics ===")
    print(f"  filtered created_date : {filtered['created_date'].min()}  →  {filtered['created_date'].max()}")
    print(f"  outcomes injury_date  : {outcomes['injury_date'].min()}  →  {outcomes['injury_date'].max()}")
    print()

    kd_row = outcomes[outcomes["player"] == "kevin_durant"].iloc[0]
    kd_injury = kd_row["injury_date"]
    kd_start  = kd_injury - pd.Timedelta(days=WINDOW_DAYS)
    kd_end    = kd_injury + pd.Timedelta(days=WINDOW_DAYS)
    print(f"=== kevin_durant deep-dive ===")
    print(f"  injury_date : {kd_injury}")
    print(f"  window      : {kd_start}  →  {kd_end}")

    in_window      = (filtered["created_date"] >= kd_start) & (filtered["created_date"] <= kd_end)
    mentions_name  = filtered["text_content"].str.contains(r"\b(Kevin|Durant)\b", case=False, na=False)
    print(f"  records in date window (no name filter) : {in_window.sum():,}")
    print(f"  records mentioning Durant/Kevin (no date filter) : {mentions_name.sum():,}")
    print(f"  records passing BOTH filters : {(in_window & mentions_name).sum():,}")
    print()

    # ── Per-player episodes ───────────────────────────────────────────────────
    valid_episodes: list[pd.DataFrame] = []
    summary: list[dict] = []

    for _, row in outcomes.iterrows():
        player: str = str(row["player"]).strip()
        injury_date: pd.Timestamp = row["injury_date"]

        if pd.isna(injury_date):
            summary.append({"player": player, "records": 0, "status": "❌ skipped (invalid injury_date)"})
            continue

        episode = build_episode(filtered, player, injury_date)
        n = len(episode)

        if n < MIN_RECORDS:
            summary.append({"player": player, "records": n, "status": f"❌ skipped (< {MIN_RECORDS} records)"})
        else:
            valid_episodes.append(episode)
            summary.append({"player": player, "records": n, "status": "✅ included"})

    # ── Summary table ─────────────────────────────────────────────────────────
    print(f"{'Player':<25} {'Records':>8}  Status")
    print("-" * 55)
    for s in summary:
        print(f"{s['player']:<25} {s['records']:>8,}  {s['status']}")
    print()

    # ── Combine & save ────────────────────────────────────────────────────────
    if not valid_episodes:
        print("❌ No valid episodes found — nothing saved.")
        return

    combined = pd.concat(valid_episodes, ignore_index=True)
    print(f"Shape before dedup : {combined.shape}")
    combined = combined.drop_duplicates()
    print(f"Shape after dedup  : {combined.shape}")
    print()

    combined.to_csv(OUTPUT_CSV, index=False)
    print(f"💾 Saved → {OUTPUT_CSV}  ({len(combined):,} rows, {len(combined.columns)} cols)")


if __name__ == "__main__":
    main()
