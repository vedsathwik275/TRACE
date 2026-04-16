"""
compute_outcomes.py

Derives a binary outcome label for each player in player_stats_raw.csv.

Outcome = 1 only if ALL of:
  - days_out  <= 540  (returned within ~18 months)
  - post_injury_per >= 0.80 * pre_injury_per  (retained ≥ 80% of prior form)
  - games in return season >= 25              (played meaningful minutes)
Otherwise outcome = 0.
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
STATS_CSV = DATA_DIR / "player_stats_raw.csv"
OUTPUT_CSV = DATA_DIR / "player_outcomes.csv"

# ── Injury dates (hand-curated) ───────────────────────────────────────────────
INJURY_DATES: dict[str, str] = {
    "kevin_durant":      "2019-06-10",
    "demarcus_cousins":  "2018-01-26",
    "john_wall":         "2019-01-26",
    "klay_thompson":     "2020-11-18",
    "rudy_gay":          "2017-01-18",
    "brandon_jennings":  "2015-01-25",
    "wesley_matthews":   "2015-03-05",
    "jj_barea":          "2019-01-11",
    "rodney_hood":       "2019-12-06",
}

# ── Thresholds ────────────────────────────────────────────────────────────────
MAX_DAYS_OUT   = (365 * 2)
MIN_PER_RATIO  = 0.70
MIN_GAMES      = 30


def season_start_year(season: str) -> int:
    """Extract the start calendar year from a 'YYYY-YYYY' season string.

    Args:
        season: Season string, e.g. '2020-2021'.

    Returns:
        Integer start year, e.g. 2020.
    """
    return int(season.split("-")[0])


def compute_days_out(injury_date: pd.Timestamp, return_season: str) -> int:
    """Days between injury and October 1 of the return season's start year.

    Args:
        injury_date: Date of injury as a pandas Timestamp.
        return_season: Season string for the return season, e.g. '2020-2021'.

    Returns:
        Integer number of days out.
    """
    start_year = season_start_year(return_season)
    season_open = pd.Timestamp(f"{start_year}-10-01")
    return (season_open - injury_date).days


def main() -> None:
    # ── Load ──────────────────────────────────────────────────────────────────
    stats = pd.read_csv(STATS_CSV)
    stats.columns = stats.columns.str.lower().str.strip()
    print(f"🔍 Loaded player_stats_raw: {stats.shape}\n")

    records: list[dict] = []

    print("=" * 72)
    for player, group in stats.groupby("player_name", sort=False):
        group = group.reset_index(drop=True)

        if len(group) != 3:
            print(f"⚠️  {player}: expected 3 rows, got {len(group)} — skipping")
            continue

        pre_row1   = group.iloc[0]
        pre_row2   = group.iloc[1]
        return_row = group.iloc[2]

        pre_injury_per  = (pre_row1["per"] + pre_row2["per"]) / 2
        post_injury_per = return_row["per"]
        games_return    = int(return_row["g"])
        return_season   = return_row["season"]

        injury_date_str = INJURY_DATES.get(player)
        if injury_date_str is None:
            print(f"⚠️  {player}: no injury date found — skipping")
            continue

        injury_date = pd.Timestamp(injury_date_str)
        days_out    = compute_days_out(injury_date, return_season)

        # ── Criteria evaluation ───────────────────────────────────────────────
        ok_days  = days_out    <= MAX_DAYS_OUT
        ok_per   = post_injury_per >= MIN_PER_RATIO * pre_injury_per
        ok_games = games_return >= MIN_GAMES
        outcome  = int(ok_days and ok_per and ok_games)

        # ── Per-player printout ───────────────────────────────────────────────
        per_threshold = MIN_PER_RATIO * pre_injury_per
        print(f"Player : {player}")
        print(f"  Pre-injury seasons : {pre_row1['season']} (PER {pre_row1['per']}),"
              f" {pre_row2['season']} (PER {pre_row2['per']})")
        print(f"  Pre-injury avg PER : {pre_injury_per:.2f}")
        print(f"  Return season      : {return_season}  →  PER {post_injury_per},"
              f" {games_return} G")
        print(f"  Injury date        : {injury_date_str}")
        print(f"  Days out           : {days_out}"
              f"  {'✅' if ok_days else '❌'} (<= {MAX_DAYS_OUT})")
        print(f"  PER retention      : {post_injury_per:.1f} / {per_threshold:.2f}"
              f"  {'✅' if ok_per else '❌'} (>= 80% of {pre_injury_per:.2f})")
        print(f"  Games played       : {games_return}"
              f"  {'✅' if ok_games else '❌'} (>= {MIN_GAMES})")
        print(f"  OUTCOME            : {'1 (success)' if outcome else '0 (decline)'}")
        print("-" * 72)

        records.append({
            "player":             player,
            "injury_date":        injury_date_str,
            "pre_injury_per":     round(pre_injury_per, 2),
            "post_injury_per":    post_injury_per,
            "days_out":           days_out,
            "games_return_season": games_return,
            "outcome":            outcome,
        })

    # ── Summary ───────────────────────────────────────────────────────────────
    outcomes_df = pd.DataFrame(records)
    print()
    print("=== Outcome summary ===")
    print(f"  outcome=1 (success) : {(outcomes_df['outcome'] == 1).sum()}")
    print(f"  outcome=0 (decline) : {(outcomes_df['outcome'] == 0).sum()}")
    print()

    # ── Save ──────────────────────────────────────────────────────────────────
    outcomes_df.to_csv(OUTPUT_CSV, index=False)
    print(f"💾 Saved → {OUTPUT_CSV}  ({len(outcomes_df)} rows)")


if __name__ == "__main__":
    main()
