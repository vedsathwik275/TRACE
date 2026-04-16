"""
train_classifier.py
-------------------
Trains a Logistic Regression classifier using Leave-One-Out CV on
temporal_sequences.csv produced by build_sequences.py.
"""

import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.model_selection import LeaveOneOut
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"
SEQUENCES_PATH = DATA_DIR / "temporal_sequences.csv"
PREDICTIONS_PATH = DATA_DIR / "loo_predictions.csv"
MODEL_PATH = DATA_DIR / "trace_classifier.pkl"

NUM_BINS = 52
TIME_FEATURES = [
    "avg_sentiment_positive",
    "avg_sentiment_neutral",
    "avg_sentiment_negative",
    "total_engagement",
]
N_TIME_FEATURES = NUM_BINS * len(TIME_FEATURES)  # 208


# ---------------------------------------------------------------------------
# 1. Load data
# ---------------------------------------------------------------------------
def load_data() -> pd.DataFrame:
    df = pd.read_csv(SEQUENCES_PATH)
    print(f"\n{'=' * 60}")
    print("  temporal_sequences.csv")
    print(f"{'=' * 60}")
    print(f"Shape: {df.shape}")
    print(f"\nOutcome distribution:")
    counts = df.groupby("outcome")["player_name"].nunique()
    for label, n in counts.items():
        print(f"  outcome={label}: {n} player(s)")
    if df["outcome"].nunique() == 1:
        print("\n  *** NOTE: Only one outcome class present in the dataset. ***")
        print("  *** LOO-CV will still run but the problem is trivially one-class. ***")
    return df


# ---------------------------------------------------------------------------
# 2. Feature engineering
# ---------------------------------------------------------------------------
def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray, np.ndarray, list[str]]:
    """Returns (player_df, X, y, feature_names)."""
    records = []

    for player, grp in df.groupby("player_name"):
        grp = grp.sort_values("bin_index").set_index("bin_index")

        # Reindex to ensure all 52 bins exist
        grp = grp.reindex(range(NUM_BINS), fill_value=0)

        # Flatten time-series features: [feat0_bin0, feat1_bin0, ..., feat3_bin51]
        time_vec = []
        feat_names = []
        for b in range(NUM_BINS):
            for feat in TIME_FEATURES:
                time_vec.append(grp.loc[b, feat] if feat in grp.columns else 0.0)
                feat_names.append(f"{feat}_bin{b:02d}")

        # Summary features over non-zero bins only
        non_zero = grp[grp["record_count"] > 0]
        mean_pos = non_zero["avg_sentiment_positive"].mean() if len(non_zero) > 0 else 0.0
        mean_neg = non_zero["avg_sentiment_negative"].mean() if len(non_zero) > 0 else 0.0
        total_recs = grp["record_count"].sum()
        non_zero_bins = (grp["record_count"] > 0).sum()

        summary_vec = [mean_pos, mean_neg, total_recs, non_zero_bins]
        summary_names = [
            "summary_mean_positive",
            "summary_mean_negative",
            "summary_total_records",
            "summary_non_zero_bins",
        ]

        outcome = grp["outcome"].iloc[0] if "outcome" in grp.columns else np.nan

        records.append(
            {
                "player_name": player,
                "outcome": outcome,
                "features": time_vec + summary_vec,
            }
        )

    # Assemble into arrays
    all_feat_names = feat_names + summary_names
    player_df = pd.DataFrame([{"player_name": r["player_name"], "outcome": r["outcome"]}
                               for r in records])
    X = np.array([r["features"] for r in records], dtype=float)
    y = np.array([r["outcome"] for r in records], dtype=int)

    assert X.shape[1] == N_TIME_FEATURES + 4, (
        f"Expected {N_TIME_FEATURES + 4} features, got {X.shape[1]}"
    )

    print(f"\n{'=' * 60}")
    print("  Feature matrix")
    print(f"{'=' * 60}")
    print(f"X shape: {X.shape}  (players × features)")
    print(f"y shape: {y.shape}")
    print(f"\nPlayers in dataset:")
    for i, (pname, label) in enumerate(zip(player_df["player_name"], y)):
        print(f"  [{i}] {pname:<30}  outcome={label}")

    return player_df, X, y, all_feat_names


# ---------------------------------------------------------------------------
# 4. LOO-CV
# ---------------------------------------------------------------------------
def run_loo_cv(
    player_df: pd.DataFrame, X: np.ndarray, y: np.ndarray
) -> pd.DataFrame:
    loo = LeaveOneOut()
    preds = []

    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(C=1.0, max_iter=1000, solver="lbfgs")),
    ])

    for train_idx, test_idx in loo.split(X):
        X_train, X_test = X[train_idx], X[test_idx]
        y_train = y[train_idx]

        n_classes_train = len(np.unique(y_train))
        if n_classes_train < 2:
            # Only one class in training fold — predict that class
            pred = int(y_train[0])
        else:
            pipe.fit(X_train, y_train)
            pred = int(pipe.predict(X_test)[0])

        preds.append(pred)

    player_df = player_df.copy()
    player_df["predicted_outcome"] = preds
    player_df["correct"] = player_df["outcome"].astype(int) == player_df["predicted_outcome"]
    player_df = player_df.rename(columns={"outcome": "true_outcome"})
    return player_df


# ---------------------------------------------------------------------------
# 5. Results
# ---------------------------------------------------------------------------
def print_results(results: pd.DataFrame) -> None:
    print(f"\n{'=' * 60}")
    print("  LOO-CV Results")
    print(f"{'=' * 60}")

    acc = accuracy_score(results["true_outcome"], results["predicted_outcome"])
    print(f"Accuracy: {acc:.3f} ({results['correct'].sum()}/{len(results)} correct)\n")

    print(f"{'Player':<30} {'True':>6} {'Pred':>6} {'Correct':>8}")
    print("-" * 55)
    for _, row in results.iterrows():
        mark = "✓" if row["correct"] else "✗"
        print(f"{row['player_name']:<30} {row['true_outcome']:>6} "
              f"{row['predicted_outcome']:>6}  {mark}")

    cm = confusion_matrix(results["true_outcome"], results["predicted_outcome"])
    labels = sorted(results["true_outcome"].unique())
    print(f"\nConfusion matrix (rows=true, cols=predicted):")
    print(f"Labels: {labels}")
    print(cm)

    wrong = results[~results["correct"]]
    if wrong.empty:
        print("\nAll predictions correct.")
    else:
        print(f"\nMis-classified players ({len(wrong)}):")
        for _, row in wrong.iterrows():
            print(f"  {row['player_name']}: true={row['true_outcome']}, "
                  f"predicted={row['predicted_outcome']}")


# ---------------------------------------------------------------------------
# 6. Final model on all data + feature importances
# ---------------------------------------------------------------------------
def train_final_model(
    X: np.ndarray, y: np.ndarray, feat_names: list[str]
) -> Pipeline:
    pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(C=1.0, max_iter=1000, solver="lbfgs")),
    ])

    n_classes = len(np.unique(y))
    if n_classes < 2:
        print(f"\n{'=' * 60}")
        print("  Final model (trained on all players)")
        print(f"{'=' * 60}")
        print("  Only one outcome class — skipping feature importance (logistic regression")
        print("  requires at least 2 classes). Model saved with DummyClassifier fallback.")
        from sklearn.dummy import DummyClassifier
        pipe = Pipeline([
            ("scaler", StandardScaler()),
            ("clf", DummyClassifier(strategy="most_frequent")),
        ])
        pipe.fit(X, y)
        return pipe

    pipe.fit(X, y)
    coefs = pipe.named_steps["clf"].coef_[0]  # shape (n_features,)
    top10_idx = np.argsort(np.abs(coefs))[::-1][:10]

    print(f"\n{'=' * 60}")
    print("  Final model — top 10 features by |coefficient|")
    print(f"{'=' * 60}")
    print(f"{'Rank':<5} {'Feature':<45} {'Coef':>8}  Week range")
    print("-" * 75)
    for rank, idx in enumerate(top10_idx, 1):
        fname = feat_names[idx]
        coef = coefs[idx]
        # Decode week range from feature name
        if "_bin" in fname:
            bin_num = int(fname.split("_bin")[-1])
            day_start = bin_num * 7
            day_end = day_start + 6
            week_label = f"days {day_start}–{day_end} (week {bin_num + 1})"
        else:
            week_label = "summary stat"
        print(f"{rank:<5} {fname:<45} {coef:>8.4f}  {week_label}")

    return pipe


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    df = load_data()
    player_df, X, y, feat_names = build_features(df)
    results = run_loo_cv(player_df, X, y)
    print_results(results)
    final_model = train_final_model(X, y, feat_names)

    # Save predictions
    results[["player_name", "true_outcome", "predicted_outcome", "correct"]].to_csv(
        PREDICTIONS_PATH, index=False
    )
    print(f"\nPredictions saved  →  {PREDICTIONS_PATH}")

    # Save model
    joblib.dump(final_model, MODEL_PATH)
    print(f"Model saved        →  {MODEL_PATH}")


if __name__ == "__main__":
    main()
