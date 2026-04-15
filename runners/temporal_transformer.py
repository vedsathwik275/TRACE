"""
TRACE Temporal Transformer
==========================
Predicts NBA player career outcomes following Achilles tendon injuries
by learning from 52-week sentiment trajectories.

Inputs:
  - sentiment_results.csv  (Phase 1/2 output — already collected)
  - player_outcomes.csv    (Phase 3 Stage 1 — you are collecting this)

Output:
  - P(successful_return) ∈ [0, 1] per player

Usage:
  python temporal_transformer.py --train
  python temporal_transformer.py --predict --player "Jayson Tatum" --injury_date 2025-05-10
"""

import os
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from sklearn.metrics import roc_auc_score, accuracy_score, classification_report, confusion_matrix
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# ─────────────────────────────────────────────
# 1. CONFIGURATION
# ─────────────────────────────────────────────

class Config:
    # Paths — adjust if your files live elsewhere
    SENTIMENT_CSV   = "sentiment_results.csv"
    OUTCOMES_CSV    = "player_outcomes.csv"
    CLASSIFICATIONS_CSV = "data/llm_classifications_full.csv"
    MIN_CONFIDENCE  = 0.5
    CHECKPOINT_DIR  = "checkpoints"
    FIGURES_DIR     = "figures"

    # Sequence parameters
    MAX_DAYS  = 364        # Track up to 52 weeks post-injury
    BIN_DAYS  = 7          # Weekly aggregation → 52 time steps
    SEQ_LEN   = MAX_DAYS // BIN_DAYS   # = 52

    # Feature columns produced per weekly bin
    # [sentiment_positive, sentiment_neutral, sentiment_negative, log_engagement, record_count]
    INPUT_DIM = 5

    # Transformer hyperparameters
    D_MODEL         = 128
    N_HEADS         = 8
    N_LAYERS        = 6
    DIM_FEEDFORWARD = 512
    DROPOUT         = 0.1

    # Training
    EPOCHS       = 100
    BATCH_SIZE   = 8        # Small — limited labeled players
    LR           = 1e-4
    WEIGHT_DECAY = 1e-4
    PATIENCE     = 15       # Early stopping

    # Temporal splits (by injury date)
    TRAIN_END = "2021-01-01"
    VAL_END   = "2022-01-01"
    # Test: 2022-01-01 onward

    # Outcome label threshold
    # outcome=1 if: days_out ≤ 540 AND post_per ≥ 0.80 × pre_per AND games_return ≥ 50
    # This is set in player_outcomes.csv — model just reads the 'outcome' column.

    RANDOM_SEED = 42


torch.manual_seed(Config.RANDOM_SEED)
np.random.seed(Config.RANDOM_SEED)


# ─────────────────────────────────────────────
# 2. DATA LOADING & SEQUENCE CONSTRUCTION
# ─────────────────────────────────────────────

def load_and_merge(sentiment_path: str, outcomes_path: str,
                   classifications_path: str = None) -> list[dict]:
    """
    Merge sentiment records with player outcome labels.

    Returns a list of episode dicts, one per player:
        {
            'player':       str,
            'injury_date':  pd.Timestamp,
            'outcome':      int (0 or 1),
            'sequence':     np.ndarray (52, 5),
            'mask':         np.ndarray (52,) bool — True where data exists
        }
    """
    print("Loading sentiment data...")
    sent_df = pd.read_csv(sentiment_path)

    # Normalise column names — your CSV may use slightly different names
    sent_df.columns = sent_df.columns.str.lower().str.strip()

    # Filter to Gemini-classified SUITABLE rows
    if classifications_path and Path(classifications_path).exists():
        cls_df = pd.read_csv(classifications_path)
        cls_df.columns = cls_df.columns.str.lower().str.strip()
        suitable_idx = set(
            cls_df.loc[
                (cls_df["classification"] == "SUITABLE") &
                (cls_df["confidence"] >= Config.MIN_CONFIDENCE),
                "row_index"
            ].tolist()
        )
        before = len(sent_df)
        sent_df = sent_df[sent_df.index.isin(suitable_idx)]
        print(f"Classification filter: kept {len(sent_df)} / {before} records "
              f"(SUITABLE, confidence >= {Config.MIN_CONFIDENCE})")
    else:
        print(f"WARNING: classifications file not found at "
              f"'{classifications_path}' — proceeding without filtering.")

    # Required columns in sentiment_results.csv
    required_sent = {"created_at",
                     "sentiment_positive", "sentiment_neutral", "sentiment_negative",
                     "engagement_score"}
    missing = required_sent - set(sent_df.columns)
    if missing:
        raise ValueError(
            f"sentiment_results.csv is missing columns: {missing}\n"
            f"Found: {list(sent_df.columns)}"
        )

    sent_df["created_at"] = pd.to_datetime(sent_df["created_at"], utc=True, errors="coerce")
    sent_df = sent_df.dropna(subset=["created_at"])
    if "player_name" in sent_df.columns:
        sent_df["_match_player"] = sent_df["player_name"].astype(str).str.strip().str.lower()
    elif "mentioned_players" in sent_df.columns:
        sent_df["_match_player"] = sent_df["mentioned_players"].astype(str).str.strip().str.lower()
    else:
        raise ValueError(
            "sentiment CSV must contain 'player_name' or 'mentioned_players' column"
        )

    print("Loading player outcome labels...")
    outcomes_df = pd.read_csv(outcomes_path)
    outcomes_df.columns = outcomes_df.columns.str.lower().str.strip()

    # Required columns in player_outcomes.csv
    required_out = {"player_name", "injury_date", "outcome"}
    missing = required_out - set(outcomes_df.columns)
    if missing:
        raise ValueError(
            f"player_outcomes.csv is missing columns: {missing}\n"
            f"Expected: player_name, injury_date, outcome (0 or 1)\n"
            f"Found: {list(outcomes_df.columns)}"
        )

    outcomes_df["injury_date"] = pd.to_datetime(outcomes_df["injury_date"], utc=True, errors="coerce")
    outcomes_df["player_name"] = outcomes_df["player_name"].str.strip().str.lower()
    outcomes_df = outcomes_df.dropna(subset=["injury_date", "outcome"])

    episodes = []
    for _, row in outcomes_df.iterrows():
        player   = row["player_name"]
        inj_date = row["injury_date"]
        outcome  = int(row["outcome"])

        # Filter sentiment records: same player, within [0, MAX_DAYS) of injury
        player_sent = sent_df[
            sent_df["_match_player"].str.contains(player, regex=False) &
            (sent_df["created_at"] >= inj_date) &
            (sent_df["created_at"] <  inj_date + pd.Timedelta(days=Config.MAX_DAYS))
        ].copy()

        if len(player_sent) < 5:
            print(f"  [SKIP] {player} — only {len(player_sent)} sentiment records "
                  f"(need ≥5). Collect more data for this player.")
            continue

        sequence, mask = build_sequence(player_sent, inj_date)

        episodes.append({
            "player":       player,
            "injury_date":  inj_date,
            "outcome":      outcome,
            "sequence":     sequence,
            "mask":         mask,
            "n_records":    len(player_sent),
        })

        print(f"  [OK]   {player:<25} outcome={outcome}  "
              f"records={len(player_sent):>4}  "
              f"weeks_with_data={mask.sum():>2}/{Config.SEQ_LEN}")

    print(f"\nTotal episodes: {len(episodes)}")
    if len(episodes) < 10:
        print("WARNING: Fewer than 10 episodes. Model will be unreliable. "
              "Target 20-25 labeled players.")
    return episodes


def build_sequence(player_sent: pd.DataFrame, injury_date: pd.Timestamp):
    """
    Bin irregular sentiment records into 52 weekly slots.

    Features per bin (INPUT_DIM = 5):
        [0] avg sentiment_positive
        [1] avg sentiment_neutral
        [2] avg sentiment_negative
        [3] log1p(sum of engagement_score)  ← captures buzz intensity
        [4] log1p(record count)              ← captures data density

    mask[i] = True if at least 1 record fell in week i.
    Empty weeks get zeros and mask=False (transformer ignores them).
    """
    seq = np.zeros((Config.SEQ_LEN, Config.INPUT_DIM), dtype=np.float32)
    mask = np.zeros(Config.SEQ_LEN, dtype=bool)

    player_sent = player_sent.copy()
    player_sent["days"] = (player_sent["created_at"] - injury_date).dt.days.clip(lower=0)
    player_sent["bin"] = (player_sent["days"] // Config.BIN_DAYS).clip(upper=Config.SEQ_LEN - 1).astype(int)

    for bin_idx, group in player_sent.groupby("bin"):
        seq[bin_idx, 0] = group["sentiment_positive"].mean()
        seq[bin_idx, 1] = group["sentiment_neutral"].mean()
        seq[bin_idx, 2] = group["sentiment_negative"].mean()
        seq[bin_idx, 3] = np.log1p(group["engagement_score"].fillna(0).sum())
        seq[bin_idx, 4] = np.log1p(len(group))
        mask[bin_idx] = True

    return seq, mask


# ─────────────────────────────────────────────
# 3. DATASET & DATA LOADERS
# ─────────────────────────────────────────────

class InjuryEpisodeDataset(Dataset):
    def __init__(self, episodes: list[dict]):
        self.episodes = episodes

    def __len__(self):
        return len(self.episodes)

    def __getitem__(self, idx):
        ep = self.episodes[idx]
        return {
            "sequence": torch.tensor(ep["sequence"], dtype=torch.float32),
            "mask":     torch.tensor(ep["mask"],     dtype=torch.bool),
            "label":    torch.tensor([ep["outcome"]], dtype=torch.float32),
            "player":   ep["player"],
        }


def collate_fn(batch):
    sequences = torch.stack([b["sequence"] for b in batch])
    masks     = torch.stack([b["mask"]     for b in batch])
    labels    = torch.stack([b["label"]    for b in batch])
    players   = [b["player"] for b in batch]
    return sequences, masks, labels, players


def temporal_split(episodes: list[dict]):
    """
    Split by injury_date — critical to prevent data leakage.
    Train: injured before 2021 | Val: 2021 | Test: 2022+
    """
    train_end = pd.Timestamp(Config.TRAIN_END, tz="UTC")
    val_end   = pd.Timestamp(Config.VAL_END,   tz="UTC")

    train = [e for e in episodes if e["injury_date"] < train_end]
    val   = [e for e in episodes if train_end <= e["injury_date"] < val_end]
    test  = [e for e in episodes if e["injury_date"] >= val_end]

    print(f"\nTemporal split:")
    print(f"  Train : {len(train)} players (injuries before 2021)")
    print(f"  Val   : {len(val)}   players (injuries in 2021)")
    print(f"  Test  : {len(test)}  players (injuries 2022+)")

    # If any split is empty, fall back to random 70/15/15
    if len(train) == 0 or len(test) == 0:
        print("\nWARNING: Not enough temporal spread for date-based split. "
              "Falling back to 70/15/15 random split.")
        np.random.shuffle(episodes)
        n = len(episodes)
        train = episodes[:int(0.7 * n)]
        val   = episodes[int(0.7 * n):int(0.85 * n)]
        test  = episodes[int(0.85 * n):]

    return train, val, test


# ─────────────────────────────────────────────
# 4. MODEL ARCHITECTURE
# ─────────────────────────────────────────────

class TemporalTransformer(nn.Module):
    """
    Transformer encoder that reads a 52-week sentiment trajectory
    and predicts P(career_success).

    Architecture:
        1. Linear projection  : (B, 52, 5) → (B, 52, 128)
        2. Learnable positional encoding: captures that week 8 ≠ week 40
        3. Transformer encoder (6 layers, 8 heads):
             self-attention → each week attends to all other weeks
             feed-forward   → per-position non-linear mixing
        4. Masked mean pooling: aggregate across only observed weeks
        5. Classification MLP → scalar logit → sigmoid → probability
    """

    def __init__(self):
        super().__init__()

        # Project raw features into transformer's working space
        self.input_proj = nn.Sequential(
            nn.Linear(Config.INPUT_DIM, Config.D_MODEL),
            nn.LayerNorm(Config.D_MODEL),
        )

        # Learnable positional encoding — one vector per week
        # The model learns that "week 2 post-injury" is fundamentally
        # different from "week 40 post-injury" without being told explicitly
        self.pos_enc = nn.Parameter(
            torch.randn(1, Config.SEQ_LEN, Config.D_MODEL) * 0.02
        )

        # Transformer encoder stack
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=Config.D_MODEL,
            nhead=Config.N_HEADS,
            dim_feedforward=Config.DIM_FEEDFORWARD,
            dropout=Config.DROPOUT,
            batch_first=True,
            norm_first=True,     # Pre-LN: more stable training
        )
        self.encoder = nn.TransformerEncoder(
            encoder_layer,
            num_layers=Config.N_LAYERS,
            enable_nested_tensor=False,
        )

        # Classification head
        self.classifier = nn.Sequential(
            nn.Linear(Config.D_MODEL, 64),
            nn.LayerNorm(64),
            nn.GELU(),
            nn.Dropout(Config.DROPOUT),
            nn.Linear(64, 1),
        )

        self._init_weights()

    def _init_weights(self):
        for m in self.modules():
            if isinstance(m, nn.Linear):
                nn.init.xavier_uniform_(m.weight)
                if m.bias is not None:
                    nn.init.zeros_(m.bias)

    def forward(self, x, mask):
        """
        Args:
            x    : (B, 52, 5)    — weekly sentiment features
            mask : (B, 52) bool  — True where a week has real data

        Returns:
            logits : (B, 1)  — pre-sigmoid; use BCEWithLogitsLoss
        """
        # Project to d_model
        x = self.input_proj(x)                   # (B, 52, 128)
        x = x + self.pos_enc                      # add positional info

        # TransformerEncoder src_key_padding_mask:
        # True = IGNORE that position (opposite of our "mask" convention)
        padding_mask = ~mask                      # (B, 52): True=empty week

        x = self.encoder(x, src_key_padding_mask=padding_mask)   # (B, 52, 128)

        # Masked mean pool — only average over weeks that actually had data
        mask_f = mask.unsqueeze(-1).float()       # (B, 52, 1)
        pooled = (x * mask_f).sum(dim=1) / mask_f.sum(dim=1).clamp(min=1)  # (B, 128)

        logits = self.classifier(pooled)          # (B, 1)
        return logits


# ─────────────────────────────────────────────
# 5. TRAINING
# ─────────────────────────────────────────────

def compute_pos_weight(episodes):
    """BCEWithLogitsLoss pos_weight balances the 0/1 label ratio."""
    n_pos = sum(e["outcome"] for e in episodes)
    n_neg = len(episodes) - n_pos
    if n_pos == 0 or n_neg == 0:
        return torch.tensor([1.0])
    return torch.tensor([n_neg / n_pos])


def evaluate(model, loader, criterion, device):
    model.eval()
    total_loss, all_probs, all_labels = 0.0, [], []

    with torch.no_grad():
        for seqs, masks, labels, _ in loader:
            seqs, masks, labels = seqs.to(device), masks.to(device), labels.to(device)
            logits = model(seqs, masks)
            total_loss += criterion(logits, labels).item()
            all_probs.extend(torch.sigmoid(logits).cpu().numpy().flatten())
            all_labels.extend(labels.cpu().numpy().flatten())

    all_probs  = np.array(all_probs)
    all_labels = np.array(all_labels)
    preds = (all_probs > 0.5).astype(int)

    metrics = {
        "loss":     total_loss / max(len(loader), 1),
        "accuracy": accuracy_score(all_labels, preds),
        "probs":    all_probs,
        "labels":   all_labels,
    }
    if len(np.unique(all_labels)) > 1:
        metrics["auc"] = roc_auc_score(all_labels, all_probs)
    else:
        metrics["auc"] = float("nan")
        print("  NOTE: Only one class in this split — AUC not defined.")

    return metrics


def train(episodes):
    os.makedirs(Config.CHECKPOINT_DIR, exist_ok=True)
    os.makedirs(Config.FIGURES_DIR, exist_ok=True)

    train_eps, val_eps, test_eps = temporal_split(episodes)

    train_loader = DataLoader(
        InjuryEpisodeDataset(train_eps),
        batch_size=Config.BATCH_SIZE, shuffle=True, collate_fn=collate_fn
    )
    val_loader = DataLoader(
        InjuryEpisodeDataset(val_eps),
        batch_size=Config.BATCH_SIZE, collate_fn=collate_fn
    )
    test_loader = DataLoader(
        InjuryEpisodeDataset(test_eps),
        batch_size=Config.BATCH_SIZE, collate_fn=collate_fn
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"\nDevice: {device}")

    model = TemporalTransformer().to(device)
    n_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"Model parameters: {n_params:,}")

    pos_weight = compute_pos_weight(train_eps).to(device)
    criterion  = nn.BCEWithLogitsLoss(pos_weight=pos_weight)

    optimizer = torch.optim.AdamW(
        model.parameters(), lr=Config.LR, weight_decay=Config.WEIGHT_DECAY
    )
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=5
    )

    history = {"train_loss": [], "val_loss": [], "val_acc": [], "val_auc": []}
    best_val_loss   = float("inf")
    patience_counter = 0
    best_epoch      = 0

    print("\n" + "═" * 60)
    print("  TRAINING")
    print("═" * 60)

    for epoch in range(1, Config.EPOCHS + 1):
        # ── Training step ──────────────────────────────────────
        model.train()
        epoch_loss = 0.0
        for seqs, masks, labels, _ in train_loader:
            seqs, masks, labels = seqs.to(device), masks.to(device), labels.to(device)
            optimizer.zero_grad()
            logits = model(seqs, masks)
            loss   = criterion(logits, labels)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            epoch_loss += loss.item()

        train_loss = epoch_loss / max(len(train_loader), 1)

        # ── Validation step ────────────────────────────────────
        val_m = evaluate(model, val_loader, criterion, device)
        scheduler.step(val_m["loss"])

        history["train_loss"].append(train_loss)
        history["val_loss"].append(val_m["loss"])
        history["val_acc"].append(val_m["accuracy"])
        history["val_auc"].append(val_m["auc"])

        if epoch % 5 == 0 or epoch == 1:
            print(f"Epoch {epoch:>3}/{Config.EPOCHS} | "
                  f"train_loss={train_loss:.4f}  "
                  f"val_loss={val_m['loss']:.4f}  "
                  f"val_acc={val_m['accuracy']:.3f}  "
                  f"val_auc={val_m['auc']:.3f}")

        # ── Checkpoint & early stopping ────────────────────────
        if val_m["loss"] < best_val_loss:
            best_val_loss = val_m["loss"]
            best_epoch    = epoch
            patience_counter = 0
            torch.save(
                {"epoch": epoch, "model_state": model.state_dict(),
                 "optimizer_state": optimizer.state_dict(),
                 "val_loss": best_val_loss, "val_auc": val_m["auc"]},
                os.path.join(Config.CHECKPOINT_DIR, "best_model.pt")
            )
            print(f"  ✓ Best model saved (epoch {epoch})")
        else:
            patience_counter += 1
            if patience_counter >= Config.PATIENCE:
                print(f"\nEarly stopping at epoch {epoch} "
                      f"(best was epoch {best_epoch})")
                break

    # ── Load best and test ─────────────────────────────────────
    ckpt = torch.load(os.path.join(Config.CHECKPOINT_DIR, "best_model.pt"),
                      map_location=device)
    model.load_state_dict(ckpt["model_state"])
    print(f"\nLoaded best model from epoch {ckpt['epoch']}")

    test_m = evaluate(model, test_loader, criterion, device)
    print("\n" + "═" * 60)
    print("  TEST RESULTS")
    print("═" * 60)
    print(f"  Accuracy : {test_m['accuracy']:.3f}  (target: 0.75–0.80)")
    print(f"  AUC      : {test_m['auc']:.3f}  (target: >0.80)")
    print()

    if len(test_eps) > 0:
        print(classification_report(
            test_m["labels"], (test_m["probs"] > 0.5).astype(int),
            target_names=["Decline (0)", "Success (1)"], digits=3
        ))

    plot_training(history, test_m, test_eps)
    return model, history, test_m


# ─────────────────────────────────────────────
# 6. VISUALISATION
# ─────────────────────────────────────────────

def plot_training(history, test_m, test_eps):
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    fig.suptitle("TRACE Temporal Transformer — Training Results", fontsize=13, y=1.02)

    # Loss curves
    axes[0].plot(history["train_loss"], label="Train", color="#2563eb")
    axes[0].plot(history["val_loss"],   label="Val",   color="#dc2626")
    axes[0].set_title("Loss")
    axes[0].set_xlabel("Epoch")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    # Val accuracy & AUC
    axes[1].plot(history["val_acc"], label="Accuracy", color="#059669")
    axes[1].plot(history["val_auc"], label="AUC",      color="#7c3aed")
    axes[1].axhline(0.75, color="gray", linestyle="--", alpha=0.5, label="Target acc")
    axes[1].axhline(0.80, color="gray", linestyle=":",  alpha=0.5, label="Target AUC")
    axes[1].set_title("Validation Metrics")
    axes[1].set_xlabel("Epoch")
    axes[1].set_ylim(0, 1)
    axes[1].legend()
    axes[1].grid(True, alpha=0.3)

    # Confusion matrix on test
    if len(test_eps) > 1:
        preds  = (test_m["probs"] > 0.5).astype(int)
        labels = test_m["labels"].astype(int)
        cm = confusion_matrix(labels, preds)
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", ax=axes[2],
                    xticklabels=["Decline", "Success"],
                    yticklabels=["Decline", "Success"])
        axes[2].set_title(f"Test Confusion Matrix\nAcc={test_m['accuracy']:.2f}  AUC={test_m['auc']:.2f}")
        axes[2].set_xlabel("Predicted")
        axes[2].set_ylabel("Actual")
    else:
        axes[2].text(0.5, 0.5, "Too few test samples\nfor confusion matrix",
                     ha="center", va="center", transform=axes[2].transAxes)
        axes[2].set_title("Test Confusion Matrix")

    plt.tight_layout()
    out = os.path.join(Config.FIGURES_DIR, "training_results.png")
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"\nFigure saved: {out}")
    plt.close()


def plot_player_timeline(episode: dict, save_path: str = None):
    """
    Visualise a single player's 52-week sentiment trajectory.
    Useful for the paper's qualitative analysis section.
    """
    seq  = episode["sequence"]   # (52, 5)
    mask = episode["mask"]       # (52,) bool
    weeks = np.arange(Config.SEQ_LEN)

    fig, axes = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    fig.suptitle(
        f"{episode['player'].title()} — "
        f"{'Successful Return ✓' if episode['outcome'] == 1 else 'Career Decline ✗'}",
        fontsize=12
    )

    # Sentiment
    axes[0].fill_between(weeks, seq[:, 0], alpha=0.4, color="#22c55e", label="Positive")
    axes[0].fill_between(weeks, seq[:, 1], alpha=0.2, color="#94a3b8", label="Neutral")
    axes[0].fill_between(weeks, seq[:, 2], alpha=0.4, color="#ef4444", label="Negative")
    axes[0].plot(weeks[mask], seq[mask, 0], "o", ms=3, color="#16a34a")
    axes[0].plot(weeks[mask], seq[mask, 2], "o", ms=3, color="#dc2626")
    axes[0].set_ylabel("Avg Sentiment Score")
    axes[0].legend(loc="upper right", fontsize=8)
    axes[0].set_ylim(0, 1)
    axes[0].grid(True, alpha=0.3)

    # Engagement (log-scaled)
    axes[1].bar(weeks, seq[:, 3], color="#3b82f6", alpha=0.7, label="log(engagement)")
    axes[1].set_ylabel("Log Engagement")
    axes[1].set_xlabel("Weeks post-injury")
    axes[1].grid(True, alpha=0.3)

    # Phase boundaries
    phase_boundaries = {2: "Surgery", 6: "Rehab", 36: "Return anticipation"}
    for week, label in phase_boundaries.items():
        for ax in axes:
            ax.axvline(week, color="gray", linestyle="--", alpha=0.4, linewidth=0.8)
        axes[0].text(week + 0.3, 0.95, label, fontsize=7, color="gray", va="top")

    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"Timeline saved: {save_path}")
    plt.close()
    return fig


# ─────────────────────────────────────────────
# 7. INFERENCE (live players)
# ─────────────────────────────────────────────

def predict_player(player_name: str, injury_date: str,
                   sentiment_path: str, checkpoint_path: str):
    """
    Run inference on a player with no outcome label yet
    (e.g., Tatum, Lillard, Haliburton — 2025 injuries).
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = TemporalTransformer().to(device)
    ckpt  = torch.load(checkpoint_path, map_location=device)
    model.load_state_dict(ckpt["model_state"])
    model.eval()

    sent_df = pd.read_csv(sentiment_path)
    sent_df.columns = sent_df.columns.str.lower().str.strip()
    sent_df["created_at"]  = pd.to_datetime(sent_df["created_at"], utc=True, errors="coerce")
    sent_df["player_name"] = sent_df["player_name"].str.strip().str.lower()

    inj_date = pd.Timestamp(injury_date, tz="UTC")
    player_lower = player_name.strip().lower()

    player_sent = sent_df[
        (sent_df["player_name"] == player_lower) &
        (sent_df["created_at"] >= inj_date) &
        (sent_df["created_at"] <  inj_date + pd.Timedelta(days=Config.MAX_DAYS))
    ]

    if len(player_sent) == 0:
        print(f"No sentiment records found for '{player_name}' after {injury_date}.")
        return None

    sequence, mask = build_sequence(player_sent, inj_date)

    seq_t  = torch.tensor(sequence, dtype=torch.float32).unsqueeze(0).to(device)
    mask_t = torch.tensor(mask,     dtype=torch.bool).unsqueeze(0).to(device)

    with torch.no_grad():
        logit = model(seq_t, mask_t)
        prob  = torch.sigmoid(logit).item()

    weeks_elapsed = (pd.Timestamp.now(tz="UTC") - inj_date).days // 7

    print("\n" + "═" * 50)
    print(f"  TRACE Prediction: {player_name.title()}")
    print("═" * 50)
    print(f"  Injury date       : {injury_date}")
    print(f"  Weeks elapsed     : {weeks_elapsed} / {Config.SEQ_LEN}")
    print(f"  Sentiment records : {len(player_sent)}")
    print(f"  P(success)        : {prob:.1%}")
    print(f"  Prediction        : {'✓ Successful Return' if prob > 0.5 else '✗ Career Decline'}")
    print(f"  Confidence        : {max(prob, 1 - prob):.1%}")
    print()

    episode = {
        "player": player_lower, "injury_date": inj_date,
        "outcome": -1,  # unknown
        "sequence": sequence, "mask": mask, "n_records": len(player_sent)
    }
    plot_player_timeline(
        episode,
        save_path=os.path.join(Config.FIGURES_DIR,
                               f"{player_lower.replace(' ', '_')}_timeline.png")
    )

    return {"player": player_name, "injury_date": injury_date,
            "prob_success": prob, "weeks_elapsed": weeks_elapsed,
            "n_records": len(player_sent)}


# ─────────────────────────────────────────────
# 8. EXPECTED player_outcomes.csv FORMAT
# ─────────────────────────────────────────────

OUTCOMES_TEMPLATE = """
player_outcomes.csv — required format
======================================

player_name,injury_date,outcome,pre_injury_per,post_injury_per,games_return_season,days_out,notes
kevin durant,2019-06-10,1,27.3,24.1,35,374,GSW Achilles — partial tear; returned with BKN
demarcus cousins,2018-01-26,0,26.5,10.2,30,588,Left Achilles rupture; never regained All-Star form
john wall,2019-01-26,0,19.5,12.3,40,921,Left Achilles; missed nearly 2 full seasons
klay thompson,2019-06-13,1,21.0,18.7,32,941,Right Achilles then knee; outcome=1 per your note
wesley matthews,2015-03-05,1,14.2,12.5,64,184,Right Achilles; returned same season next year
rudy gay,2017-01-18,1,16.4,13.1,54,292,Left Achilles; returned and played effectively

Outcome rule:
  outcome = 1  if  days_out ≤ 540
              AND  post_injury_per ≥ 0.80 × pre_injury_per
              AND  games_return_season ≥ 50
  outcome = 0  otherwise
"""

# ─────────────────────────────────────────────
# 9. ENTRY POINT
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TRACE Temporal Transformer")
    parser.add_argument("--train",    action="store_true", help="Train the model")
    parser.add_argument("--predict",  action="store_true", help="Predict for a new player")
    parser.add_argument("--player",   type=str, help="Player name for --predict")
    parser.add_argument("--injury_date", type=str, help="YYYY-MM-DD for --predict")
    parser.add_argument("--show_format", action="store_true",
                        help="Print the required player_outcomes.csv format")
    parser.add_argument("--sentiment_csv",       default=Config.SENTIMENT_CSV)
    parser.add_argument("--outcomes_csv",        default=Config.OUTCOMES_CSV)
    parser.add_argument("--classifications_csv", default=Config.CLASSIFICATIONS_CSV)
    parser.add_argument("--checkpoint",
                        default=os.path.join(Config.CHECKPOINT_DIR, "best_model.pt"))
    args = parser.parse_args()

    if args.show_format:
        print(OUTCOMES_TEMPLATE)
        return

    if args.train:
        if not Path(args.sentiment_csv).exists():
            print(f"ERROR: {args.sentiment_csv} not found.")
            return
        if not Path(args.outcomes_csv).exists():
            print(f"ERROR: {args.outcomes_csv} not found.")
            print("Run: python temporal_transformer.py --show_format")
            return

        episodes = load_and_merge(args.sentiment_csv, args.outcomes_csv,
                                   args.classifications_csv)
        if len(episodes) < 5:
            print("Not enough episodes to train. Need at least 5 labeled players.")
            return

        train(episodes)

    elif args.predict:
        if not args.player or not args.injury_date:
            print("--predict requires --player and --injury_date")
            return
        if not Path(args.checkpoint).exists():
            print(f"No checkpoint at {args.checkpoint}. Run --train first.")
            return

        os.makedirs(Config.FIGURES_DIR, exist_ok=True)
        predict_player(args.player, args.injury_date,
                       args.sentiment_csv, args.checkpoint)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
