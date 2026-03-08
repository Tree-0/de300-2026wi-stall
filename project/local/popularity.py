#!/usr/bin/env python3
"""
ListenBrainz Popularity Prediction Pipeline

Standalone script (no Jupyter required) — reads credentials from .env
Trains XGBoost + LSTM models to predict artist/track popularity surges

Usage:
    python popularity.py
"""

import os
import sys
import warnings
from pathlib import Path
from dotenv import load_dotenv

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from xgboost import XGBClassifier, XGBRegressor
from sklearn.metrics import (
    roc_auc_score, f1_score, precision_score, recall_score,
    mean_squared_error, mean_absolute_error,
    roc_curve, precision_recall_curve,
)
from sklearn.model_selection import TimeSeriesSplit
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

warnings.filterwarnings("ignore")

# ============================================================================
# CONFIG & SETUP
# ============================================================================

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
print(f"Loading .env from: {env_path}")
if not env_path.exists():
    print(f"❌ ERROR: .env not found at {env_path}")
    sys.exit(1)

load_dotenv(dotenv_path=env_path, override=True)

# Get credentials
PGHOST = os.getenv("PGHOST", "127.0.0.1")
PGPORT = int(os.getenv("PGPORT", 5433))
PGDATABASE = os.getenv("PGDATABASE", "postgres")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")

print("\n" + "="*70)
print("LISTENBRAINZ POPULARITY PREDICTION")
print("="*70)
print(f"DB: {PGHOST}:{PGPORT}/{PGDATABASE} as {PGUSER}\n")

# ============================================================================
# TEST CONNECTION
# ============================================================================

def test_connection():
    """Test direct PostgreSQL connection."""
    print("Testing PostgreSQL connection...")
    try:
        conn = psycopg2.connect(
            host=PGHOST,
            port=PGPORT,
            database=PGDATABASE,
            user=PGUSER,
            password=PGPASSWORD,
            connect_timeout=5
        )
        print("✓ Connection successful!\n")
        
        # List tables
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema='public' ORDER BY table_name
            """)
            tables = [t[0] for t in cur.fetchall()]
            print(f"Tables ({len(tables)}):")
            for t in tables:
                print(f"  • {t}")
            
            # Check row counts
            print("\nRow counts:")
            for table in ['artist_daily_stats', 'artist_daily_listens', 
                          'track_daily_stats', 'track_daily_listens']:
                if table in tables:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    print(f"  • {table}: {count:,} rows")
        
        conn.close()
        return True
        
    except psycopg2.OperationalError as e:
        print(f"✗ Connection FAILED: {e}\n")
        print("Make sure SSH tunnel is active:")
        print("  ssh -i 'sm-key.pem' -L 5433:...")
        return False

if not test_connection():
    sys.exit(1)

# ============================================================================
# SPARK SETUP
# ============================================================================

print("\nSetting up Spark session...")
spark = (
    SparkSession.builder
    .appName("ListenBrainz Popularity")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

jdbc_url = f"jdbc:postgresql://{PGHOST}:{PGPORT}/{PGDATABASE}"
jdbc_props = {
    "user": PGUSER,
    "password": PGPASSWORD,
    "driver": "org.postgresql.Driver",
}
print("✓ Spark session ready\n")

# ============================================================================
# DATA LOADING & FEATURE ENGINEERING
# ============================================================================

def load_and_engineer(stats_table, listens_table, id_col):
    """Load and engineer features from raw data."""
    print(f"Loading {stats_table} and {listens_table}...")
    stats = spark.read.jdbc(url=jdbc_url, table=stats_table, properties=jdbc_props)
    listens = spark.read.jdbc(url=jdbc_url, table=listens_table, properties=jdbc_props)
    print(f"  Stats: {stats.count():,} rows")
    print(f"  Listens: {listens.count():,} rows")

    df = stats.join(listens, on=["day", id_col], how="inner")

    w_id = Window.partitionBy(id_col).orderBy("day")
    w_id_7 = w_id.rowsBetween(-6, 0)
    w_id_30 = w_id.rowsBetween(-29, 0)
    w_first = Window.partitionBy(id_col)

    df = (
        df
        .withColumn("_7d_lag7", F.lag("listen_count_past_7_days", 7).over(w_id))
        .withColumn("momentum_7d", F.col("listen_count_past_7_days") - F.col("_7d_lag7"))
        .withColumn("_30d_lag30", F.lag("listen_count_past_30_days", 30).over(w_id))
        .withColumn("momentum_30d", F.col("listen_count_past_30_days") - F.col("_30d_lag30"))
        .withColumn("velocity_ratio",
                    F.when(F.col("listen_count_past_30_days") > 0,
                           F.col("listen_count_past_7_days") / F.col("listen_count_past_30_days"))
                    .otherwise(None))
        .withColumn("_first_day", F.min("day").over(w_first))
        .withColumn("days_since_first_listen", F.datediff(F.col("day"), F.col("_first_day")))
        .withColumn("listen_count_lag_1d", F.lag("listen_count", 1).over(w_id))
        .withColumn("listen_count_lag_7d", F.lag("listen_count", 7).over(w_id))
        .withColumn("day_of_week", (F.dayofweek("day") + 5) % 7)
        .drop("_7d_lag7", "_30d_lag30", "_first_day")
    )

    print(f"  Feature-engineered: {df.count():,} rows, {len(df.columns)} columns\n")
    return df

print("Loading data...\n")
artist_df = load_and_engineer("artist_daily_stats", "artist_daily_listens", "artist_mbid")
track_df = load_and_engineer("track_daily_stats", "track_daily_listens", "recording_id")

# ============================================================================
# ADD TARGETS
# ============================================================================

def add_targets(df, id_col, threshold=0.95):
    """Add forward-looking target labels."""
    w_id = Window.partitionBy(id_col).orderBy("day")
    w_fwd_7 = w_id.rowsBetween(1, 7)
    w_fwd_30 = w_id.rowsBetween(1, 30)

    df = (
        df
        .withColumn("_max_gp_7", F.max("growth_percentile").over(w_fwd_7))
        .withColumn("_max_gp_30", F.max("growth_percentile").over(w_fwd_30))
        .withColumn("_mean_gp_7", F.avg("growth_percentile").over(w_fwd_7))
        .withColumn("_mean_gp_30", F.avg("growth_percentile").over(w_fwd_30))
        .withColumn("_fwd_count_7", F.count("growth_percentile").over(w_fwd_7))
        .withColumn("_fwd_count_30", F.count("growth_percentile").over(w_fwd_30))
    )

    df = df.filter(F.col("_fwd_count_30") >= 30)

    df = (
        df
        .withColumn("is_popular_7d", F.when(F.col("_max_gp_7") >= threshold, 1).otherwise(0))
        .withColumn("is_popular_30d", F.when(F.col("_max_gp_30") >= threshold, 1).otherwise(0))
        .withColumn("future_growth_pctl_7d", F.col("_mean_gp_7"))
        .withColumn("future_growth_pctl_30d", F.col("_mean_gp_30"))
        .drop("_max_gp_7", "_max_gp_30", "_mean_gp_7", "_mean_gp_30", "_fwd_count_7", "_fwd_count_30")
    )

    n = df.count()
    pos_7 = df.filter(F.col("is_popular_7d") == 1).count()
    pos_30 = df.filter(F.col("is_popular_30d") == 1).count()
    print(f"  {id_col}: {n:,} rows, 7d: {100*pos_7/n:.1f}% positive, 30d: {100*pos_30/n:.1f}% positive\n")
    return df

print("Adding target labels...\n")
artist_df = add_targets(artist_df, "artist_mbid")
track_df = add_targets(track_df, "recording_id")

# ============================================================================
# CONVERT TO PANDAS
# ============================================================================

def spark_to_pandas_split(sdf, id_col, test_days=30):
    """Convert Spark DF to pandas with train/test split."""
    pdf = sdf.toPandas()
    pdf["day"] = pd.to_datetime(pdf["day"])
    pdf = pdf.sort_values(["day", id_col]).reset_index(drop=True)

    cutoff = pdf["day"].max() - pd.Timedelta(days=test_days)
    train = pdf[pdf["day"] <= cutoff].copy()
    test = pdf[pdf["day"] > cutoff].copy()

    target_cols = ["is_popular_7d", "is_popular_30d",
                   "future_growth_pctl_7d", "future_growth_pctl_30d"]
    exclude = {id_col, "day"} | set(target_cols)
    feature_cols = [c for c in pdf.columns 
                   if c not in exclude and pdf[c].dtype in ("float64", "float32", "int64", "int32")]

    print(f"  {id_col}:")
    print(f"    Train: {len(train):,} rows ({train['day'].min().date()} – {train['day'].max().date()})")
    print(f"    Test:  {len(test):,} rows ({test['day'].min().date()} – {test['day'].max().date()})")
    print(f"    Features: {len(feature_cols)}\n")
    
    return train, test, feature_cols, target_cols

print("Converting to pandas and splitting...\n")
art_train, art_test, art_features, target_cols = spark_to_pandas_split(artist_df, "artist_mbid")
trk_train, trk_test, trk_features, _ = spark_to_pandas_split(track_df, "recording_id")

spark.stop()
print("✓ Spark session stopped\n")

# ============================================================================
# XGBOOST MODELS
# ============================================================================

def train_xgb_classifier(train_df, test_df, feature_cols, target_col, n_splits=3):
    """Train XGBoost classifier."""
    X_train = train_df[feature_cols].fillna(0).values
    y_train = train_df[target_col].values
    X_test = test_df[feature_cols].fillna(0).values
    y_test = test_df[target_col].values

    pos_rate = y_train.mean()
    spw = (1 - pos_rate) / max(pos_rate, 1e-6)

    tscv = TimeSeriesSplit(n_splits=n_splits)
    best_rounds = []
    for fold, (tr_idx, val_idx) in enumerate(tscv.split(X_train)):
        model = XGBClassifier(
            n_estimators=500, max_depth=6, learning_rate=0.05,
            scale_pos_weight=spw, eval_metric="auc",
            early_stopping_rounds=30, random_state=42, tree_method="hist",
        )
        model.fit(X_train[tr_idx], y_train[tr_idx],
                  eval_set=[(X_train[val_idx], y_train[val_idx])],
                  verbose=False)
        best_rounds.append(model.best_iteration)
        val_proba = model.predict_proba(X_train[val_idx])[:, 1]
        val_auc = roc_auc_score(y_train[val_idx], val_proba) if y_train[val_idx].sum() > 0 else float("nan")
        print(f"    Fold {fold+1}: best_iter={model.best_iteration}, val_AUC={val_auc:.4f}")

    final_n = int(np.median(best_rounds))
    model = XGBClassifier(
        n_estimators=final_n, max_depth=6, learning_rate=0.05,
        scale_pos_weight=spw, eval_metric="auc", random_state=42,
        tree_method="hist",
    )
    model.fit(X_train, y_train, verbose=False)

    test_proba = model.predict_proba(X_test)[:, 1]
    test_pred = (test_proba >= 0.5).astype(int)

    metrics = {
        "roc_auc": roc_auc_score(y_test, test_proba) if y_test.sum() > 0 else float("nan"),
        "f1": f1_score(y_test, test_pred, zero_division=0),
        "precision": precision_score(y_test, test_pred, zero_division=0),
        "recall": recall_score(y_test, test_pred, zero_division=0),
    }
    print(f"    Test: AUC={metrics['roc_auc']:.4f} F1={metrics['f1']:.4f} "
          f"Prec={metrics['precision']:.4f} Rec={metrics['recall']:.4f}\n")
    return model, metrics, test_proba


def train_xgb_regressor(train_df, test_df, feature_cols, target_col, n_splits=3):
    """Train XGBoost regressor."""
    X_train = train_df[feature_cols].fillna(0).values
    y_train = train_df[target_col].values
    X_test = test_df[feature_cols].fillna(0).values
    y_test = test_df[target_col].values

    tscv = TimeSeriesSplit(n_splits=n_splits)
    best_rounds = []
    for fold, (tr_idx, val_idx) in enumerate(tscv.split(X_train)):
        model = XGBRegressor(
            n_estimators=500, max_depth=6, learning_rate=0.05,
            eval_metric="rmse", early_stopping_rounds=30,
            random_state=42, tree_method="hist",
        )
        model.fit(X_train[tr_idx], y_train[tr_idx],
                  eval_set=[(X_train[val_idx], y_train[val_idx])],
                  verbose=False)
        best_rounds.append(model.best_iteration)

    final_n = int(np.median(best_rounds))
    model = XGBRegressor(
        n_estimators=final_n, max_depth=6, learning_rate=0.05,
        eval_metric="rmse", random_state=42, tree_method="hist",
    )
    model.fit(X_train, y_train, verbose=False)

    test_preds = model.predict(X_test)
    test_binary = (test_preds >= 0.95).astype(int)
    y_test_binary = (y_test >= 0.95).astype(int)

    metrics = {
        "rmse": np.sqrt(mean_squared_error(y_test, test_preds)),
        "mae": mean_absolute_error(y_test, test_preds),
        "roc_auc_thresholded": roc_auc_score(y_test_binary, test_preds) if y_test_binary.sum() > 0 else float("nan"),
        "f1_thresholded": f1_score(y_test_binary, test_binary, zero_division=0),
    }
    print(f"    Test: RMSE={metrics['rmse']:.4f} MAE={metrics['mae']:.4f} "
          f"AUC={metrics['roc_auc_thresholded']:.4f} F1={metrics['f1_thresholded']:.4f}\n")
    return model, metrics, test_preds


print("="*70)
print("TRAINING XGBOOST MODELS (8 total)")
print("="*70 + "\n")

xgb_results = {}
configs = [
    ("artist", art_train, art_test, art_features),
    ("track", trk_train, trk_test, trk_features),
]

for entity, train_df, test_df, feat_cols in configs:
    for horizon in ["7d", "30d"]:
        clf_target = f"is_popular_{horizon}"
        reg_target = f"future_growth_pctl_{horizon}"

        print(f"XGBoost Classifier — {entity} — {horizon}")
        m, met, proba = train_xgb_classifier(train_df, test_df, feat_cols, clf_target)
        xgb_results[(entity, horizon, "clf")] = (m, met, proba)

        print(f"XGBoost Regressor — {entity} — {horizon}")
        m, met, preds = train_xgb_regressor(train_df, test_df, feat_cols, reg_target)
        xgb_results[(entity, horizon, "reg")] = (m, met, preds)

print("✓ XGBoost training complete\n")

# ============================================================================
# LSTM MODELS
# ============================================================================

class TimeSeriesDataset(Dataset):
    """Sliding-window dataset for LSTM."""
    def __init__(self, df, id_col, feature_cols, target_col, seq_len=30):
        self.sequences = []
        self.targets = []

        for _, group in df.groupby(id_col):
            group = group.sort_values("day")
            X = group[feature_cols].fillna(0).values.astype(np.float32)
            y = group[target_col].values.astype(np.float32)

            for i in range(seq_len, len(group)):
                self.sequences.append(X[i - seq_len : i])
                self.targets.append(y[i])

        self.sequences = np.array(self.sequences)
        self.targets = np.array(self.targets)

    def __len__(self):
        return len(self.targets)

    def __getitem__(self, idx):
        return (
            torch.tensor(self.sequences[idx], dtype=torch.float32),
            torch.tensor(self.targets[idx], dtype=torch.float32),
        )


class PopularityLSTM(nn.Module):
    def __init__(self, n_features, hidden_size=64, num_layers=2, dropout=0.2):
        super().__init__()
        self.lstm = nn.LSTM(n_features, hidden_size, num_layers,
                            batch_first=True, dropout=dropout)
        self.head = nn.Linear(hidden_size, 1)

    def forward(self, x):
        _, (h_n, _) = self.lstm(x)
        out = self.head(h_n[-1])
        return out.squeeze(-1)


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def train_lstm(train_df, test_df, id_col, feature_cols, target_col,
               task="clf", epochs=30, batch_size=256, lr=1e-3):
    """Train LSTM model."""
    train_ds = TimeSeriesDataset(train_df, id_col, feature_cols, target_col)
    test_ds = TimeSeriesDataset(test_df, id_col, feature_cols, target_col)

    if len(train_ds) == 0 or len(test_ds) == 0:
        print(f"    ⚠ Not enough sequences — skipping\n")
        return None, {}, np.array([])

    print(f"    Train sequences: {len(train_ds):,}  Test sequences: {len(test_ds):,}")

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False)

    n_features = train_ds.sequences.shape[2]
    model = PopularityLSTM(n_features).to(device)

    if task == "clf":
        pos_rate = train_ds.targets.mean()
        pos_weight = torch.tensor([(1 - pos_rate) / max(pos_rate, 1e-6)], device=device)
        criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    else:
        criterion = nn.MSELoss()

    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5, factor=0.5)

    best_loss = float("inf")
    best_state = None
    patience_counter = 0

    for epoch in range(epochs):
        model.train()
        train_loss = 0
        for X_batch, y_batch in train_loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            optimizer.zero_grad()
            out = model(X_batch)
            loss = criterion(out, y_batch)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            train_loss += loss.item() * len(y_batch)
        train_loss /= len(train_ds)

        model.eval()
        val_loss = 0
        with torch.no_grad():
            for X_batch, y_batch in test_loader:
                X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                out = model(X_batch)
                val_loss += criterion(out, y_batch).item() * len(y_batch)
        val_loss /= len(test_ds)
        scheduler.step(val_loss)

        if val_loss < best_loss:
            best_loss = val_loss
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1

        if (epoch + 1) % 5 == 0:
            print(f"      Epoch {epoch+1:3d}: train_loss={train_loss:.4f}  val_loss={val_loss:.4f}")

        if patience_counter >= 10:
            print(f"      Early stopping at epoch {epoch+1}")
            break

    model.load_state_dict(best_state)
    model.eval()
    all_preds, all_targets = [], []
    with torch.no_grad():
        for X_batch, y_batch in test_loader:
            X_batch = X_batch.to(device)
            out = model(X_batch)
            if task == "clf":
                out = torch.sigmoid(out)
            all_preds.append(out.cpu().numpy())
            all_targets.append(y_batch.numpy())

    preds = np.concatenate(all_preds)
    targets = np.concatenate(all_targets)

    metrics = {}
    if task == "clf":
        binary_preds = (preds >= 0.5).astype(int)
        metrics["roc_auc"] = roc_auc_score(targets, preds) if targets.sum() > 0 else float("nan")
        metrics["f1"] = f1_score(targets, binary_preds, zero_division=0)
        metrics["precision"] = precision_score(targets, binary_preds, zero_division=0)
        metrics["recall"] = recall_score(targets, binary_preds, zero_division=0)
        print(f"    Test: AUC={metrics['roc_auc']:.4f} F1={metrics['f1']:.4f} "
              f"Prec={metrics['precision']:.4f} Rec={metrics['recall']:.4f}\n")
    else:
        binary_preds = (preds >= 0.95).astype(int)
        binary_targets = (targets >= 0.95).astype(int)
        metrics["rmse"] = np.sqrt(mean_squared_error(targets, preds))
        metrics["mae"] = mean_absolute_error(targets, preds)
        metrics["roc_auc_thresholded"] = roc_auc_score(binary_targets, preds) if binary_targets.sum() > 0 else float("nan")
        metrics["f1_thresholded"] = f1_score(binary_targets, binary_preds, zero_division=0)
        print(f"    Test: RMSE={metrics['rmse']:.4f} MAE={metrics['mae']:.4f} "
              f"AUC={metrics['roc_auc_thresholded']:.4f} F1={metrics['f1_thresholded']:.4f}\n")

    return model, metrics, preds


print("="*70)
print("TRAINING LSTM MODELS (8 total)")
print("="*70 + "\n")

lstm_results = {}
lstm_configs = [
    ("artist", art_train, art_test, "artist_mbid", art_features),
    ("track", trk_train, trk_test, "recording_id", trk_features),
]

for entity, train_df, test_df, id_col, feat_cols in lstm_configs:
    for horizon in ["7d", "30d"]:
        clf_target = f"is_popular_{horizon}"
        reg_target = f"future_growth_pctl_{horizon}"

        print(f"LSTM Classifier — {entity} — {horizon}")
        m, met, preds = train_lstm(train_df, test_df, id_col, feat_cols, clf_target, task="clf")
        lstm_results[(entity, horizon, "clf")] = (m, met, preds)

        print(f"LSTM Regressor — {entity} — {horizon}")
        m, met, preds = train_lstm(train_df, test_df, id_col, feat_cols, reg_target, task="reg")
        lstm_results[(entity, horizon, "reg")] = (m, met, preds)

print("✓ LSTM training complete\n")

# ============================================================================
# RESULTS SUMMARY
# ============================================================================

print("="*70)
print("MODEL COMPARISON")
print("="*70 + "\n")

rows = []
for key_set, label in [(xgb_results, "XGBoost"), (lstm_results, "LSTM")]:
    for (entity, horizon, mtype), (_, metrics, _) in key_set.items():
        if not metrics:
            continue
        row = {"Model": label, "Entity": entity, "Horizon": horizon, "Type": mtype}
        row.update(metrics)
        rows.append(row)

metrics_df = pd.DataFrame(rows)
print(metrics_df.round(4).to_string())
print("\n✓ COMPLETE!\n")
