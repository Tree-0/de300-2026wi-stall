# put this file into airflow
from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import numpy as np
import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

"""
HW3 DAG: MovieLens 1M BERT-based recommendations on MWAA (NO FAISS).

Assumes these artifacts exist in S3 under hw3/ml-1m/:
- movies.dat
- users.dat
- ratings_956620800-965347200
- ratings_965347200-973036800
- ratings_973036800-975196800
- ratings_975196800-1798761600
- item_emb_full.npy               (float32, shape [N, D], L2-normalized rows)
OPTIONAL (recommended):
- item_emb_movie_ids.npy          (int array of movie_ids aligned with item_emb_full.npy rows)

Writes outputs to:
- hw3/recommendations/recs_iter{iter}_{ds}.csv
"""

# ----------------------------
# Configuration
# ----------------------------

OUTPUT_BUCKET = os.getenv("HW3_MWAA_BUCKET", "stall-munezero-mwaa")
HW3_S3_PREFIX = "hw3"

MOVIELENS_DIR = Path("/tmp/ml-1m")
MOVIELENS_DIR.mkdir(parents=True, exist_ok=True)

MOVIES_KEY = f"{HW3_S3_PREFIX}/ml-1m/movies.dat"
USERS_KEY = f"{HW3_S3_PREFIX}/ml-1m/users.dat"
RATINGS_PART_KEYS: List[str] = [
    f"{HW3_S3_PREFIX}/ml-1m/ratings_956620800-965347200",
    f"{HW3_S3_PREFIX}/ml-1m/ratings_965347200-973036800",
    f"{HW3_S3_PREFIX}/ml-1m/ratings_973036800-975196800",
    f"{HW3_S3_PREFIX}/ml-1m/ratings_975196800-1798761600",
]

# NEW: NumPy embedding artifacts (no FAISS)
ITEM_EMB_NPY_KEY = f"{HW3_S3_PREFIX}/ml-1m/item_emb_full.npy"
ITEM_EMB_MOVIE_IDS_KEY = f"{HW3_S3_PREFIX}/ml-1m/item_emb_movie_ids.npy"  # optional

N_ITERATIONS = 4
ITERATION_INTERVAL_MINUTES = 10


# ----------------------------
# Helpers
# ----------------------------

def _get_s3_client():
    """Use instance/IAM credentials on MWAA."""
    return boto3.Session().client("s3")


def _download_from_s3(s3_client, bucket: str, key: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3_client.download_file(bucket, key, str(local_path))
    return local_path


def _try_download_from_s3(s3_client, bucket: str, key: str, local_path: Path) -> Optional[Path]:
    """Return None if object missing."""
    try:
        return _download_from_s3(s3_client, bucket, key, local_path)
    except Exception as e:
        # MWAA S3 client errors vary; keep it simple
        print(f"Optional download failed for s3://{bucket}/{key}: {e}")
        return None


def _load_movies_and_users(s3_client) -> Tuple[pd.DataFrame, pd.DataFrame]:
    movies_path = _download_from_s3(s3_client, OUTPUT_BUCKET, MOVIES_KEY, MOVIELENS_DIR / "movies.dat")
    users_path = _download_from_s3(s3_client, OUTPUT_BUCKET, USERS_KEY, MOVIELENS_DIR / "users.dat")

    movies = pd.read_csv(
        movies_path,
        sep="::",
        engine="python",
        names=["movie_id", "title", "genres"],
        encoding="latin-1",
    )
    users = pd.read_csv(
        users_path,
        sep="::",
        engine="python",
        names=["user_id", "gender", "age", "occupation", "zip"],
    )

    movies["text"] = movies["title"].fillna("") + " [SEP] " + movies["genres"].fillna("")
    return movies, users


def _load_ratings_partitions(s3_client, up_to_iter: int) -> pd.DataFrame:
    """
    Load and combine rating partitions up to and including `up_to_iter`.

    IMPORTANT: these files are assumed to be MovieLens-style '::' separated
    with 4 fields: user_id::movie_id::rating::timestamp
    """
    dfs: List[pd.DataFrame] = []
    for idx in range(up_to_iter + 1):
        key = RATINGS_PART_KEYS[idx]
        local_path = MOVIELENS_DIR / f"ratings_part_{idx}.dat"
        _download_from_s3(s3_client, OUTPUT_BUCKET, key, local_path)

        df = pd.read_csv(
            local_path,
            sep="::",
            engine="python",
            names=["user_id", "movie_id", "rating", "timestamp"],
        )
        dfs.append(df)

    ratings = pd.concat(dfs, axis=0, ignore_index=True)
    return ratings


def _load_item_embeddings(s3_client, movies_df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """
    Returns:
      item_mat: float32 array [N, D], L2-normalized rows
      item_movie_ids: int array [N], movie_id aligned with row index of item_mat

    If item_emb_movie_ids.npy is missing, we assume embeddings are in the same
    row order as movies_df (as loaded from movies.dat).
    """
    emb_path = _download_from_s3(s3_client, OUTPUT_BUCKET, ITEM_EMB_NPY_KEY, MOVIELENS_DIR / "item_emb_full.npy")
    item_mat = np.load(emb_path).astype("float32")

    ids_path = _try_download_from_s3(
        s3_client,
        OUTPUT_BUCKET,
        ITEM_EMB_MOVIE_IDS_KEY,
        MOVIELENS_DIR / "item_emb_movie_ids.npy",
    )
    if ids_path is not None:
        item_movie_ids = np.load(ids_path).astype("int64")
    else:
        item_movie_ids = movies_df["movie_id"].to_numpy(dtype="int64")

    if item_mat.shape[0] != item_movie_ids.shape[0]:
        raise ValueError(
            f"Embedding rows ({item_mat.shape[0]}) != item_movie_ids ({item_movie_ids.shape[0]}). "
            "Fix by uploading aligned item_emb_movie_ids.npy."
        )

    # Ensure normalized (safe even if already normalized)
    norms = np.linalg.norm(item_mat, axis=1, keepdims=True) + 1e-12
    item_mat = item_mat / norms

    return item_mat, item_movie_ids


def _get_bert_pretrained():
    """Lazy import so DAG parsing doesn't require transformers/torch."""
    device = "cpu"
    model_name = "distilbert-base-uncased"

    from transformers import AutoModel, AutoTokenizer  # lazy import
    import torch  # lazy import

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    encoder = AutoModel.from_pretrained(model_name).to(device)
    encoder.eval()

    # optional: reduce CPU overhead a bit
    torch.set_num_threads(int(os.getenv("TORCH_NUM_THREADS", "2")))

    return tokenizer, encoder, device


def _encode_single_text(tokenizer, encoder, device: str, text: str, max_len: int = 128) -> np.ndarray:
    """
    Returns embedding shape (1, D) float32 normalized.
    If text empty, returns zeros (so caller can still run).
    """
    if not text:
        # DistilBERT hidden size is 768; keep it explicit.
        return np.zeros((1, 768), dtype="float32")

    import torch  # lazy import
    import torch.nn.functional as F  # lazy import

    with torch.no_grad():
        batch = tokenizer(
            [text],
            padding=True,
            truncation=True,
            max_length=max_len,
            return_tensors="pt",
        ).to(device)
        out = encoder(**batch).last_hidden_state[:, 0, :]  # CLS token
        emb = F.normalize(out, p=2, dim=1).cpu().numpy().astype("float32")
        return emb


def _build_user_history(
    user_id: int,
    events_df: pd.DataFrame,
    movies_df: pd.DataFrame,
    n: int = 10,
) -> Tuple[List[str], set, Optional[pd.Timestamp]]:
    user_events = events_df[events_df["user_id"] == user_id].sort_values("ts")
    last_ts = user_events["ts"].max() if not user_events.empty else None
    seen = set(user_events["movie_id"].tolist())

    texts: List[str] = []
    if not user_events.empty:
        recent = user_events.tail(n)["movie_id"].tolist()

        # robust selection: some movie_ids may be missing
        m = movies_df.set_index("movie_id")
        available = [mid for mid in recent if mid in m.index]
        if available:
            texts = m.loc[available, "text"].fillna("").tolist()

    return texts, seen, last_ts


def _topk_from_matrix(item_mat: np.ndarray, u: np.ndarray, k: int) -> np.ndarray:
    """
    item_mat: [N, D] normalized
    u: [1, D] normalized
    Returns indices into item_mat (not movie_ids) sorted by score descending.
    """
    scores = item_mat @ u[0]  # (N,)
    if k >= scores.shape[0]:
        return np.argsort(scores)[::-1]

    idxs = np.argpartition(scores, -k)[-k:]
    idxs = idxs[np.argsort(scores[idxs])[::-1]]
    return idxs


def _recommend_for_user(
    user_id: int,
    movies_df: pd.DataFrame,
    events_df: pd.DataFrame,
    ratings_df: pd.DataFrame,
    item_mat: np.ndarray,
    item_movie_ids: np.ndarray,
    tokenizer,
    encoder,
    device: str,
    k: int = 5,
) -> Dict:
    """
    Shared recommender: uses history when present, otherwise popularity fallback.
    Uses matrix similarity (no FAISS).
    """
    texts, seen, last_ts = _build_user_history(user_id, events_df, movies_df, n=10)

    if texts:
        user_text = " ".join(texts)
        u = _encode_single_text(tokenizer, encoder, device, user_text)

        # search more than k to allow filtering out "seen"
        candidate_k = min(item_mat.shape[0], k + len(seen) + 50)
        idxs = _topk_from_matrix(item_mat, u, candidate_k)

        recs: List[int] = []
        for j in idxs:
            mid = int(item_movie_ids[int(j)])
            if mid in seen:
                continue
            recs.append(mid)
            if len(recs) == k:
                break
    else:
        top_popular = (
            ratings_df[ratings_df["rating"] >= 4]
            .groupby("movie_id")["rating"]
            .size()
            .sort_values(ascending=False)
            .head(k)
            .index.tolist()
        )
        recs = [int(x) for x in top_popular]

    return {"user_id": user_id, "last_ts": last_ts, "recs": recs, "seen": list(seen)}


def _sample_users(ratings: pd.DataFrame, sample_pct: float = 30.0) -> pd.DataFrame:
    user_ids = ratings["user_id"].unique()
    users_df = pd.DataFrame({"user_id": user_ids})

    if not 0 < sample_pct <= 100:
        raise ValueError(f"sample_pct must be in (0, 100], got {sample_pct}")

    return users_df.sample(frac=(sample_pct / 100.0), random_state=42)


def _build_positive_events(ratings: pd.DataFrame, sampled_users: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    ratings = ratings.copy()
    ratings["timestamp"] = pd.to_datetime(ratings["timestamp"], unit="s")

    user_subset = set(sampled_users["user_id"].tolist())
    sample_ratings = ratings[ratings["user_id"].isin(user_subset)]

    pos = sample_ratings[sample_ratings["rating"] >= 4].copy()
    pos = pos.sort_values(["user_id", "timestamp"])
    events = pos[["user_id", "movie_id", "timestamp"]].rename(columns={"timestamp": "ts"})
    return events, sample_ratings


def _pick_cold_and_top_users(
    ratings: pd.DataFrame,
    all_users_df: pd.DataFrame,
    sampled_users: pd.DataFrame,
) -> Tuple[int, int]:
    observed_users = set(ratings["user_id"].unique())
    sampled_user_ids = set(sampled_users["user_id"].tolist())

    all_user_ids = set(all_users_df["user_id"])
    cold_candidates = list(all_user_ids - observed_users)
    if cold_candidates:
        cold_user = int(sorted(cold_candidates)[0])
    else:
        remaining = list(all_user_ids - sampled_user_ids)
        cold_user = int(sorted(remaining)[0]) if remaining else int(sorted(all_user_ids)[0])

    user_counts = ratings.groupby("user_id").size().reset_index(name="interaction_count")
    threshold = user_counts["interaction_count"].quantile(0.95)
    top_users = user_counts[user_counts["interaction_count"] >= threshold]
    top_user = int(top_users.sample(1, random_state=42)["user_id"].iloc[0])

    return cold_user, top_user


def _summarize_user_row(user_id: int, user_type: str, rec_info: Dict, users_df: pd.DataFrame) -> Dict:
    row = users_df[users_df["user_id"] == user_id]
    summary: Dict = {
        "User_Type": user_type,
        "User_ID": user_id,
        "Last_Interaction_Time": rec_info["last_ts"],
        "Interactions": len(rec_info["seen"]),
        "Recs": rec_info["recs"],
    }
    if not row.empty:
        summary.update(
            {
                "Gender": row.iloc[0]["gender"],
                "Age": row.iloc[0]["age"],
                "Occupation": row.iloc[0]["occupation"],
                "Zip": row.iloc[0]["zip"],
            }
        )
    return summary


# ----------------------------
# Task callable
# ----------------------------

def run_recommendation_iteration(**context) -> None:
    logical_date = context.get("logical_date") or context.get("execution_date")
    if logical_date is None:
        raise ValueError("logical_date/execution_date missing from context")

    delta_minutes = (logical_date - DAG_START).total_seconds() / 60.0
    iter_idx = int(delta_minutes // ITERATION_INTERVAL_MINUTES)

    if iter_idx < 0 or iter_idx >= N_ITERATIONS:
        print(f"Skipping run at {logical_date}: iter_idx={iter_idx} outside [0, {N_ITERATIONS - 1}]")
        return

    s3_client = _get_s3_client()

    movies, users = _load_movies_and_users(s3_client)
    ratings = _load_ratings_partitions(s3_client, up_to_iter=iter_idx)

    # Load embeddings (no FAISS)
    item_mat, item_movie_ids = _load_item_embeddings(s3_client, movies)

    # BERT encoder
    tokenizer, encoder, device = _get_bert_pretrained()

    sampled_users = _sample_users(ratings, sample_pct=30.0)
    events, _sample_ratings = _build_positive_events(ratings, sampled_users)

    cold_user, top_user = _pick_cold_and_top_users(ratings, users, sampled_users)

    cold_rec = _recommend_for_user(
        cold_user, movies, events, ratings, item_mat, item_movie_ids, tokenizer, encoder, device, k=5
    )
    top_rec = _recommend_for_user(
        top_user, movies, events, ratings, item_mat, item_movie_ids, tokenizer, encoder, device, k=5
    )

    cold_summary = _summarize_user_row(cold_user, "cold", cold_rec, users)
    top_summary = _summarize_user_row(top_user, "top", top_rec, users)
    df = pd.DataFrame([cold_summary, top_summary])

    ds_nodash = context["ds_nodash"]
    local_out = MOVIELENS_DIR / f"recs_iter{iter_idx}_{ds_nodash}.csv"
    df.to_csv(local_out, index=False)

    s3_key = f"{HW3_S3_PREFIX}/recommendations/recs_iter{iter_idx}_{ds_nodash}.csv"
    s3_client.upload_file(str(local_out), OUTPUT_BUCKET, s3_key)

    print(json.dumps({"s3_bucket": OUTPUT_BUCKET, "s3_key": s3_key, "rows": len(df)}))


# ----------------------------
# DAG Definition
# ----------------------------

DAG_START = pendulum.now("UTC")
DAG_END = DAG_START.add(hours=48)

default_args = {
    "owner": "de300",
    "depends_on_past": False,
    "start_date": DAG_START,
    "retries": 1,
}

with DAG(
    dag_id="hw3_stall_munezero_recs",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    end_date=DAG_END,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=48),
) as dag:
    run_iteration = PythonOperator(
        task_id="run_recommendation_iteration",
        python_callable=run_recommendation_iteration,
        provide_context=True,
    )