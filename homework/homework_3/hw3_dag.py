# put this file into airflow
from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

import boto3
import faiss  # type: ignore
import numpy as np
import pandas as pd
import pendulum
import torch
import torch.nn.functional as F
from airflow import DAG
from airflow.operators.python import PythonOperator

"""
HW3 DAG: MovieLens 1M BERT-based recommendations on MWAA.

This DAG assumes that the following one-time/offline artifacts already exist
in the MWAA S3 bucket:
- ml-1m/movies.dat
- ml-1m/users.dat
- ml-1m/ratings_956620800-965347200   (partition I)
- ml-1m/ratings_965347200-973036800   (partition II)
- ml-1m/ratings_973036800-975196800   (partition III)
- ml-1m/ratings_975196800-1798761600  (partition IV)
- ml-1m/item_emb_full.index           (Faiss index of movie BERT embeddings)

The DAG reuses those movie embeddings and, at each of four iterations,
combines observation partitions up to that point, samples 30% of users,
and writes recommendations for a cold user and a top user to S3 under
the `recommendations/` prefix without overwriting previous iterations.
"""

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Bucket that backs your MWAA environment (already created per instructions)
OUTPUT_BUCKET = os.getenv("HW3_MWAA_BUCKET", "stall-munezero-mwaa")

# Local working directory inside the MWAA worker
MOVIELENS_DIR = Path("/tmp/ml-1m")
MOVIELENS_DIR.mkdir(parents=True, exist_ok=True)

# S3 keys for artifacts prepared offline
MOVIES_KEY = "ml-1m/movies.dat"
USERS_KEY = "ml-1m/users.dat"
RATINGS_PART_KEYS: List[str] = [
    "ml-1m/ratings_956620800-965347200",
    "ml-1m/ratings_965347200-973036800",
    "ml-1m/ratings_973036800-975196800",
    "ml-1m/ratings_975196800-1798761600",
]
FAISS_INDEX_KEY = "ml-1m/item_emb_full.index"

# Four logical iterations at 0h, 10h, 20h, 30h.
N_ITERATIONS = 4


def _get_s3_client():
    """Use instance/IAM credentials on MWAA."""
    session = boto3.Session()
    return session.client("s3")


def _download_from_s3(s3_client, bucket: str, key: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3_client.download_file(bucket, key, str(local_path))
    return local_path


def _load_movies_and_users(s3_client) -> tuple[pd.DataFrame, pd.DataFrame]:
    movies_path = _download_from_s3(
        s3_client, OUTPUT_BUCKET, MOVIES_KEY, MOVIELENS_DIR / "movies.dat"
    )
    users_path = _download_from_s3(
        s3_client, OUTPUT_BUCKET, USERS_KEY, MOVIELENS_DIR / "users.dat"
    )

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

    # Add text field used by BERT encoder
    movies["text"] = movies["title"].fillna("") + " [SEP] " + movies["genres"].fillna("")
    return movies, users


def _load_ratings_partitions(s3_client, up_to_iter: int) -> pd.DataFrame:
    """Load and combine rating partitions up to and including `up_to_iter`."""
    dfs: List[pd.DataFrame] = []
    for idx in range(up_to_iter + 1):
        key = RATINGS_PART_KEYS[idx]
        local_csv = MOVIELENS_DIR / f"ratings_part_{idx}.csv"
        _download_from_s3(s3_client, OUTPUT_BUCKET, key, local_csv)
        df = pd.read_csv(
            local_csv,
            index_col=0,
            names=["user_id", "movie_id", "rating", "timestamp"],
            header=0,
        )
        dfs.append(df)
    ratings = pd.concat(dfs, axis=0, ignore_index=True)
    return ratings


def _load_faiss_index(s3_client) -> faiss.IndexFlatIP:
    index_path = _download_from_s3(
        s3_client, OUTPUT_BUCKET, FAISS_INDEX_KEY, MOVIELENS_DIR / "item_emb_full.index"
    )
    index = faiss.read_index(str(index_path))
    return index


def _get_bert_pretrained():
    device = "cpu"
    model_name = "distilbert-base-uncased"
    from transformers import AutoModel, AutoTokenizer  # lazy import

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    encoder = AutoModel.from_pretrained(model_name).to(device)
    encoder.eval()
    return tokenizer, encoder, device


def _encode_single_text(tokenizer, encoder, device: str, text: str, max_len: int = 128) -> np.ndarray:
    if not text:
        return None
    with torch.no_grad():
        batch = tokenizer(
            [text],
            padding=True,
            truncation=True,
            max_length=max_len,
            return_tensors="pt",
        ).to(device)
        out = encoder(**batch).last_hidden_state[:, 0, :]
        emb = F.normalize(out, p=2, dim=1).cpu().numpy().astype("float32")
        return emb


def _build_user_history(
    user_id: int,
    events_df: pd.DataFrame,
    movies_df: pd.DataFrame,
    n: int = 10,
):
    user_events = events_df[events_df["user_id"] == user_id].sort_values("ts")
    last_ts = user_events["ts"].max() if not user_events.empty else None
    seen = set(user_events["movie_id"].tolist())
    texts: List[str] = []
    if not user_events.empty:
        recent = user_events.tail(n)["movie_id"].tolist()
        texts = (
            movies_df.set_index("movie_id")
            .loc[recent, "text"]
            .fillna("")
            .tolist()
        )
    return texts, seen, last_ts


def _recommend_for_user(
    user_id: int,
    movies_df: pd.DataFrame,
    events_df: pd.DataFrame,
    ratings_df: pd.DataFrame,
    idx: faiss.IndexFlatIP,
    tokenizer,
    encoder,
    device: str,
    k: int = 5,
) -> Dict:
    """Shared recommender: uses history when present, otherwise popularity fallback."""
    texts, seen, last_ts = _build_user_history(user_id, events_df, movies_df, n=10)

    if texts:
        user_text = " ".join(texts)
        u = _encode_single_text(tokenizer, encoder, device, user_text)
        scores, idxs = idx.search(u, k + len(seen) + 20)
        recs: List[int] = []
        for j in idxs[0]:
            mid = int(movies_df.iloc[j]["movie_id"])
            if mid in seen:
                continue
            recs.append(mid)
            if len(recs) == k:
                break
    else:
        # Popularity-based fallback on currently observed ratings
        top_popular = (
            ratings_df[ratings_df["rating"] >= 4]
            .groupby("movie_id")["rating"]
            .size()
            .sort_values(ascending=False)
            .head(k)
            .index.tolist()
        )
        recs = top_popular

    return {"user_id": user_id, "last_ts": last_ts, "recs": recs, "seen": list(seen)}


def _sample_users(ratings: pd.DataFrame, sample_pct: float = 30.0) -> pd.DataFrame:
    user_ids = ratings["user_id"].unique()
    users_df = pd.DataFrame({"user_id": user_ids})
    if not 0 < sample_pct <= 100:
        raise ValueError(f"sample_pct must be in (0, 100], got {sample_pct}")
    frac = sample_pct / 100.0
    return users_df.sample(frac=frac, random_state=42)


def _build_positive_events(
    ratings: pd.DataFrame, sampled_users: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ratings = ratings.copy()
    ratings["timestamp"] = pd.to_datetime(ratings["timestamp"], unit="s")
    user_subset = set(sampled_users["user_id"].tolist())
    sample_ratings = ratings[ratings["user_id"].isin(user_subset)]
    pos = sample_ratings[sample_ratings["rating"] >= 4].copy()
    pos["value"] = 1.0
    pos = pos.sort_values(["user_id", "timestamp"])
    events = pos[["user_id", "movie_id", "timestamp"]].rename(columns={"timestamp": "ts"})
    return events, sample_ratings


def _pick_cold_and_top_users(
    ratings: pd.DataFrame,
    all_users_df: pd.DataFrame,
    sampled_users: pd.DataFrame,
) -> tuple[int, int]:
    observed_users = set(ratings["user_id"].unique())
    sampled_user_ids = set(sampled_users["user_id"].tolist())

    # Cold user: a user from users.dat with no interactions so far
    all_user_ids = set(all_users_df["user_id"])
    cold_candidates = list(all_user_ids - observed_users)
    if cold_candidates:
        cold_user = sorted(cold_candidates)[0]
    else:
        # If every user has interacted, pick someone outside the 30% sample
        remaining = list(all_user_ids - sampled_user_ids)
        cold_user = sorted(remaining)[0] if remaining else int(sorted(all_user_ids)[0])

    # Top user: top 5% of users by interaction count in currently observed ratings
    user_counts = ratings.groupby("user_id").size().reset_index(name="interaction_count")
    threshold = user_counts["interaction_count"].quantile(0.95)
    top_users = user_counts[user_counts["interaction_count"] >= threshold]
    top_user = int(
        top_users.sample(1, random_state=42)["user_id"].iloc[0]
    )
    return cold_user, top_user


def _summarize_user_row(
    user_id: int,
    user_type: str,
    rec_info: Dict,
    users_df: pd.DataFrame,
) -> Dict:
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


def run_recommendation_iteration(iter_idx: int, **context) -> None:
    """
    Core callable for each of the four iterations (0h, 10h, 20h, 30h).
    """
    if iter_idx < 0 or iter_idx >= N_ITERATIONS:
        raise ValueError(f"iter_idx must be between 0 and {N_ITERATIONS - 1}, got {iter_idx}")

    s3_client = _get_s3_client()
    movies, users = _load_movies_and_users(s3_client)
    ratings = _load_ratings_partitions(s3_client, up_to_iter=iter_idx)
    index = _load_faiss_index(s3_client)
    tokenizer, encoder, device = _get_bert_pretrained()

    sampled_users = _sample_users(ratings, sample_pct=30.0)
    events, sample_ratings = _build_positive_events(ratings, sampled_users)
    cold_user, top_user = _pick_cold_and_top_users(ratings, users, sampled_users)

    cold_rec = _recommend_for_user(
        cold_user, movies, events, ratings, index, tokenizer, encoder, device, k=5
    )
    top_rec = _recommend_for_user(
        top_user, movies, events, ratings, index, tokenizer, encoder, device, k=5
    )

    cold_summary = _summarize_user_row(cold_user, "cold", cold_rec, users)
    top_summary = _summarize_user_row(top_user, "top", top_rec, users)
    df = pd.DataFrame([cold_summary, top_summary])

    # Use iteration index and logical execution date in filename to avoid overwrites
    logical_date = context["ds_nodash"]
    local_out = MOVIELENS_DIR / f"recs_iter{iter_idx}_{logical_date}.csv"
    df.to_csv(local_out, index=False)

    s3_key = f"recommendations/recs_iter{iter_idx}_{logical_date}.csv"
    s3_client.upload_file(str(local_out), OUTPUT_BUCKET, s3_key)

    # Also log basic info for debugging
    print(json.dumps({"s3_bucket": OUTPUT_BUCKET, "s3_key": s3_key, "rows": len(df)}))


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

start = pendulum.now("UTC")
end = start.add(hours=48)

default_args = {
    "owner": "de300",
    "depends_on_past": False,
    "start_date": start,
    "retries": 1,
}

with DAG(
    dag_id="hw3_movielens_mwaa_recommendations",
    default_args=default_args,
    schedule_interval="0 */10 * * *",  # every 10 hours
    end_date=end,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=48),
) as dag:
    iter0 = PythonOperator(
        task_id="iteration_0h",
        python_callable=run_recommendation_iteration,
        op_kwargs={"iter_idx": 0},
        provide_context=True,
    )

    iter1 = PythonOperator(
        task_id="iteration_10h",
        python_callable=run_recommendation_iteration,
        op_kwargs={"iter_idx": 1},
        provide_context=True,
    )

    iter2 = PythonOperator(
        task_id="iteration_20h",
        python_callable=run_recommendation_iteration,
        op_kwargs={"iter_idx": 2},
        provide_context=True,
    )

    iter3 = PythonOperator(
        task_id="iteration_30h",
        python_callable=run_recommendation_iteration,
        op_kwargs={"iter_idx": 3},
        provide_context=True,
    )

    iter0 >> iter1 >> iter2 >> iter3

