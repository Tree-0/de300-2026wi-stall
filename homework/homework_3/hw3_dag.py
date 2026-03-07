# initial attempt on airflow 2.10.3
# NOT THE FINAL ATTEMPT

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

"""
HW3 DAG: MovieLens 1M BERT-based recommendations on MWAA.

Our DAG assumes that the following one-time/offline artifacts already exist
in the MWAA S3 bucket under the hw3/ prefix (e.g. hw3/ml-1m/...):
- hw3/ml-1m/movies.dat
- hw3/ml-1m/users.dat
- hw3/ml-1m/ratings_956620800-965347200   (partition I)
- hw3/ml-1m/ratings_965347200-973036800   (partition II)
- hw3/ml-1m/ratings_973036800-975196800   (partition III)
- hw3/ml-1m/ratings_975196800-1798761600  (partition IV)
- hw3/ml-1m/item_emb_full.index           (Faiss index of movie BERT embeddings)

The DAG reuses those movie embeddings and, at each of four iterations,
combines observation partitions up to that point, samples 30% of users,
and writes recommendations for a cold user and a top user to S3 under
hw3/recommendations/ without overwriting previous iterations.
"""

# Configuration

# Bucket that backs MWAA environment (already created per instructions)
OUTPUT_BUCKET = os.getenv("HW3_MWAA_BUCKET", "stall-munezero-mwaa")

# All HW3 data lives under this prefix in the bucket (hw3/ with ml-1m/ and recommendations/ inside)
HW3_S3_PREFIX = "hw3"

# Local working directory inside the MWAA worker
MOVIELENS_DIR = Path("/tmp/ml-1m")
MOVIELENS_DIR.mkdir(parents=True, exist_ok=True)

# S3 keys for artifacts prepared offline (under hw3/)
MOVIES_KEY = f"{HW3_S3_PREFIX}/ml-1m/movies.dat"
USERS_KEY = f"{HW3_S3_PREFIX}/ml-1m/users.dat"
RATINGS_PART_KEYS: List[str] = [
    f"{HW3_S3_PREFIX}/ml-1m/ratings_956620800-965347200",
    f"{HW3_S3_PREFIX}/ml-1m/ratings_965347200-973036800",
    f"{HW3_S3_PREFIX}/ml-1m/ratings_973036800-975196800",
    f"{HW3_S3_PREFIX}/ml-1m/ratings_975196800-1798761600",
]
FAISS_INDEX_KEY = f"{HW3_S3_PREFIX}/ml-1m/item_emb_full.index"

# Four logical iterations; spacing controlled by ITERATION_INTERVAL_MINUTES.
N_ITERATIONS = 4

# For testing, run an iteration every 10 minutes. You can later change this to
# 60 (hourly), 600 (every 10 hours), etc., as long as you update the schedule.
ITERATION_INTERVAL_MINUTES = 10


def _get_s3_client():
    """Use instance/IAM credentials on MWAA."""
    import boto3

    session = boto3.Session()
    return session.client("s3")


def _determine_next_iter_idx(s3_client) -> int:
    """
    Inspect existing recommendation files in S3 to determine the next
    iteration index to run. This makes iteration assignment robust to
    manual runs and worker timing.

    Returns an integer in [0, N_ITERATIONS-1], or raises AirflowSkipException
    if all iterations have already been produced.
    """
    from botocore.exceptions import ClientError

    prefix = f"{HW3_S3_PREFIX}/recommendations/recs_iter"
    try:
        resp = s3_client.list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=prefix)
    except ClientError as exc:
        # If listing fails for some reason, fall back to starting at 0
        print(f"Warning: failed to list S3 objects for prefix {prefix}: {exc}")
        return 0

    existing_iters = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        # Expect keys like hw3/recommendations/recs_iter{idx}_{ds}.csv
        if "recs_iter" in key:
            try:
                after = key.split("recs_iter", 1)[1]
                idx_str = after.split("_", 1)[0]
                existing_iters.append(int(idx_str))
            except (IndexError, ValueError):
                continue

    if not existing_iters:
        next_idx = 0
    else:
        next_idx = max(existing_iters) + 1

    if next_idx >= N_ITERATIONS:
        msg = f"All {N_ITERATIONS} iterations already completed; nothing left to run."
        print(msg)
        from airflow.exceptions import AirflowSkipException as _Skip

        raise _Skip(msg)

    return next_idx


def _download_from_s3(s3_client, bucket: str, key: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3_client.download_file(bucket, key, str(local_path))
    return local_path


def _load_movies_and_users(s3_client) -> tuple["pd.DataFrame", "pd.DataFrame"]:
    import pandas as pd

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


def _load_ratings_partitions(s3_client, up_to_iter: int) -> "pd.DataFrame":
    """Load and combine rating partitions up to and including `up_to_iter`."""
    import pandas as pd

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


def _load_faiss_index(s3_client) -> "faiss.IndexFlatIP":
    import faiss

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
    import numpy as np
    import torch
    import torch.nn.functional as F

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
    import pandas as pd

    user_ids = ratings["user_id"].unique()
    users_df = pd.DataFrame({"user_id": user_ids})
    if not 0 < sample_pct <= 100:
        raise ValueError(f"sample_pct must be in (0, 100], got {sample_pct}")
    frac = sample_pct / 100.0
    return users_df.sample(frac=frac, random_state=42)


def _build_positive_events(
    ratings: pd.DataFrame, sampled_users: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    import pandas as pd

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


def combine_observations(**context) -> Dict:
    """
    Task 1: determine iteration index and combine rating partitions
    up to and including that iteration.

    Returns XCom payload with iter_idx and local file paths to the
    combined ratings, movies, and users data.
    """
    s3_client = _get_s3_client()
    iter_idx = _determine_next_iter_idx(s3_client)

    logical_ds_nodash = context["ds_nodash"]
    movies, users = _load_movies_and_users(s3_client)
    ratings = _load_ratings_partitions(s3_client, up_to_iter=iter_idx)

    movies_path = MOVIELENS_DIR / f"movies_iter{iter_idx}.csv"
    users_path = MOVIELENS_DIR / f"users_iter{iter_idx}.csv"
    ratings_path = MOVIELENS_DIR / f"ratings_combined_iter{iter_idx}.csv"

    movies.to_csv(movies_path, index=False)
    users.to_csv(users_path, index=False)
    ratings.to_csv(ratings_path, index=False)

    return {
        "iter_idx": iter_idx,
        "logical_ds_nodash": logical_ds_nodash,
        "movies_path": str(movies_path),
        "users_path": str(users_path),
        "ratings_path": str(ratings_path),
    }


def generate_recommendations(**context) -> Dict:
    """
    Task 2: using the cumulative observations, generate recommendations
    for a cold user and a top user, and save them locally.
    """
    import pandas as pd

    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="combine_observations")
    if not payload:
        msg = "No combined observations found in XCom from combine_observations."
        print(msg)
        raise AirflowSkipException(msg)

    iter_idx = int(payload["iter_idx"])
    logical_ds_nodash = payload["logical_ds_nodash"]

    movies = pd.read_csv(payload["movies_path"])
    users = pd.read_csv(payload["users_path"])
    ratings = pd.read_csv(payload["ratings_path"])

    s3_client = _get_s3_client()
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

    local_out = MOVIELENS_DIR / f"recs_iter{iter_idx}_{logical_ds_nodash}.csv"
    df.to_csv(local_out, index=False)

    return {
        "iter_idx": iter_idx,
        "logical_ds_nodash": logical_ds_nodash,
        "recs_path": str(local_out),
        "rows": len(df),
    }


def upload_recommendations(**context) -> None:
    """
    Task 3: upload the locally saved recommendations file to S3 under
    hw3/recommendations/ with a non-overwriting naming scheme.
    """
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="generate_recommendations")
    if not payload:
        msg = "No recommendations found in XCom from generate_recommendations."
        print(msg)
        raise AirflowSkipException(msg)

    iter_idx = int(payload["iter_idx"])
    logical_ds_nodash = payload["logical_ds_nodash"]
    recs_path = Path(payload["recs_path"])

    if not recs_path.exists():
        raise FileNotFoundError(f"Recommendations file not found at {recs_path}")

    s3_client = _get_s3_client()
    s3_key = f"{HW3_S3_PREFIX}/recommendations/recs_iter{iter_idx}_{logical_ds_nodash}.csv"
    s3_client.upload_file(str(recs_path), OUTPUT_BUCKET, s3_key)

    print(json.dumps({"s3_bucket": OUTPUT_BUCKET, "s3_key": s3_key, "rows": payload.get("rows")}))


# DAG Definition

DAG_START = pendulum.now("UTC")
DAG_END = DAG_START.add(hours=48)

default_args = {
    "owner": "de300",
    "depends_on_past": False,
    "start_date": DAG_START,
    "retries": 1,
}

with DAG(
    dag_id="hw3_stall_munezero_recs_dag",
    default_args=default_args,
    schedule_interval="*/10 * * * *",  # every 10 minutes (for testing)
    end_date=DAG_END,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=48),
) as dag:
    combine_task = PythonOperator(
        task_id="combine_observations",
        python_callable=combine_observations,
        provide_context=True,
    )

    recommend_task = PythonOperator(
        task_id="generate_recommendations",
        python_callable=generate_recommendations,
        provide_context=True,
    )

    upload_task = PythonOperator(
        task_id="upload_recommendations",
        python_callable=upload_recommendations,
        provide_context=True,
    )

    combine_task >> recommend_task >> upload_task

