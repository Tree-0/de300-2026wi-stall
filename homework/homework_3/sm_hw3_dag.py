# FINAL ATTEMPT AT DAG FOR AIRFLOW 3.X

"""
Airflow DAG for DE300 HW3 - Movie Recommendations

Generates movie recommendations for cold and top users by:
1. Loading data partitions and embeddings from S3
2. Computing recommendations using collaborative filtering
3. Saving results with unique naming to prevent overwrites
"""

# Expected S3 layout after DAG runs on MWAA:
# s3://stall-munezero-final-project/
#   hw3/ml-1m/
#     count.json                  # run counter incremented each execution
#     full_embedding.npy
#     movies.dat
#     rating_1.csv ... rating_4.csv
#     tmp/
#       merged_rating_{run_count}.csv
#     output/
#       recs_{run_count}_file.csv

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import pandas as pd
import numpy as np
import json
import boto3
import tempfile
from pathlib import Path


BUCKET_NAME = "stall-munezero-final-project"
BASE_PATH = "hw3/ml-1m"
OUTPUT_PATH = f"{BASE_PATH}/output"
TMP_PATH = f"{BASE_PATH}/tmp"
MAX_PARTITIONS = 4



def df_to_xcom(df: pd.DataFrame) -> str:
    """Convert DataFrame to JSON string for XCom passing."""
    return df.to_json(orient="records")

def xcom_to_df(payload: str) -> pd.DataFrame:
    """Convert JSON string from XCom back to DataFrame."""
    if not payload:
        return pd.DataFrame()
    return pd.DataFrame(json.loads(payload))

def load_movies(bucket, path):
    """Download and load movies metadata from S3."""
    s3 = boto3.client('s3')
    
    with tempfile.TemporaryDirectory() as td:
        movies_path = Path(td) / 'movies.dat'
        s3.download_file(bucket, path, str(movies_path))
        
        movies = pd.read_csv(
            movies_path,
            sep='::',
            engine='python',
            names=['movie_id', 'title', 'genres'],
            encoding='latin-1'
        )
        
        movie_ids = movies["movie_id"].astype(int)
        id_to_row = {int(mid): i for i, mid in enumerate(movie_ids.tolist())}
    
    return movies, movie_ids, id_to_row

def pick_top_user(df, seed=123):
    """Randomly select a user from the top 5% by interaction count."""
    if df.empty:
        return None
    
    user_counts = df["user_id"].value_counts()
    if len(user_counts) == 0:
        return None

    cutoff = np.quantile(user_counts.values, 0.95)
    top_users = user_counts[user_counts >= cutoff].index.to_numpy()

    if len(top_users) == 0:
        return None

    rng = np.random.default_rng(seed)
    return int(rng.choice(top_users))

def format_movie_list(movie_ids, items_df):
    """Format movie IDs as readable strings with titles and genres."""
    m = items_df.set_index('movie_id')[['title', 'genres']]
    parts = []
    for mid in movie_ids:
        mid = int(mid)
        if mid in m.index:
            title = m.loc[mid, 'title']
            genres = m.loc[mid, 'genres']
            parts.append(f'{mid}: {title} [{genres}]')
        else:
            parts.append(str(mid))
    return ' | '.join(parts)

def recommend(user_id, events, E, item_ids, id_to_row, item_col='movie_id',
              ts_col='ts', topk=5, N=10, min_rating=4):
    """Generate recommendations for an existing user using collaborative filtering."""
    uev = (
        events[events['user_id'] == int(user_id)]
        .sort_values(ts_col, ascending=False)
        .head(N)
    )
    uev = uev[uev['rating'] >= float(min_rating)]

    mids = [int(m) for m in uev[item_col].tolist() if int(m) in id_to_row]
    if not mids:
        return []

    rows = np.array([id_to_row[m] for m in mids], dtype=int)
    seen_rows = set(rows.tolist())

    u = E[rows].mean(axis=0)
    u = u / max(np.linalg.norm(u), 1e-12)

    sims = E @ u
    candidates = [i for i in np.argsort(-sims) if i not in seen_rows]
    top_rows = candidates[:topk]

    return [int(item_ids[i]) for i in top_rows]

def recommend_cold(events, items, k=5):
    """Generate recommendations for cold users based on popularity."""
    subset_ids = set(items['movie_id'].astype(int).tolist())
    pop = (events[events['movie_id'].isin(subset_ids)]
           ['movie_id'].value_counts().head(k).index.astype(int).tolist())
    return pop


def download(**context):
    """Download and merge data partitions from S3."""
    s3 = boto3.client("s3")
    run_count_key = f'{BASE_PATH}/count.json'

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)

        # Get run count
        run_count_path = td / "count.json"
        s3.download_file(BUCKET_NAME, run_count_key, str(run_count_path))
        run_count = int(run_count_path.read_text().strip())
        n_parts = min(run_count, MAX_PARTITIONS)

        # Download and merge partitions
        parts = []
        for i in range(1, n_parts + 1):
            local_path = td / f"rating_{i}.csv"
            s3.download_file(BUCKET_NAME, f'{BASE_PATH}/rating_{i}.csv', str(local_path))
            parts.append(pd.read_csv(local_path))

        partition_data = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

        # Save and upload merged partition
        merged_path = td / f"merged_rating_{run_count}.csv"
        partition_data.to_csv(merged_path, index=False)
        
        merged_key = f"{TMP_PATH}/merged_rating_{run_count}.csv"
        s3.upload_file(str(merged_path), BUCKET_NAME, merged_key)

    return merged_key

def check_run_count(**context):
    """Check if count exceeds max partitions."""
    s3 = boto3.client('s3')
    run_count_key = f'{BASE_PATH}/count.json'

    with tempfile.TemporaryDirectory() as td:
        local_path = Path(td) / 'count.json'
        s3.download_file(BUCKET_NAME, run_count_key, str(local_path))
        run_count = int(local_path.read_text().strip())

    return "stop_dag" if run_count > MAX_PARTITIONS else "continue_dag"

def run_recommend(**context):
    """Generate recommendations for cold and top users."""
    s3 = boto3.client('s3')
    
    # Load embeddings
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        emb_path = td / "full_embedding.npy"
        s3.download_file(BUCKET_NAME, f'{BASE_PATH}/full_embedding.npy', str(emb_path))

        E = np.load(emb_path)
        norms = np.linalg.norm(E, axis=1, keepdims=True)
        E = E / np.maximum(norms, 1e-12)

    # Load partition data
    merged_key = context['ti'].xcom_pull(task_ids="download")
    with tempfile.TemporaryDirectory() as td:
        local_path = Path(td) / "merged_rating.csv"
        s3.download_file(BUCKET_NAME, merged_key, str(local_path))
        ratings = pd.read_csv(local_path)
        ratings['ts'] = pd.to_datetime(ratings['timestamp'], unit='s')

    # Load movie metadata
    movies, movie_ids, id_to_row = load_movies(BUCKET_NAME, f'{BASE_PATH}/movies.dat')
    items = movies[['movie_id', 'title', 'genres']]

    # Select top user
    top_user = pick_top_user(ratings, seed=3)
    if top_user is None:
        print("No valid top user found")
        return df_to_xcom(pd.DataFrame())

    # Prepare user data
    user_counts = ratings.groupby('user_id').size().rename('Num_Interactions')
    top5_cutoff = int(np.quantile(user_counts.values, 0.95))
    top_user_ratings = ratings[ratings['user_id'] == top_user]
    last_interaction_time = top_user_ratings['ts'].max().strftime("%Y-%m-%d %H:%M:%S")

    # Generate recommendations
    top_ids = recommend(
        user_id=top_user,
        events=ratings,
        E=E,
        item_ids=movie_ids,
        id_to_row=id_to_row
    )

    cold_ids = recommend_cold(ratings, items, k=5)

    # Create recommendation records
    recs = pd.DataFrame([
        {
            'User_Type': 'Top user',
            'User_ID': int(top_user),
            'Last_Interaction_Time': last_interaction_time,
            'Num_Interactions': int(user_counts.loc[top_user]),
            'Top5_Percentile_Cutoff': top5_cutoff,
            'Recommended_Movies': format_movie_list(top_ids, items),
        },
        {
            'User_Type': 'Cold user',
            'User_ID': None,
            'Last_Interaction_Time': None,
            'Num_Interactions': 0,
            'Top5_Percentile_Cutoff': None,
            'Recommended_Movies': format_movie_list(cold_ids, items),
        }
    ])

    return df_to_xcom(recs)

def upload(**context):
    """Upload recommendations to S3."""
    recs = xcom_to_df(context["ti"].xcom_pull(task_ids="run_recommend"))
    s3 = boto3.client("s3")

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)

        # Get run count
        run_count_path = td / "count.json"
        s3.download_file(BUCKET_NAME, f'{BASE_PATH}/count.json', str(run_count_path))
        run_count = int(run_count_path.read_text().strip())

        # Upload recommendations
        recs_file = td / f"recs_{run_count}_file.csv"
        recs.to_csv(recs_file, index=False)
        
        if not recs_file.exists():
            print(f"ERROR: File not created at {recs_file}")
            return
        
        print(f"File created: {recs_file}, size: {recs_file.stat().st_size} bytes")
        
        out_key = f'{OUTPUT_PATH}/recs_{run_count}_file.csv'
        print(f"Uploading to: s3://{BUCKET_NAME}/{out_key}")
        
        try:
            s3.upload_file(str(recs_file), BUCKET_NAME, out_key)
            print(f"Recommendations uploaded to {out_key}")
            
            # Verify file exists in S3
            response = s3.head_object(Bucket=BUCKET_NAME, Key=out_key)
            print(f"Verified in S3: {out_key}, size: {response['ContentLength']} bytes")
        except Exception as e:
            print(f"ERROR: {e}")
            raise
        
def update_count(**context):
    """Increment count in S3."""
    s3 = boto3.client('s3')
    run_count_key = f'{BASE_PATH}/count.json'

    with tempfile.TemporaryDirectory() as td:
        local_path = Path(td) / 'count.json'
        s3.download_file(BUCKET_NAME, run_count_key, str(local_path))
        run_count = int(local_path.read_text().strip()) + 1
        local_path.write_text(json.dumps(run_count))
        s3.upload_file(str(local_path), BUCKET_NAME, run_count_key)
        
        print(f"Updated count to {run_count}")


default_args = {
    "owner": "stall-munezero",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 1,
}

with DAG(
    dag_id="stall_munezero_hw3_dag",
    default_args=default_args,
    description="Movie recommendations DAG for HW3",
    schedule=pendulum.duration(hours=10),
    tags=["de300"],
    catchup=False,
    max_active_runs=1,
) as dag:

    branch = BranchPythonOperator(task_id="check_count", python_callable=check_run_count)
    stop = EmptyOperator(task_id="stop_dag")
    cont = EmptyOperator(task_id="continue_dag")
    t_download = PythonOperator(task_id="download", python_callable=download)
    t_recommend = PythonOperator(task_id="run_recommend", python_callable=run_recommend)
    t_upload = PythonOperator(task_id="upload", python_callable=upload)
    t_update = PythonOperator(task_id="update_count", python_callable=update_count)

    branch >> [stop, cont]
    cont >> t_download >> t_recommend >> t_upload >> t_update