# DATA_ENG 300 Homework 2
Nathaniel Stall - alt5629

## Instructions to Run
- Make sure you have copied and pasted the most recent aws access keys into `homework_2/.env`. See `homework_2/example.env` for the expected keys.
- After that, you should be able to run the Jupyter Notebook from top to bottom, verifying that the corresponding results for each task are stored in S3 or loaded from S3 if they already exist. The sequence of functions in the jupyter notebook is described below.

## Sequence of Functions
### Task 1
- `orchestrate_movielens_download()`
    - calls several subfunctions to do the described task 1 functionality
    - `download_movielens_1m()`
    - `s3_bucket_contains() -> bool`
    - `send_to_s3_bucket()`
    - These functions skip redundant work; if the file for each stage already exists, the process will be skipped.

### Task 2
- `read_ml1m() -> tuple[pd.DataFrame]`
    - Loads ratings, movies, and users dataframes from the MovieLens 1M dataset
- `sample_or_load_users(sample_pct: float) -> pd.DataFrame`
    - Loads or creates a 30% random sample of users; saves for consistency
- `get_bert_pretrained()`
    - Loads the DistilBERT tokenizer and encoder model
- `encode_texts_in_batches(texts, batch_size=128, max_len=64) -> torch.Tensor`
    - Encodes a list of text descriptions into L2-normalized embeddings
- `create_movie_embeddings(movies: pd.DataFrame) -> torch.Tensor`
    - Creates embeddings for all movies by combining title and genres
- `build_positive_ratings(ratings, users_sample) -> tuple[pd.DataFrame]`
    - Filters positive interactions (rating >= 4) from sampled users
- `build_faiss_index(item_emb) -> faiss.IndexFlatIP`
    - Builds a FAISS index for approximate nearest neighbor search using embeddings
- `save_embeddings_to_s3(item_emb, index, movies, bucket_name)`
    - Persists embeddings, FAISS index, and sampled users to local disk and S3

### Task 3
- `get_cold_user(users, users_sample) -> int`
    - Selects a cold user (not in the 30% sample)
- `get_top_user(sample_ratings) -> int`
    - Selects a top user (top 5% by interaction count)
- `build_user_history(user_id: int, events_df, movies_df, n: int) -> tuple`
    - Retrieves recent movie interactions and build user profile
- `_encode_single_text(text: str) -> np.ndarray`
    - Encodes a single text string into an embedding
- `recommend(user_id, movies_df, events_df, idx, k=5, ...) -> dict`
    - Core recommendation function using user history or popularity fallback
- `recommend_for_user(user_id: int, sample_ratings) -> dict`
    - Wrapper for sampled user recommendations
- `summarize_user(user_id: int, user_type: str, sample_ratings) -> dict`
    - Creates a summary record for a user with recommendations and metadata
- `orchestrate_task3() -> pd.DataFrame`
    - Generates recommendations for cold and top users and saves to CSV/S3

### Task 4
- `build_full_embeddings(emb_path, idx_path) -> tuple[pd.DataFrame, torch.Tensor, faiss.IndexFlatIP]`
    - Creates or loads embeddings and FAISS index for the full dataset
- `recommend_for_user_full(user_id: int, movies_df, embs, idx, k=5) -> dict`
    - Generates recommendations using full dataset embeddings
- `summarize_user_full(user_id: int, user_type: str, embs_full, idx) -> dict`
    - Creates a summary record for a user using full dataset recommendations
- `orchestrate_task4_full()`
    - Repeats task 2-3 with full dataset; generates recommendations for cold and top users

### Task 5
- `create_self_profile() -> pd.DataFrame`
    - Creates a personal user profile with 10 rated movies and saves to CSV/S3
- `recommend_for_self(k: int = 5) -> pd.DataFrame`
    - Generates 5 movie recommendations based on personal profile
- `orchestrate_task5() -> pd.DataFrame`
    - Orchestrates self profile creation and recommendation generation; saves to S3