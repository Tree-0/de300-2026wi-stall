from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import json
import io
import pickle

OUTPUT_BUCKET = "stall-munezero-final-project"

SOURCE_BUCKET = "dinglin-winter26"
SOURCE_KEY = "lab7/cars.csv"

FEATURES = ["Weight", "Drive Ratio", "Horsepower", "Displacement", "Cylinders"]
TARGET = "MPG"
TEST_SIZE = 0.25
RANDOM_STATE = 42

default_args = {
    "owner": "de300",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 1,
}

def read_data_from_s3(**context):
    import pandas as pd
    import boto3

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    for col in FEATURES + [TARGET]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=FEATURES + [TARGET])
    print(f"Loaded {len(df)} rows from S3. Columns: {list(df.columns)}")
    return df.to_json(orient="records")


def split_train_test(**context):
    """Pull data from XCom, split into train/test, push back as JSON."""
    import pandas as pd
    from sklearn.model_selection import train_test_split

    ti = context["ti"]
    data_json = ti.xcom_pull(task_ids="read_data_from_s3")
    df = pd.read_json(data_json)

    X = df[FEATURES]
    y = df[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )
    print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")

    ti.xcom_push(key="X_train", value=json.loads(X_train.to_json(orient="values")))
    ti.xcom_push(key="X_test", value=json.loads(X_test.to_json(orient="values")))
    ti.xcom_push(key="y_train", value=y_train.tolist())
    ti.xcom_push(key="y_test", value=y_test.tolist())


def train_linear_regression(**context):
    """Train LinearRegression and upload model pickle to S3."""
    import numpy as np
    from sklearn.linear_model import LinearRegression
    import boto3

    ti = context["ti"]
    ds = context["ds"]

    X_train = np.array(ti.xcom_pull(task_ids="split_train_test", key="X_train"))
    y_train = np.array(ti.xcom_pull(task_ids="split_train_test", key="y_train"))

    model = LinearRegression()
    model.fit(X_train, y_train)
    print("Model fitted. Coefficients:", model.coef_, "Intercept:", model.intercept_)

    key = f"lab7/output/dt={ds}/model.pkl"
    buffer = io.BytesIO()
    pickle.dump(model, buffer)
    buffer.seek(0)
    s3 = boto3.client("s3")
    s3.upload_fileobj(buffer, OUTPUT_BUCKET, key)
    print(f"Uploaded model to s3://{OUTPUT_BUCKET}/{key}")


def evaluate_linear_regression(**context):
    """Evaluate model and write metrics.json to S3. Uses model from S3 if present."""
    import numpy as np
    from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
    import boto3

    ti = context["ti"]
    ds = context["ds"]

    X_test = np.array(ti.xcom_pull(task_ids="split_train_test", key="X_test"))
    y_test = np.array(ti.xcom_pull(task_ids="split_train_test", key="y_test"))

    s3 = boto3.client("s3")
    key = f"lab7/output/dt={ds}/model.pkl"
    buffer = io.BytesIO()
    s3.download_fileobj(OUTPUT_BUCKET, key, buffer)
    buffer.seek(0)
    model = pickle.load(buffer)

    y_pred = model.predict(X_test)
    metrics = {
        "r2_score": float(r2_score(y_test, y_pred)),
        "mse": float(mean_squared_error(y_test, y_pred)),
        "mae": float(mean_absolute_error(y_test, y_pred)),
        "ds": ds,
        "n_test": int(len(y_test)),
    }
    print("Metrics:", metrics)

    metrics_key = f"lab7/output/dt={ds}/metrics.json"
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=metrics_key,
        Body=json.dumps(metrics, indent=2),
        ContentType="application/json",
    )
    print(f"Uploaded metrics to s3://{OUTPUT_BUCKET}/{metrics_key}")
    return metrics


with DAG(
    dag_id="lab7_linear_regression",
    default_args=default_args,
    description="Lab 7 HW: Linear regression on cars.csv from S3",
    schedule="@daily",
    catchup=False,
    tags=["de300", "lab7"],
) as dag:

    read_s3 = PythonOperator(
        task_id="read_data_from_s3",
        python_callable=read_data_from_s3,
    )

    split = PythonOperator(
        task_id="split_train_test",
        python_callable=split_train_test,
    )

    train = PythonOperator(
        task_id="train_linear_regression",
        python_callable=train_linear_regression,
    )

    evaluate = PythonOperator(
        task_id="evaluate_linear_regression",
        python_callable=evaluate_linear_regression,
    )

    read_s3 >> split >> train >> evaluate
