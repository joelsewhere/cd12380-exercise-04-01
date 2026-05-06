"""
Setup DAG: pushes the pre-generated exercise data into the student's S3 bucket.

Source files live alongside this DAG under ./landing/ and get uploaded to
s3://<s3_bucket>/landing/ preserving the directory structure.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from botocore.config import Config

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

AWS_CONN_ID       = "aws_default"
LOCAL_LANDING_DIR = Path(__file__).parent / "landing"
S3_LANDING_PREFIX = "landing/"
MAX_WORKERS       = 32


with DAG(
    dag_id="setup_s3_data",
    schedule=None,            # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["setup"],
) as dag:

    @task
    def push_to_s3() -> None:
        from airflow.models import Variable

        bucket = Variable.get("s3_bucket")

        # Pull a boto3 Session from the Airflow connection (so we keep connection-based
        # credentials), then build a client whose connection pool is sized to match the
        # worker count — the default pool of 10 would otherwise bottleneck the threads.
        session   = S3Hook(aws_conn_id=AWS_CONN_ID).get_session()
        s3_client = session.client(
            "s3",
            config=Config(max_pool_connections=MAX_WORKERS),
        )

        # Walk every file under landing/ regardless of dataset or interval depth
        files = [p for p in LOCAL_LANDING_DIR.rglob("*") if p.is_file()]
        if not files:
            raise FileNotFoundError(f"No files found under {LOCAL_LANDING_DIR}")

        def upload(path: Path) -> str:
            # Mirror the local directory structure under the S3 landing prefix
            relative = path.relative_to(LOCAL_LANDING_DIR)
            s3_key   = f"{S3_LANDING_PREFIX}{relative.as_posix()}"
            s3_client.upload_file(str(path), bucket, s3_key)
            return s3_key

        # Boto3 clients are thread-safe; ThreadPool gives concurrent PutObject calls
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            uploaded = list(pool.map(upload, files))

        print(f"Uploaded {len(uploaded)} files to s3://{bucket}/{S3_LANDING_PREFIX}")

    push_to_s3()