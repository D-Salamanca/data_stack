import os
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# ========= CONFIG mínima (sin credenciales aquí) =========
PARQUET_URL = os.environ["PARQUET_URL"]              # ej: https://...parquet
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]        # ej: http://minio:9000
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "raw") # bucket destino
MINIO_OBJECT_KEY = os.environ.get("MINIO_OBJECT_KEY", "nyc/yellow_tripdata_2025-01.parquet")

ACCESS_KEY_FILE = os.environ["MINIO_ACCESS_KEY_FILE"]  # /run/secrets/minio_access_key
SECRET_KEY_FILE = os.environ["MINIO_SECRET_KEY_FILE"]  # /run/secrets/minio_secret_key

CHUNK_SIZE = 8 * 1024 * 1024  # 8MB


def read_secret(path: str) -> str:
    if not os.path.exists(path):
        raise RuntimeError(f"Secret file no existe: {path}")
    with open(path, "r", encoding="utf-8") as f:
        val = f.read().strip()
    if not val:
        raise RuntimeError(f"Secret vacío: {path}")
    return val


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def download_stream(url: str, out_path: str):
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)


def main():
    access_key = read_secret(ACCESS_KEY_FILE)
    secret_key = read_secret(SECRET_KEY_FILE)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    ensure_bucket(s3, MINIO_BUCKET)

    local_path = "/home/jovyan/data/yellow_tripdata_2025-01.parquet"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    download_stream(PARQUET_URL, local_path)
    s3.upload_file(local_path, MINIO_BUCKET, MINIO_OBJECT_KEY)

    print(f"OK: s3://{MINIO_BUCKET}/{MINIO_OBJECT_KEY}")


if __name__ == "__main__":
    main()