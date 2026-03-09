import os
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

PARQUET_URL      = os.environ["PARQUET_URL"]
MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET", "raw")
MINIO_OBJECT_KEY = os.environ.get("MINIO_OBJECT_KEY", "nyc/yellow_tripdata_2025-01.parquet")

# ← Sin secrets, directo de variables de entorno
access_key = os.environ["MINIO_ACCESS_KEY"]
secret_key = os.environ["MINIO_SECRET_KEY"]

CHUNK_SIZE = 8 * 1024 * 1024  # 8MB


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' creado.")


def download_stream(url: str, out_path: str):
    print(f"Descargando {url}...")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    print(" Descarga completa.")


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

print(f" OK: s3://{MINIO_BUCKET}/{MINIO_OBJECT_KEY}")