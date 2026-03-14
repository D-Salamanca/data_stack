from __future__ import annotations

import importlib
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"


def ensure_dependencies() -> None:
    required_modules = [
        "dlt",
        "clickhouse_driver",
        "numpy",
        "pyarrow",
        "clickhouse_connect",
        "pyiceberg.catalog",
        "dotenv",
    ]
    missing_modules: list[str] = []

    for module_name in required_modules:
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError:
            missing_modules.append(module_name)

    if not missing_modules:
        return

    print(f"Installing missing dependencies: {', '.join(missing_modules)}")
    subprocess.run(
        [sys.executable, "-m", "ensurepip", "--upgrade"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-r", str(REPO_ROOT / "requirements.txt"), "numpy"]
    )


ensure_dependencies()

import dlt
from dotenv import load_dotenv
import pyarrow as pa


def load_project_env() -> None:
    load_dotenv(ENV_PATH)


def get_env(name: str, default: str | None = None, required: bool = False) -> str:
    load_project_env()
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    if value is None:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_minio_settings() -> dict[str, str]:
    return {
        "endpoint": get_env("MINIO_ENDPOINT", "http://localhost:9000"),
        "access_key": get_env("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": get_env("MINIO_SECRET_KEY", "minioadmin123"),
        "iceberg_bucket": get_env("MINIO_BUCKET_ICEBERG", "iceberg"),
        "region": get_env("AWS_REGION", "us-east-1"),
    }


def get_iceberg_settings() -> dict[str, str]:
    return {
        "nessie_url": get_env("NESSIE_URL", "http://localhost:19120/iceberg"),
        "branch": get_env("NESSIE_BRANCH", "main"),
        "namespace": get_env("ICEBERG_NAMESPACE", "nyc"),
        "table": get_env("ICEBERG_TABLE", "yellow_tripdata"),
    }


def get_clickhouse_settings() -> dict[str, Any]:
    write_mode = get_env("CLICKHOUSE_WRITE_MODE", "replace").lower()
    if write_mode not in {"append", "replace"}:
        raise RuntimeError("CLICKHOUSE_WRITE_MODE must be 'append' or 'replace'")

    host = get_env("CLICKHOUSE_HOST", "localhost")
    http_port = int(get_env("CLICKHOUSE_PORT", get_env("CLICKHOUSE_HTTP_PORT", "8123")))
    default_native_port = "9002" if host in {"localhost", "127.0.0.1"} else "9000"

    return {
        "host": host,
        "http_port": http_port,
        "native_port": int(get_env("CLICKHOUSE_NATIVE_PORT", default_native_port)),
        "username": get_env("CLICKHOUSE_USER", "default"),
        "password": get_env("CLICKHOUSE_PASSWORD", ""),
        "database": get_env("CLICKHOUSE_DATABASE", "lakehouse"),
        "table": get_env("CLICKHOUSE_TABLE", get_env("ICEBERG_TABLE", "yellow_tripdata")),
        "secure": parse_bool(os.getenv("CLICKHOUSE_SECURE"), default=False),
        "write_mode": write_mode,
    }


def get_dlt_clickhouse_credentials(settings: dict[str, Any]) -> dict[str, Any]:
    return {
        "host": settings["host"],
        "port": settings["native_port"],
        "http_port": settings["http_port"],
        "username": settings["username"],
        "password": settings["password"],
        "database": settings["database"],
        "secure": 1 if settings["secure"] else 0,
    }


def build_nessie_catalog(minio: dict[str, str], iceberg: dict[str, str]) -> Any:
    from pyiceberg.catalog import load_catalog

    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": iceberg["nessie_url"],
            "ref": iceberg["branch"],
            "warehouse": f"s3://{minio['iceberg_bucket']}",
            "s3.endpoint": minio["endpoint"],
            "s3.access-key-id": minio["access_key"],
            "s3.secret-access-key": minio["secret_key"],
            "s3.path-style-access": "true",
            "s3.region": minio["region"],
        },
    )


def build_clickhouse_client(settings: dict[str, Any], database: str | None = None) -> Any:
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=settings["host"],
        port=settings["http_port"],
        username=settings["username"],
        password=settings["password"],
        database=database or settings["database"],
        secure=settings["secure"],
    )


def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def iceberg_table_identifier(namespace: str, table: str) -> tuple[str, str]:
    return namespace, table


@dlt.resource(name="iceberg_rows")
def iceberg_rows_resource() -> Any:
    minio = get_minio_settings()
    iceberg = get_iceberg_settings()
    catalog = build_nessie_catalog(minio, iceberg)
    iceberg_table = catalog.load_table(iceberg_table_identifier(iceberg["namespace"], iceberg["table"]))

    for batch in iceberg_table.scan().to_arrow_batch_reader():
        arrow_table = pa.Table.from_batches([batch])
        yield arrow_table.to_pylist()


def print_clickhouse_summary() -> None:
    clickhouse = get_clickhouse_settings()
    client = build_clickhouse_client(clickhouse)
    tables = [
        row[0]
        for row in client.query(f"SHOW TABLES FROM {quote_identifier(clickhouse['database'])}").result_rows
    ]
    metadata_tables = [table for table in tables if table.startswith("_dlt_") or table == "dlt_sentinel_table"]
    row_count = client.query(
        f"SELECT count() FROM {quote_identifier(clickhouse['database'])}.{quote_identifier(clickhouse['table'])}"
    ).result_rows[0][0]

    print("ClickHouse validation")
    print(f"table: {clickhouse['database']}.{clickhouse['table']}")
    print(f"row_count: {row_count:,}")
    print(f"metadata_tables: {', '.join(metadata_tables)}")


def main() -> None:
    clickhouse = get_clickhouse_settings()
    bootstrap_client = build_clickhouse_client(clickhouse, database="default")
    bootstrap_client.command(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(clickhouse['database'])}")

    client = build_clickhouse_client(clickhouse)

    pipeline = dlt.pipeline(
        pipeline_name="iceberg_to_clickhouse",
        destination=dlt.destinations.clickhouse(
            credentials=get_dlt_clickhouse_credentials(clickhouse)
        ),
        progress="log",
    )

    resource = iceberg_rows_resource().with_name(clickhouse["table"])
    resource.apply_hints(write_disposition=clickhouse["write_mode"])

    load_info = pipeline.run(resource)
    print(load_info)
    print_clickhouse_summary()


if __name__ == "__main__":
    main()
