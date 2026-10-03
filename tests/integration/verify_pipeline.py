"""Verify the end-to-end ingestion boundaries in CI."""

import json
import os
import time
from collections import Counter
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import boto3
import psycopg
from botocore.client import Config


API_BASE_URL = "http://mock_api:8000"
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
S3_BUCKET = "elt-raw"

DB_CONFIG = {
    "host": "db",
    "port": 5432,
    "dbname": "elt_pipeline",
    "user": "postgre_user",
    "password": "postgre_password",
}

DATASETS = {
    "orders": "bronze.orders",
    "deliveries": "bronze.deliveries",
    "inventory": "bronze.inventory",
}


def get_json(url: str):
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    with urlopen(request, timeout=10) as response:
        if response.status != 200:
            raise AssertionError(f"Expected HTTP 200 from {url}, got {response.status}")
        return json.load(response)


def wait_for_api() -> None:
    for _ in range(30):
        try:
            payload = get_json(f"{API_BASE_URL}/health")
            if payload:
                return
        except (HTTPError, URLError):
            pass
        time.sleep(1)
    raise AssertionError("Mock API did not become ready")


def verify_api_contract() -> None:
    for endpoint in ("/orders?limit=10", "/delivery?limit=10", "/inventory"):
        payload = get_json(f"{API_BASE_URL}{endpoint}")
        if not isinstance(payload, list) or not payload:
            raise AssertionError(f"Mock API returned no data for {endpoint}")


def create_s3_client():
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")

    if not access_key or not secret_key:
        raise AssertionError("S3 credentials are not configured for integration verification")

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def get_s3_json(client, object_key: str):
    response = client.get_object(Bucket=S3_BUCKET, Key=object_key)
    return json.loads(response["Body"].read().decode("utf-8"))


def canonical_records(records):
    return Counter(
        json.dumps(record, sort_keys=True, separators=(",", ":"))
        for record in records
    )


def verify_bronze() -> None:
    s3_client = create_s3_client()

    with psycopg.connect(**DB_CONFIG) as connection:
        for dataset, table in DATASETS.items():
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT source_object_key, raw_payload
                    FROM {table}
                    WHERE source_object_key IS NOT NULL
                    """
                )
                rows = cursor.fetchall()

            if not rows:
                raise AssertionError(f"Bronze table {table} is empty")

            object_keys = sorted({row[0] for row in rows})
            if not object_keys:
                raise AssertionError(f"No source object keys found in {table}")

            for object_key in object_keys:
                payload = get_s3_json(s3_client, object_key)

                if not isinstance(payload, list) or not payload:
                    raise AssertionError(
                        f"SeaweedFS object is empty or invalid: {object_key}"
                    )

                bronze_records = [
                    row[1]
                    for row in rows
                    if row[0] == object_key
                ]

                if len(bronze_records) != len(payload):
                    raise AssertionError(
                        f"{dataset}: row count mismatch for {object_key}: "
                        f"SeaweedFS={len(payload)}, Bronze={len(bronze_records)}"
                    )

                if canonical_records(payload) != canonical_records(bronze_records):
                    raise AssertionError(
                        f"{dataset}: Bronze payload differs from SeaweedFS object "
                        f"{object_key}"
                    )

            print(
                f"[OK] {dataset}: "
                f"{len(rows)} Bronze rows verified against {len(object_keys)} raw object(s)"
            )


def main() -> None:
    wait_for_api()
    verify_api_contract()
    print("[OK] Mock API returned successful non-empty JSON responses")

    verify_bronze()
    print("[OK] End-to-end ingestion boundaries verified")


if __name__ == "__main__":
    main()
