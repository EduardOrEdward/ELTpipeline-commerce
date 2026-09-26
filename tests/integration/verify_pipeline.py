"""Verify the end-to-end ingestion boundaries in CI."""

import json
import time
from collections import Counter
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psycopg


API_BASE_URL = "http://mock_api:8000"
S3_BASE_URL = "http://seaweedfs:8333"
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


def canonical_records(records):
    return Counter(
        json.dumps(record, sort_keys=True, separators=(",", ":"))
        for record in records
    )


def verify_bronze() -> None:
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
                object_url = (
                    f"{S3_BASE_URL}/{S3_BUCKET}/"
                    f"{object_key.lstrip('/')}"
                )
                payload = get_json(object_url)

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
