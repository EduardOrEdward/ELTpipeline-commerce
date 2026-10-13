"""Verify immutable snapshot and Bronze boundaries in CI."""

import json
import os
import time
from collections import Counter
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import boto3
import psycopg
from botocore.client import Config
from botocore.exceptions import ClientError

from ingestion.bronze import load_snapshot_to_bronze


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


def verify_immutable_object(client, object_key: str, original_payload) -> None:
    """A conditional write to an existing key must fail without changing it."""
    try:
        client.put_object(
            Bucket=S3_BUCKET,
            Key=object_key,
            Body=b'{"overwrite":"must-not-happen"}',
            ContentType="application/json",
            IfNoneMatch="*",
        )
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = exc.response.get("Error", {}).get("Code")
        if status not in (409, 412) and code not in (
            "PreconditionFailed",
            "ConditionalRequestConflict",
        ):
            raise AssertionError(
                f"Conditional write failed for an unexpected reason: {code} ({status})"
            ) from exc
    else:
        raise AssertionError(f"Existing S3 object was overwritten: {object_key}")

    if get_s3_json(client, object_key) != original_payload:
        raise AssertionError(f"S3 object changed after rejected overwrite: {object_key}")


def verify_bronze() -> None:
    s3_client = create_s3_client()

    # The ingestion identity intentionally has no ListBucket permission.
    try:
        s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix="snapshots/")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code not in {"AccessDenied", "403"} and status != 403:
            raise AssertionError(
                f"Listing S3 unexpectedly failed with {code} ({status})"
            ) from exc
    else:
        raise AssertionError("Ingestion credentials unexpectedly have ListBucket access")

    catalog = get_s3_json(s3_client, "snapshots/catalog.json")
    if catalog.get("format_version") != 1 or not isinstance(catalog.get("snapshots"), list):
        raise AssertionError("Snapshot catalog has an invalid format")
    catalog_manifest_keys = {
        entry.get("manifest_key")
        for entry in catalog["snapshots"]
        if isinstance(entry, dict)
    }

    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT snapshot_id, manifest_key
                FROM bronze.loaded_snapshots
                ORDER BY snapshot_id
                """
            )
            snapshots = cursor.fetchall()

        if not snapshots:
            raise AssertionError("No completed snapshots are registered in Bronze")

        expected_objects = set()
        manifests = {}
        for snapshot_id, manifest_key in snapshots:
            manifest = get_s3_json(s3_client, manifest_key)
            if manifest.get("snapshot_id") != snapshot_id:
                raise AssertionError(f"Snapshot ID mismatch in {manifest_key}")
            if manifest_key not in catalog_manifest_keys:
                raise AssertionError(f"Loaded manifest is missing from the catalog: {manifest_key}")
            if manifest.get("status") != "COMPLETED":
                raise AssertionError(f"Non-completed snapshot was loaded: {manifest_key}")
            if set(manifest.get("datasets", {})) != set(DATASETS):
                raise AssertionError(f"Incomplete dataset list in {manifest_key}")
            manifests[snapshot_id] = (manifest_key, manifest)
            expected_objects.update(
                details["object_key"] for details in manifest["datasets"].values()
            )

        actual_objects = set()
        total_rows_by_table = {}
        for dataset, table in DATASETS.items():
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT source_object_key, record_index, raw_payload
                    FROM {table}
                    WHERE source_object_key IS NOT NULL
                    """
                )
                rows = cursor.fetchall()

            if not rows:
                raise AssertionError(f"Bronze table {table} is empty")

            table_objects = {row[0] for row in rows}
            actual_objects.update(table_objects)
            total_rows_by_table[table] = len(rows)

            for object_key in sorted(table_objects):
                if object_key not in expected_objects:
                    raise AssertionError(
                        f"Bronze references an object outside completed snapshots: {object_key}"
                    )
                payload = get_s3_json(s3_client, object_key)
                if not isinstance(payload, list):
                    raise AssertionError(f"SeaweedFS object is not a JSON array: {object_key}")

                bronze_rows = [row for row in rows if row[0] == object_key]
                if len(bronze_rows) != len(payload):
                    raise AssertionError(
                        f"{dataset}: row count mismatch for {object_key}: "
                        f"SeaweedFS={len(payload)}, Bronze={len(bronze_rows)}"
                    )
                if sorted(row[1] for row in bronze_rows) != list(range(len(payload))):
                    raise AssertionError(f"{dataset}: record indexes are not complete for {object_key}")
                if canonical_records(payload) != canonical_records(
                    [row[2] for row in bronze_rows]
                ):
                    raise AssertionError(
                        f"{dataset}: Bronze payload differs from S3 object {object_key}"
                    )

            print(f"[OK] {dataset}: {len(rows)} Bronze rows verified")

        if actual_objects != expected_objects:
            missing = sorted(expected_objects - actual_objects)
            unexpected = sorted(actual_objects - expected_objects)
            raise AssertionError(
                f"Bronze/S3 object coverage mismatch; missing={missing}, unexpected={unexpected}"
            )

        # Replaying an already loaded snapshot must be a no-op.
        snapshot_id, (manifest_key, manifest) = next(iter(manifests.items()))
        raw_objects = {
            details["object_key"]: get_s3_json(s3_client, details["object_key"])
            for details in manifest["datasets"].values()
        }
        result = load_snapshot_to_bronze(
            connection,
            snapshot_id,
            manifest_key,
            manifest,
            raw_objects,
        )
        if not result["already_loaded"] or result["bronze_rows"]:
            raise AssertionError(f"Replaying a loaded snapshot was not a no-op: {result}")

        # Verify the raw object is protected against overwrite.
        first_dataset = next(iter(manifest["datasets"].values()))
        object_key = first_dataset["object_key"]
        verify_immutable_object(
            s3_client,
            object_key,
            get_s3_json(s3_client, object_key),
        )

        print(
            f"[OK] {len(snapshots)} completed snapshot(s), catalog coverage, "
            "restricted S3 listing, immutable objects, and idempotent Bronze replay verified"
        )


def main() -> None:
    wait_for_api()
    verify_api_contract()
    print("[OK] Mock API returned successful non-empty JSON responses")
    verify_bronze()
    print("[OK] End-to-end ingestion boundaries verified")


if __name__ == "__main__":
    main()
