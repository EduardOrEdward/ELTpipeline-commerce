"""Load completed immutable raw snapshots into PostgreSQL Bronze."""

import json
from typing import Any, Protocol

import boto3
from botocore.client import Config

from ingestion.config import (
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_KEY,
)
from ingestion.storage import manifest_key_for_snapshot


class DatabaseConnection(Protocol):
    """Minimal DB-API connection interface required by this module."""

    def cursor(self) -> Any: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


DATASET_TABLES = {
    "orders": "bronze.orders",
    "deliveries": "bronze.deliveries",
    "inventory": "bronze.inventory",
}


class ObjectStorageReader:
    """Read raw JSON objects from the S3-compatible SeaweedFS endpoint."""

    def __init__(
        self,
        endpoint: str = S3_ENDPOINT,
        bucket: str = S3_BUCKET,
        access_key: str | None = S3_ACCESS_KEY,
        secret_key: str | None = S3_SECRET_KEY,
        timeout: int = 10,
    ) -> None:
        if not access_key or not secret_key:
            raise ValueError("S3_ACCESS_KEY and S3_SECRET_KEY must be configured")

        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint.rstrip("/"),
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=Config(signature_version="s3v4", read_timeout=timeout),
        )

    def get_json(self, object_key: str) -> Any:
        """Download and decode one raw JSON object."""
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=object_key.lstrip("/"),
        )
        return json.loads(response["Body"].read().decode("utf-8"))


def _dataset_from_key(object_key: str) -> str:
    """Extract and validate the dataset name from an object key."""
    dataset = object_key.split("/", 1)[0]
    if dataset not in DATASET_TABLES:
        raise ValueError(f"Unsupported Bronze dataset in object key: {object_key}")
    return dataset


def ensure_bronze_schema(connection: DatabaseConnection) -> None:
    """Create snapshot tracking and idempotency structures, including on old volumes."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.loaded_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                manifest_key TEXT NOT NULL,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        for dataset, table in DATASET_TABLES.items():
            cursor.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS record_index INTEGER"
            )
            cursor.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {dataset}_source_record_uidx
                ON {table} (source_object_key, record_index)
                """
            )


def _insert_object_to_bronze(
    connection: DatabaseConnection,
    object_key: str,
    payload: Any,
) -> int:
    """Insert a raw array with stable per-object record indexes."""
    dataset = _dataset_from_key(object_key)
    table = DATASET_TABLES[dataset]

    if not isinstance(payload, list):
        raise ValueError(f"Expected a JSON array in raw object: {object_key}")

    if not payload:
        return 0

    query = f"""
        INSERT INTO {table} (source_object_key, record_index, raw_payload)
        VALUES (%s, %s, %s::jsonb)
        ON CONFLICT (source_object_key, record_index) DO NOTHING
    """
    with connection.cursor() as cursor:
        cursor.executemany(
            query,
            [
                (object_key, index, json.dumps(record, ensure_ascii=False))
                for index, record in enumerate(payload)
            ],
        )
        # psycopg 3 reports the total number of rows affected by executemany.
        return max(cursor.rowcount, 0)


def load_object_to_bronze(
    connection: DatabaseConnection,
    object_key: str,
    payload: Any,
) -> int:
    """Insert one object idempotently; transaction ownership stays with caller."""
    ensure_bronze_schema(connection)
    return _insert_object_to_bronze(connection, object_key, payload)


def load_objects_to_bronze(
    connection: DatabaseConnection,
    objects: dict[str, Any],
) -> dict[str, int]:
    """Load raw objects in one transaction, with per-record duplicate protection."""
    loaded: dict[str, int] = {}

    try:
        ensure_bronze_schema(connection)
        for object_key, payload in objects.items():
            loaded[object_key] = _insert_object_to_bronze(
                connection,
                object_key,
                payload,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return loaded


def _validate_completed_snapshot(
    snapshot_id: str,
    manifest_key: str,
    manifest: dict[str, Any],
    raw_objects: dict[str, Any],
) -> dict[str, str]:
    """Validate manifest identity, completeness, and the objects supplied for loading."""
    if manifest.get("snapshot_id") != snapshot_id:
        raise ValueError("Snapshot ID does not match the S3 manifest")
    if manifest_key != manifest_key_for_snapshot(snapshot_id):
        raise ValueError("Unexpected S3 manifest key")
    if manifest.get("status") != "COMPLETED":
        raise ValueError("Only COMPLETED snapshots may be loaded into Bronze")

    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict) or set(datasets) != set(DATASET_TABLES):
        raise ValueError("Snapshot manifest must contain orders, deliveries, and inventory")

    object_keys: dict[str, str] = {}
    for dataset, details in datasets.items():
        if not isinstance(details, dict):
            raise ValueError(f"Invalid manifest entry for dataset {dataset}")
        object_key = details.get("object_key")
        if not isinstance(object_key, str) or _dataset_from_key(object_key) != dataset:
            raise ValueError(f"Invalid object key for dataset {dataset}")
        payload = raw_objects.get(object_key)
        if not isinstance(payload, list):
            raise ValueError(f"Missing or invalid raw object: {object_key}")
        if details.get("record_count") != len(payload):
            raise ValueError(f"Record count mismatch for raw object: {object_key}")
        object_keys[dataset] = object_key

    if set(raw_objects) != set(object_keys.values()):
        raise ValueError("Loaded objects do not exactly match the snapshot manifest")

    return object_keys


def load_snapshot_to_bronze(
    connection: DatabaseConnection,
    snapshot_id: str,
    manifest_key: str,
    manifest: dict[str, Any],
    raw_objects: dict[str, Any],
) -> dict[str, Any]:
    """Atomically load one completed snapshot and register it for replay safety."""
    object_keys_by_dataset = _validate_completed_snapshot(
        snapshot_id,
        manifest_key,
        manifest,
        raw_objects,
    )

    try:
        ensure_bronze_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO bronze.loaded_snapshots (snapshot_id, manifest_key)
                VALUES (%s, %s)
                ON CONFLICT (snapshot_id) DO NOTHING
                RETURNING snapshot_id
                """,
                (snapshot_id, manifest_key),
            )
            inserted_snapshot = cursor.fetchone()

        if inserted_snapshot is None:
            # This exact snapshot has already committed to Bronze.
            connection.commit()
            return {
                "snapshot_id": snapshot_id,
                "already_loaded": True,
                "bronze_rows": {},
            }

        loaded_rows: dict[str, int] = {}
        for dataset, object_key in object_keys_by_dataset.items():
            loaded_rows[object_key] = _insert_object_to_bronze(
                connection,
                object_key,
                raw_objects[object_key],
            )

        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return {
        "snapshot_id": snapshot_id,
        "already_loaded": False,
        "bronze_rows": loaded_rows,
    }
