"""Load completed immutable raw snapshots into PostgreSQL Bronze."""

import json
from typing import Any, Protocol

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from ingestion.config import (
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_KEY,
)
from ingestion.storage import (
    CATALOG_FORMAT_VERSION,
    SNAPSHOT_CATALOG_KEY,
    manifest_key_for_snapshot,
)


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


def _is_not_found_error(error: ClientError) -> bool:
    """Return whether an S3 error specifically means that an object is absent."""
    response = error.response
    code = response.get("Error", {}).get("Code")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"NoSuchKey", "NotFound", "404"} or status == 404


class ObjectStorageReader:
    """Read raw JSON from SeaweedFS without requiring S3 ListBucket permission."""

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

    def list_manifest_keys(self) -> list[str]:
        """Return known manifest references from the catalog, without listing S3."""
        try:
            catalog = self.get_json(SNAPSHOT_CATALOG_KEY)
        except ClientError as error:
            if _is_not_found_error(error):
                return []
            raise
        if (
            not isinstance(catalog, dict)
            or catalog.get("format_version") != CATALOG_FORMAT_VERSION
            or not isinstance(catalog.get("snapshots"), list)
        ):
            raise ValueError(f"Invalid snapshot catalog: {SNAPSHOT_CATALOG_KEY}")

        manifest_keys = []
        seen_snapshots: dict[str, str] = {}
        for entry in catalog["snapshots"]:
            if not isinstance(entry, dict):
                raise ValueError("Snapshot catalog contains an invalid entry")
            snapshot_id = entry.get("snapshot_id")
            manifest_key = entry.get("manifest_key")
            if (
                not isinstance(snapshot_id, str)
                or not snapshot_id
                or not isinstance(manifest_key, str)
                or manifest_key != manifest_key_for_snapshot(snapshot_id)
            ):
                raise ValueError("Snapshot catalog contains an invalid manifest reference")

            previous_key = seen_snapshots.get(snapshot_id)
            if previous_key is not None and previous_key != manifest_key:
                raise ValueError(f"Snapshot {snapshot_id} has conflicting catalog entries")
            seen_snapshots[snapshot_id] = manifest_key
            if manifest_key not in manifest_keys:
                manifest_keys.append(manifest_key)

        return sorted(manifest_keys)


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
        return max(cursor.rowcount, 0)


def _validate_completed_snapshot(
    snapshot_id: str,
    manifest_key: str,
    manifest: dict[str, Any],
    raw_objects: dict[str, Any],
) -> dict[str, str]:
    """Validate manifest identity, completeness, and the objects supplied for loading."""
    if not isinstance(manifest, dict):
        raise ValueError("Snapshot manifest must be a JSON object")
    if not isinstance(snapshot_id, str) or not snapshot_id:
        raise ValueError("Snapshot ID must be a non-empty string")
    if manifest.get("format_version") != 1:
        raise ValueError("Unsupported snapshot manifest format version")
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
        key_parts = object_key.split("/")
        if (
            len(key_parts) != 3
            or key_parts[0] != dataset
            or not key_parts[1]
            or key_parts[2] != f"{snapshot_id}.json"
        ):
            raise ValueError(
                f"Raw object key does not belong to snapshot {snapshot_id}: {object_key}"
            )

        record_count = details.get("record_count")
        if (
            not isinstance(record_count, int)
            or isinstance(record_count, bool)
            or record_count < 0
        ):
            raise ValueError(f"Invalid record count for raw object: {object_key}")

        payload = raw_objects.get(object_key)
        if not isinstance(payload, list):
            raise ValueError(f"Missing or invalid raw object: {object_key}")
        if any(not isinstance(record, dict) for record in payload):
            raise ValueError(f"Raw object must contain only JSON objects: {object_key}")
        if record_count != len(payload):
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
    ensure_schema: bool = True,
) -> dict[str, Any]:
    """Atomically load one completed snapshot and register it for replay safety."""
    object_keys_by_dataset = _validate_completed_snapshot(
        snapshot_id,
        manifest_key,
        manifest,
        raw_objects,
    )

    try:
        if ensure_schema:
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
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT manifest_key
                    FROM bronze.loaded_snapshots
                    WHERE snapshot_id = %s
                    """,
                    (snapshot_id,),
                )
                existing_snapshot = cursor.fetchone()

            if existing_snapshot is None or existing_snapshot[0] != manifest_key:
                raise ValueError(
                    f"Snapshot {snapshot_id} is already registered with a different manifest"
                )

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
