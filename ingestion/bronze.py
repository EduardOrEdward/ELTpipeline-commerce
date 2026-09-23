"""Load raw objects from S3-compatible storage into PostgreSQL Bronze."""

import json
from typing import Any, Protocol
from urllib.request import Request, urlopen

from ingestion.config import S3_BUCKET, S3_ENDPOINT


class DatabaseConnection(Protocol):
    """Minimal DB-API connection interface required by this module."""

    def cursor(self) -> Any: ...


DATASET_TABLES = {
    "orders": "bronze.orders",
    "deliveries": "bronze.deliveries",
    "inventory": "bronze.inventory",
}


class ObjectStorageReader:
    """Read raw JSON objects from the S3-compatible SeaweedFS endpoint."""

    def __init__(self, endpoint: str = S3_ENDPOINT, bucket: str = S3_BUCKET, timeout: int = 10) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.bucket = bucket
        self.timeout = timeout

    def get_json(self, object_key: str) -> Any:
        """Download and decode one raw JSON object."""
        request = Request(
            f"{self.endpoint}/{self.bucket}/{object_key.lstrip('/')}",
            headers={"Accept": "application/json"},
            method="GET",
        )
        with urlopen(request, timeout=self.timeout) as response:
            return json.load(response)


def _dataset_from_key(object_key: str) -> str:
    """Extract the dataset name from an object key such as orders/2026-09-23/file.json."""
    dataset = object_key.split("/", 1)[0]
    if dataset not in DATASET_TABLES:
        raise ValueError(f"Unsupported Bronze dataset in object key: {object_key}")
    return dataset


def load_object_to_bronze(
    connection: DatabaseConnection,
    object_key: str,
    payload: Any,
) -> int:
    """Insert every raw record from one object into its Bronze table."""
    dataset = _dataset_from_key(object_key)
    table = DATASET_TABLES[dataset]

    if not isinstance(payload, list):
        raise ValueError(f"Expected a JSON array in raw object: {object_key}")

    if not payload:
        return 0

    query = f"INSERT INTO {table} (source_object_key, raw_payload) VALUES (%s, %s::jsonb)"

    with connection.cursor() as cursor:
        cursor.executemany(
            query,
            [(object_key, json.dumps(record, ensure_ascii=False)) for record in payload],
        )

    return len(payload)


def load_objects_to_bronze(
    connection: DatabaseConnection,
    objects: dict[str, Any],
) -> dict[str, int]:
    """Load multiple raw objects into Bronze in one database transaction."""
    loaded: dict[str, int] = {}

    try:
        for object_key, payload in objects.items():
            loaded[object_key] = load_object_to_bronze(
                connection,
                object_key,
                payload,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return loaded
