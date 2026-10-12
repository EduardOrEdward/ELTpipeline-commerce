"""Raw object storage for the ingestion layer.

Raw objects and completed snapshot manifests are immutable. A manifest is
written only after every dataset object has been stored successfully.
"""

from datetime import datetime, timezone
import json
from typing import Any
from uuid import uuid4

import boto3
from botocore.client import Config

from ingestion.config import (
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_KEY,
)


def manifest_key_for_snapshot(snapshot_id: str) -> str:
    """Return the stable manifest key for a snapshot."""
    return f"snapshots/{snapshot_id}/manifest.json"


class ObjectStorage:
    """S3-compatible immutable object writer for SeaweedFS."""

    def __init__(
        self,
        endpoint: str = S3_ENDPOINT,
        bucket: str = S3_BUCKET,
        access_key: str | None = S3_ACCESS_KEY,
        secret_key: str | None = S3_SECRET_KEY,
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
            config=Config(signature_version="s3v4"),
        )

    def put_json(self, object_key: str, data: Any) -> None:
        """Write JSON only if the key does not already exist."""
        payload = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        # Conditional writes make accidental overwrites fail instead of
        # silently changing the raw source of truth.
        self.client.put_object(
            Bucket=self.bucket,
            Key=object_key.lstrip("/"),
            Body=payload,
            ContentType="application/json",
            IfNoneMatch="*",
        )

    def write_dataset(
        self,
        dataset: str,
        data: Any,
        snapshot_id: str,
        timestamp: datetime | None = None,
    ) -> str:
        """Write one dataset as an immutable object belonging to a snapshot."""
        timestamp = timestamp or datetime.now(timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        object_key = (
            f"{dataset}/{timestamp:%Y-%m-%d}/{snapshot_id}.json"
        )
        self.put_json(object_key, data)
        return object_key

    def write_manifest(
        self,
        snapshot_id: str,
        object_keys_by_dataset: dict[str, str],
        datasets: dict[str, list[dict[str, Any]]],
        timestamp: datetime | None = None,
    ) -> str:
        """Write the completion manifest after all dataset objects exist."""
        if set(object_keys_by_dataset) != set(datasets):
            raise ValueError("Manifest datasets do not match stored object keys")

        timestamp = timestamp or datetime.now(timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        manifest = {
            "format_version": 1,
            "snapshot_id": snapshot_id,
            "status": "COMPLETED",
            "created_at": timestamp.isoformat(),
            "datasets": {
                dataset: {
                    "object_key": object_keys_by_dataset[dataset],
                    "record_count": len(records),
                }
                for dataset, records in datasets.items()
            },
        }
        manifest_key = manifest_key_for_snapshot(snapshot_id)
        # The manifest is created once, after every data object succeeded.
        self.put_json(manifest_key, manifest)
        return manifest_key


def store_raw(
    datasets: dict[str, Any],
    storage: ObjectStorage | None = None,
    timestamp: datetime | None = None,
    snapshot_id: str | None = None,
) -> dict[str, str]:
    """Store all datasets, then publish a manifest marking the snapshot complete.

    If any dataset write fails, no completion manifest is written and the
    caller must not load the partial snapshot into Bronze.
    """
    if not datasets:
        raise ValueError("Cannot store an empty snapshot")

    for dataset, records in datasets.items():
        if not isinstance(records, list):
            raise ValueError(f"Dataset {dataset!r} must be a JSON array")

    storage = storage or ObjectStorage()
    snapshot_id = snapshot_id or uuid4().hex
    timestamp = timestamp or datetime.now(timezone.utc)
    timestamp = timestamp.astimezone(timezone.utc)

    object_keys_by_dataset: dict[str, str] = {}
    for dataset, records in datasets.items():
        object_keys_by_dataset[dataset] = storage.write_dataset(
            dataset,
            records,
            snapshot_id=snapshot_id,
            timestamp=timestamp,
        )

    # This is the commit marker for the snapshot. It is intentionally last.
    storage.write_manifest(
        snapshot_id=snapshot_id,
        object_keys_by_dataset=object_keys_by_dataset,
        datasets=datasets,
        timestamp=timestamp,
    )
    return object_keys_by_dataset
