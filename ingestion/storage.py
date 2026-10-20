"""Raw object storage for ingestion snapshots and their catalog.

Dataset objects and completed manifests are immutable. The mutable catalog is a
small index that lets a restricted reader find manifests without S3 ListBucket.
"""

from datetime import datetime, timezone
import json
import time
from typing import Any
from uuid import uuid4

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from ingestion.config import (
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_KEY,
)

SNAPSHOT_CATALOG_KEY = "snapshots/catalog.json"
CATALOG_FORMAT_VERSION = 1
CATALOG_WRITE_ATTEMPTS = 5
REQUIRED_DATASETS = frozenset({"orders", "deliveries", "inventory"})


def manifest_key_for_snapshot(snapshot_id: str) -> str:
    """Return the stable manifest key for a snapshot."""
    return f"snapshots/{snapshot_id}/manifest.json"


def _is_not_found_error(error: ClientError) -> bool:
    """Return whether an S3 error specifically means that the object is absent."""
    response = error.response
    code = response.get("Error", {}).get("Code")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"NoSuchKey", "NotFound", "404"} or status == 404


def _is_conditional_conflict(error: ClientError) -> bool:
    """Return whether a conditional catalog write lost a race."""
    response = error.response
    code = response.get("Error", {}).get("Code")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return status in {409, 412} or code in {
        "PreconditionFailed",
        "ConditionalRequestConflict",
    }


class ObjectStorage:
    """S3-compatible writer for immutable objects and a concurrency-safe catalog."""

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
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 4, "mode": "standard"},
            ),
        )

    def put_json(self, object_key: str, data: Any) -> None:
        """Write immutable JSON only if the key does not already exist."""
        payload = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        key = object_key.lstrip("/")
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=payload,
                ContentType="application/json",
                IfNoneMatch="*",
            )
        except ClientError as error:
            if not _is_conditional_conflict(error):
                raise

            # A previous attempt may have successfully written the object but
            # lost the response. Treat an identical immutable object as success;
            # never overwrite or silently accept different content.
            try:
                existing = self.get_json(key)
            except ClientError:
                raise error
            if existing != data:
                raise ValueError(
                    f"Immutable S3 object already exists with different content: {key}"
                ) from error

    def get_json(self, object_key: str) -> Any:
        """Download and decode one JSON object."""
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=object_key.lstrip("/"),
        )
        return json.loads(response["Body"].read().decode("utf-8"))

    def register_snapshot(
        self,
        snapshot_id: str,
        manifest_key: str,
        timestamp: datetime | None = None,
    ) -> None:
        """Add a manifest reference to the catalog without requiring ListBucket.

        Conditional writes prevent concurrent ingestion runs from silently
        overwriting one another's catalog updates. A catalog entry is an index
        entry, not proof that the snapshot is complete: the manifest is still
        written last and is the authoritative completion marker.
        """
        timestamp = timestamp or datetime.now(timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        new_entry = {
            "snapshot_id": snapshot_id,
            "manifest_key": manifest_key,
            "created_at": timestamp.isoformat(),
        }

        for attempt in range(CATALOG_WRITE_ATTEMPTS):
            etag = None
            try:
                response = self.client.get_object(
                    Bucket=self.bucket,
                    Key=SNAPSHOT_CATALOG_KEY,
                )
            except ClientError as error:
                if not _is_not_found_error(error):
                    raise
                catalog = {
                    "format_version": CATALOG_FORMAT_VERSION,
                    "snapshots": [],
                }
            else:
                etag = response.get("ETag")
                catalog = json.loads(response["Body"].read().decode("utf-8"))

            if (
                not isinstance(catalog, dict)
                or catalog.get("format_version") != CATALOG_FORMAT_VERSION
                or not isinstance(catalog.get("snapshots"), list)
            ):
                raise ValueError(f"Invalid snapshot catalog: {SNAPSHOT_CATALOG_KEY}")

            existing = {
                item.get("snapshot_id"): item
                for item in catalog["snapshots"]
                if isinstance(item, dict) and isinstance(item.get("snapshot_id"), str)
            }
            previous = existing.get(snapshot_id)
            if previous is not None:
                if previous.get("manifest_key") != manifest_key:
                    raise ValueError(
                        f"Snapshot {snapshot_id} is already registered with another manifest"
                    )
                return

            catalog["snapshots"].append(new_entry)
            catalog["snapshots"].sort(
                key=lambda item: (item.get("created_at", ""), item["snapshot_id"])
            )
            payload = json.dumps(
                catalog,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            write_args = {
                "Bucket": self.bucket,
                "Key": SNAPSHOT_CATALOG_KEY,
                "Body": payload,
                "ContentType": "application/json",
            }
            if etag is None:
                write_args["IfNoneMatch"] = "*"
            else:
                write_args["IfMatch"] = etag

            try:
                self.client.put_object(**write_args)
                return
            except ClientError as error:
                if not _is_conditional_conflict(error):
                    raise
                if attempt == CATALOG_WRITE_ATTEMPTS - 1:
                    raise RuntimeError(
                        "Could not update the snapshot catalog after concurrent writes"
                    ) from error
                time.sleep(0.05 * (attempt + 1))

        raise RuntimeError("Could not update the snapshot catalog")


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
        object_key = f"{dataset}/{timestamp:%Y-%m-%d}/{snapshot_id}.json"
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
        if set(datasets) != REQUIRED_DATASETS:
            raise ValueError("Manifest must contain orders, deliveries, and inventory")

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
        self.put_json(manifest_key, manifest)
        return manifest_key


def validate_snapshot_datasets(datasets: dict[str, Any]) -> None:
    """Validate the required dataset package shape before writing any S3 object.

    This validates package completeness and JSON record shape only. It does not
    validate business fields, deduplicate records, or infer whether the source
    API itself omitted records from an otherwise valid response.
    """
    if not isinstance(datasets, dict):
        raise ValueError("Snapshot datasets must be a mapping")

    received = set(datasets)
    if received != REQUIRED_DATASETS:
        missing = sorted(REQUIRED_DATASETS - received)
        unexpected = sorted(received - REQUIRED_DATASETS)
        details = []
        if missing:
            details.append(f"missing datasets: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected datasets: {', '.join(unexpected)}")
        raise ValueError("Incomplete snapshot package (" + "; ".join(details) + ")")

    for dataset, records in datasets.items():
        if not isinstance(records, list):
            raise ValueError(f"Dataset {dataset!r} must be a JSON array")
        if any(not isinstance(record, dict) for record in records):
            raise ValueError(f"Dataset {dataset!r} must contain only JSON objects")


def store_raw(
    datasets: dict[str, Any],
    storage: ObjectStorage | None = None,
    timestamp: datetime | None = None,
    snapshot_id: str | None = None,
) -> dict[str, str]:
    """Validate and store a complete immutable snapshot.

    If validation, a dataset write, or the catalog write fails, no completion
    manifest is published. A catalog entry can temporarily point to a missing
    manifest if manifest creation fails; restore ignores such entries and never
    loads partial data.
    """
    validate_snapshot_datasets(datasets)

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

    # Register before publishing the manifest. The manifest remains the
    # completion marker and is always the final write for a successful run.
    manifest_key = manifest_key_for_snapshot(snapshot_id)
    storage.register_snapshot(snapshot_id, manifest_key, timestamp=timestamp)
    storage.write_manifest(
        snapshot_id=snapshot_id,
        object_keys_by_dataset=object_keys_by_dataset,
        datasets=datasets,
        timestamp=timestamp,
    )
    return object_keys_by_dataset
