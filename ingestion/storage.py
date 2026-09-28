"""Raw object storage for the ingestion layer.

This module serializes extracted Python data and writes it to the S3-compatible
SeaweedFS endpoint. It deliberately does not transform or validate records.
"""

from datetime import datetime, timezone
import json
from typing import Any

import boto3
from botocore.client import Config

from ingestion.config import (
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_SECRET_KEY,
)


class ObjectStorage:
    """S3-compatible object writer for SeaweedFS."""

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
            config=Config(signature_version="s3v4"),
        )

    def put_json(self, object_key: str, data: Any) -> None:
        """Serialize data as JSON and store it under the given object key."""
        payload = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        self.client.put_object(
            Bucket=self.bucket,
            Key=object_key.lstrip("/"),
            Body=payload,
            ContentType="application/json",
        )

    def write_dataset(
        self,
        dataset: str,
        data: Any,
        timestamp: datetime | None = None,
    ) -> str:
        """Write one raw dataset and return its object key."""
        timestamp = timestamp or datetime.now(timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)

        object_key = (
            f"{dataset}/"
            f"{timestamp:%Y-%m-%d}/"
            f"{timestamp:%H-%M-%S}.json"
        )

        self.put_json(object_key, data)
        return object_key


def store_raw(
    datasets: dict[str, Any],
    storage: ObjectStorage | None = None,
    timestamp: datetime | None = None,
) -> dict[str, str]:
    """Store all extracted datasets as independent raw JSON objects."""
    storage = storage or ObjectStorage()

    return {
        dataset: storage.write_dataset(
            dataset,
            data,
            timestamp=timestamp,
        )
        for dataset, data in datasets.items()
    }
