"""Raw object storage for the ingestion layer.

This module serializes extracted Python data and writes it to the S3-compatible
SeaweedFS endpoint. It deliberately does not transform or validate records.
"""

from datetime import datetime, timezone
import json
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ingestion.config import S3_BUCKET, S3_ENDPOINT


class ObjectStorage:
    """Minimal S3-compatible object writer for SeaweedFS."""

    def __init__(
        self,
        endpoint: str = S3_ENDPOINT,
        bucket: str = S3_BUCKET,
        timeout: int = 10,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.bucket = bucket
        self.timeout = timeout

    def _url(self, object_key: str) -> str:
        return f"{self.endpoint}/{self.bucket}/{object_key.lstrip('/')}"

    def put_json(self, object_key: str, data: Any) -> None:
        """Serialize data as JSON and store it under the given object key."""
        payload = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        request = Request(
            self._url(object_key),
            data=payload,
            headers={
                "Content-Type": "application/json",
            },
            method="PUT",
        )

        with urlopen(request, timeout=self.timeout):
            return

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
