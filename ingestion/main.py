"""Entry point for one complete ingestion run.

A snapshot becomes eligible for Bronze only after every required dataset object
and its completion manifest have been persisted to immutable object storage.
"""

import logging
from typing import Any
from uuid import uuid4

import psycopg

from ingestion.bronze import ObjectStorageReader, load_snapshot_to_bronze
from ingestion.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from ingestion.fetch import fetch_all
from ingestion.storage import (
    ObjectStorage,
    manifest_key_for_snapshot,
    store_raw,
)

logger = logging.getLogger(__name__)

REQUIRED_DATASETS = {"orders", "deliveries", "inventory"}


def run_ingestion(
    client: Any | None = None,
    storage: ObjectStorage | None = None,
    storage_reader: ObjectStorageReader | None = None,
    limit: int = 100,
    snapshot_date: str | None = None,
) -> dict[str, Any]:
    """Extract, persist an immutable snapshot, then load it into Bronze."""
    if not POSTGRES_PASSWORD:
        raise RuntimeError("POSTGRES_PASSWORD must be set for an ingestion run")

    logger.info("Starting ingestion run")
    datasets = fetch_all(
        client=client,
        limit=limit,
        snapshot_date=snapshot_date,
    )
    if set(datasets) != REQUIRED_DATASETS:
        raise RuntimeError(
            "Incomplete extraction: expected datasets "
            f"{sorted(REQUIRED_DATASETS)}, received {sorted(datasets)}"
        )
    logger.info("Fetched all required datasets: %s", ", ".join(sorted(datasets)))

    snapshot_id = uuid4().hex
    writer = storage or ObjectStorage()

    # The manifest is written last. If any data-object write fails, this call
    # raises and the partial snapshot is never eligible for Bronze.
    object_keys_by_dataset = store_raw(
        datasets,
        storage=writer,
        snapshot_id=snapshot_id,
    )
    manifest_key = manifest_key_for_snapshot(snapshot_id)
    logger.info("Stored raw snapshot %s and its completion manifest", snapshot_id)

    # Read back exactly what was persisted; Bronze must reflect S3, not an
    # in-memory API response that could differ from the stored source.
    reader = storage_reader or ObjectStorageReader()
    manifest = reader.get_json(manifest_key)
    raw_objects = {
        details["object_key"]: reader.get_json(details["object_key"])
        for details in manifest.get("datasets", {}).values()
    }

    connection_config = {
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "dbname": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }

    with psycopg.connect(**connection_config) as connection:
        bronze_result = load_snapshot_to_bronze(
            connection,
            snapshot_id=snapshot_id,
            manifest_key=manifest_key,
            manifest=manifest,
            raw_objects=raw_objects,
        )

    logger.info(
        "Bronze loading completed for snapshot %s: %s",
        snapshot_id,
        bronze_result,
    )
    return {
        "snapshot_id": snapshot_id,
        "manifest_key": manifest_key,
        "object_keys": object_keys_by_dataset,
        "bronze_rows": bronze_result["bronze_rows"],
        "already_loaded": bronze_result["already_loaded"],
    }


def main() -> None:
    """Run one ingestion job from the command line."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_ingestion()


if __name__ == "__main__":
    main()
