"""Entry points for the API-to-S3 and S3-to-Bronze ingestion stages.

A snapshot becomes eligible for Bronze only after all required dataset objects
and its completion manifest have been persisted to immutable object storage.
"""

import argparse
import logging
from typing import Any
from uuid import uuid4

import psycopg
from botocore.exceptions import ClientError

from ingestion.bronze import (
    ObjectStorageReader,
    ensure_bronze_schema,
    load_snapshot_to_bronze,
)
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


def _is_not_found_error(error: ClientError) -> bool:
    """Return whether an S3 error specifically means that an object is absent."""
    response = error.response
    code = response.get("Error", {}).get("Code")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in {"NoSuchKey", "NotFound", "404"} or status == 404


def extract_to_s3(
    client: Any | None = None,
    storage: ObjectStorage | None = None,
    limit: int = 100,
    snapshot_date: str | None = None,
    snapshot_id: str | None = None,
) -> dict[str, Any]:
    """Fetch a complete API package and publish it as one completed S3 snapshot.

    This stage has no PostgreSQL dependency. The returned snapshot ID is the
    hand-off to the S3-to-Bronze task, which can retry without calling the API.
    """
    logger.info("Starting API-to-S3 extraction")
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

    snapshot_id = snapshot_id or uuid4().hex
    writer = storage or ObjectStorage()
    object_keys_by_dataset = store_raw(
        datasets,
        storage=writer,
        snapshot_id=snapshot_id,
    )
    manifest_key = manifest_key_for_snapshot(snapshot_id)
    logger.info(
        "Published completed S3 snapshot %s with manifest %s",
        snapshot_id,
        manifest_key,
    )
    return {
        "snapshot_id": snapshot_id,
        "manifest_key": manifest_key,
        "object_keys": object_keys_by_dataset,
    }


def load_bronze_from_s3(
    snapshot_id: str,
    storage_reader: ObjectStorageReader | None = None,
) -> dict[str, Any]:
    """Load one completed S3 snapshot into Bronze without calling the source API."""
    if not POSTGRES_PASSWORD:
        raise RuntimeError("POSTGRES_PASSWORD must be set for a Bronze load")
    if not snapshot_id:
        raise ValueError("snapshot_id must be a non-empty string")

    reader = storage_reader or ObjectStorageReader()
    manifest_key = manifest_key_for_snapshot(snapshot_id)
    manifest = reader.get_json(manifest_key)
    if not isinstance(manifest, dict):
        raise ValueError(f"Invalid snapshot manifest: {manifest_key}")

    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict) or set(datasets) != REQUIRED_DATASETS:
        raise ValueError(
            "Snapshot manifest must contain orders, deliveries, and inventory"
        )

    raw_objects = {}
    for dataset, details in datasets.items():
        if not isinstance(details, dict) or not isinstance(details.get("object_key"), str):
            raise ValueError(f"Invalid dataset entry in manifest: {manifest_key}")
        object_key = details["object_key"]
        raw_objects[object_key] = reader.get_json(object_key)

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
        "object_keys": {
            dataset: details["object_key"]
            for dataset, details in datasets.items()
        },
        "bronze_rows": bronze_result["bronze_rows"],
        "already_loaded": bronze_result["already_loaded"],
    }


def run_ingestion(
    client: Any | None = None,
    storage: ObjectStorage | None = None,
    storage_reader: ObjectStorageReader | None = None,
    limit: int = 100,
    snapshot_date: str | None = None,
) -> dict[str, Any]:
    """Run both ingestion stages for local use and backward compatibility."""
    if not POSTGRES_PASSWORD:
        raise RuntimeError("POSTGRES_PASSWORD must be set for an ingestion run")

    snapshot = extract_to_s3(
        client=client,
        storage=storage,
        limit=limit,
        snapshot_date=snapshot_date,
    )
    bronze_result = load_bronze_from_s3(
        snapshot["snapshot_id"],
        storage_reader=storage_reader,
    )
    return {**snapshot, **{
        "bronze_rows": bronze_result["bronze_rows"],
        "already_loaded": bronze_result["already_loaded"],
    }}


def restore_bronze_from_s3(
    storage_reader: ObjectStorageReader | None = None,
) -> dict[str, int]:
    """Restore Bronze from cataloged completed snapshots without calling the API.

    The catalog is an index, not the completion marker. If a process failed
    after registering a snapshot but before writing its manifest, the catalog
    entry is stale and is safely skipped.
    """
    if not POSTGRES_PASSWORD:
        raise RuntimeError("POSTGRES_PASSWORD must be set for a Bronze restore")

    reader = storage_reader or ObjectStorageReader()
    manifest_keys = reader.list_manifest_keys()
    if not manifest_keys:
        raise RuntimeError(
            "No snapshot manifests found in the S3 catalog; Bronze cannot be restored"
        )

    connection_config = {
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "dbname": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }
    loaded = 0
    skipped = 0

    with psycopg.connect(**connection_config) as connection:
        ensure_bronze_schema(connection)
        connection.commit()

        for manifest_key in manifest_keys:
            try:
                manifest = reader.get_json(manifest_key)
            except ClientError as error:
                if _is_not_found_error(error):
                    logger.warning(
                        "Skipping catalog entry whose manifest is not yet present: %s",
                        manifest_key,
                    )
                    skipped += 1
                    continue
                raise

            if not isinstance(manifest, dict):
                raise ValueError(f"Invalid snapshot manifest: {manifest_key}")
            if manifest.get("status") != "COMPLETED":
                logger.warning("Skipping non-completed snapshot manifest: %s", manifest_key)
                skipped += 1
                continue

            snapshot_id = manifest.get("snapshot_id")
            datasets = manifest.get("datasets")
            if not isinstance(snapshot_id, str) or not isinstance(datasets, dict):
                raise ValueError(f"Invalid snapshot manifest: {manifest_key}")

            raw_objects = {}
            for details in datasets.values():
                if not isinstance(details, dict) or not isinstance(details.get("object_key"), str):
                    raise ValueError(f"Invalid dataset entry in manifest: {manifest_key}")
                object_key = details["object_key"]
                raw_objects[object_key] = reader.get_json(object_key)

            result = load_snapshot_to_bronze(
                connection,
                snapshot_id=snapshot_id,
                manifest_key=manifest_key,
                manifest=manifest,
                raw_objects=raw_objects,
                ensure_schema=False,
            )
            if result["already_loaded"]:
                skipped += 1
            else:
                loaded += 1

    logger.info("Bronze restore finished: loaded=%s skipped=%s", loaded, skipped)
    return {"loaded_snapshots": loaded, "skipped_snapshots": skipped}


def main() -> None:
    """Run the full pipeline or one explicitly selected ingestion stage."""
    parser = argparse.ArgumentParser(description="ELT ingestion stages and Bronze recovery")
    stages = parser.add_mutually_exclusive_group()
    stages.add_argument(
        "--extract-to-s3",
        action="store_true",
        help="fetch all source datasets and publish one completed S3 snapshot",
    )
    stages.add_argument(
        "--load-bronze",
        metavar="SNAPSHOT_ID",
        help="load an existing completed S3 snapshot into Bronze without calling the API",
    )
    stages.add_argument(
        "--restore-bronze",
        action="store_true",
        help="rebuild Bronze from cataloged S3 snapshots without calling the API",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.extract_to_s3:
        result = extract_to_s3()
        # DockerOperator uses the final stdout line as XCom for the next task.
        print(result["snapshot_id"])
    elif args.load_bronze:
        load_bronze_from_s3(args.load_bronze)
    elif args.restore_bronze:
        restore_bronze_from_s3()
    else:
        run_ingestion()


if __name__ == "__main__":
    main()
