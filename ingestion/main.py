"""Entry point for one complete ingestion run.

A snapshot becomes eligible for Bronze only after every required dataset object
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

    # store_raw writes data objects, registers the manifest key in the catalog,
    # and publishes the completion manifest last.
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
    """Run ingestion, or rebuild Bronze from S3 when explicitly requested."""
    parser = argparse.ArgumentParser(description="ELT ingestion and Bronze recovery")
    parser.add_argument(
        "--restore-bronze",
        action="store_true",
        help="rebuild Bronze from cataloged S3 snapshots without calling the API",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.restore_bronze:
        restore_bronze_from_s3()
    else:
        run_ingestion()


if __name__ == "__main__":
    main()
