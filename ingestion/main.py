"""Entry point for one complete ingestion run.

The main module orchestrates the ingestion stages only. It does not implement
HTTP, object-storage, or database logic itself.
"""

import logging
from typing import Any

import psycopg

from ingestion.bronze import ObjectStorageReader, load_objects_to_bronze
from ingestion.client import APIClient
from ingestion.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from ingestion.fetch import fetch_all
from ingestion.storage import ObjectStorage, store_raw

logger = logging.getLogger(__name__)


def run_ingestion(
    client: APIClient | None = None,
    storage: ObjectStorage | None = None,
    storage_reader: ObjectStorageReader | None = None,
    limit: int = 100,
    snapshot_date: str | None = None,
) -> dict[str, Any]:
    """Run extraction, raw storage, and Bronze loading in order."""
    if not POSTGRES_PASSWORD:
        raise RuntimeError("POSTGRES_PASSWORD must be set for an ingestion run")

    logger.info("Starting ingestion run")

    # 1. Extract data from the source API.
    datasets = fetch_all(
        client=client,
        limit=limit,
        snapshot_date=snapshot_date,
    )
    logger.info("Fetched datasets: %s", ", ".join(datasets))

    # 2. Persist the untouched source data in object storage.
    object_keys_by_dataset = store_raw(
        datasets,
        storage=storage,
    )
    logger.info("Stored raw datasets in object storage")

    # 3. Read the raw objects back from object storage.
    reader = storage_reader or ObjectStorageReader()
    raw_objects = {
        object_key: reader.get_json(object_key)
        for object_key in object_keys_by_dataset.values()
    }

    # 4. Load the exact raw payloads into PostgreSQL Bronze.
    connection_config = {
        "host": POSTGRES_HOST,
        "port": POSTGRES_PORT,
        "dbname": POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
    }

    with psycopg.connect(**connection_config) as connection:
        loaded_rows = load_objects_to_bronze(
            connection,
            raw_objects,
        )

    logger.info("Bronze loading completed: %s", loaded_rows)

    return {
        "object_keys": object_keys_by_dataset,
        "bronze_rows": loaded_rows,
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
