"""Source extraction orchestration and completeness checks for ingestion.

The fetch layer coordinates API calls but does not transform business records.
It verifies the source contract: orders/deliveries return the requested limit,
and the unfiltered inventory endpoint returns the complete 500-product snapshot.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ingestion.client import APIClient

EXPECTED_INVENTORY_RECORDS = 500


def _require_complete_response(
    dataset: str,
    records: Any,
    expected_count: int,
) -> list[dict[str, Any]]:
    """Reject a malformed or truncated response before it can reach S3."""
    if not isinstance(records, list):
        raise ValueError(
            f"Incomplete API response for {dataset}: expected a JSON array"
        )
    if len(records) != expected_count:
        raise ValueError(
            f"Incomplete API response for {dataset}: "
            f"expected {expected_count} records, received {len(records)}"
        )
    if any(not isinstance(record, dict) for record in records):
        raise ValueError(
            f"Invalid API response for {dataset}: every record must be a JSON object"
        )
    return records


def fetch_orders_and_deliveries(
    client: APIClient,
    limit: int = 100,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch and validate orders followed by deliveries as one logical block."""
    orders = _require_complete_response(
        "orders",
        client.get_orders(limit=limit),
        expected_count=limit,
    )
    deliveries = _require_complete_response(
        "deliveries",
        client.get_deliveries(limit=limit),
        expected_count=limit,
    )

    return {
        "orders": orders,
        "deliveries": deliveries,
    }


def fetch_inventory(
    client: APIClient,
    snapshot_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch and validate the complete, unfiltered inventory snapshot."""
    inventory = _require_complete_response(
        "inventory",
        client.get_inventory(snapshot_date=snapshot_date),
        expected_count=EXPECTED_INVENTORY_RECORDS,
    )
    return {"inventory": inventory}


def fetch_all(
    client: APIClient | None = None,
    limit: int = 100,
    snapshot_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch all source datasets using the ingestion concurrency model."""
    if limit < 1:
        raise ValueError("limit must be greater than 0")

    client = client or APIClient()

    with ThreadPoolExecutor(max_workers=2) as executor:
        orders_and_deliveries_future = executor.submit(
            fetch_orders_and_deliveries,
            client,
            limit,
        )
        inventory_future = executor.submit(
            fetch_inventory,
            client,
            snapshot_date,
        )

        result: dict[str, list[dict[str, Any]]] = {}
        result.update(orders_and_deliveries_future.result())
        result.update(inventory_future.result())

    return result
