"""Source extraction orchestration for the ingestion layer.

The fetch layer coordinates API calls but does not inspect, transform, or
serialize the returned records. Orders and deliveries are fetched as one
logical block; the inventory snapshot is fetched independently so the two
blocks can run concurrently.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ingestion.client import APIClient


def fetch_orders_and_deliveries(
    client: APIClient,
    limit: int = 100,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch orders followed by deliveries as one logical extraction block."""
    orders = client.get_orders(limit=limit)
    deliveries = client.get_deliveries(limit=limit)

    return {
        "orders": orders,
        "deliveries": deliveries,
    }


def fetch_inventory(
    client: APIClient,
    snapshot_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch the inventory snapshot independently."""
    return {
        "inventory": client.get_inventory(snapshot_date=snapshot_date),
    }


def fetch_all(
    client: APIClient | None = None,
    limit: int = 100,
    snapshot_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch all source datasets using the ingestion concurrency model."""
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
