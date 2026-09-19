import random
from calendar import monthrange
from datetime import date
from typing import Dict, List

from mock_api.models.inventory import Inventory

PRODUCT_COUNT = 500
REPLENISHMENT_AMOUNT = 20
REPLENISHMENT_DAY = 30
INVENTORY_START_DATE = date(2025, 1, 1)

_rng = random.Random(42)

# This dictionary is the source-of-truth state of the mock warehouse.
# Some products intentionally start at zero so the pipeline can detect
# out-of-stock situations.
_initial_inventory: Dict[int, int] = {
    product_id: _rng.randint(0, 50)
    for product_id in range(1, PRODUCT_COUNT + 1)
}


def _replenishment_date(year: int, month: int) -> date:
    """Return the monthly replenishment date, capped at the month's last day."""
    last_day = monthrange(year, month)[1]
    return date(year, month, min(REPLENISHMENT_DAY, last_day))


def _count_replenishments(snapshot_date: date) -> int:
    """Count monthly replenishments that happened before or on snapshot_date."""
    if snapshot_date < INVENTORY_START_DATE:
        return 0

    total = 0
    year, month = INVENTORY_START_DATE.year, INVENTORY_START_DATE.month

    while True:
        replenishment = _replenishment_date(year, month)
        if replenishment > snapshot_date:
            break

        total += 1

        if month == 12:
            year += 1
            month = 1
        else:
            month += 1

    return total


def generate_inventory(snapshot_date: date) -> List[Inventory]:
    """Generate a deterministic warehouse snapshot for the requested date."""
    replenishments = _count_replenishments(snapshot_date)

    return [
        Inventory(
            product_id=product_id,
            quantity=quantity + replenishments * REPLENISHMENT_AMOUNT,
            snapshot_date=snapshot_date,
        )
        for product_id, quantity in _initial_inventory.items()
    ]
