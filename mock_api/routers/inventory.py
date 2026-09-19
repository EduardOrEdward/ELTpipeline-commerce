from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Query

from mock_api.generators.inventory import generate_inventory
from mock_api.models.inventory import Inventory

router = APIRouter(tags=["Inventory"])


@router.get("/inventory", response_model=List[Inventory])
async def get_inventory(
    snapshot_date: Optional[date] = Query(
        default=None,
        description="Warehouse snapshot date. Defaults to today.",
    ),
    product_id: Optional[int] = Query(
        default=None,
        ge=1,
        description="Optional product ID filter.",
    ),
) -> List[Inventory]:
    snapshot_date = snapshot_date or date.today()
    inventory = generate_inventory(snapshot_date)

    if product_id is not None:
        inventory = [
            item for item in inventory if item.product_id == product_id
        ]

    return inventory
