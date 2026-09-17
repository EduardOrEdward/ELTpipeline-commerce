## ORDERS ENDPOINT
from time import perf_counter
from typing import List

from fastapi import APIRouter, Query

from mock_api.generation.order_and_delivery import generator_orders
from mock_api.models.orders import Order

import logging

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Orders"])


@router.get("/orders", response_model=List[Order])
async def get_orders(
    limit: int = Query(default=100, ge=1, le=10_000),
) -> List[Order]:
    start = perf_counter()
    logger.info("Starting generating orders")

    orders = generator_orders(n=limit)

    logger.info("Program finished in %.4f ms", (perf_counter() - start) * 1000)
    return orders
