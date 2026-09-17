## DELIVERIES ENDPOINT
from time import perf_counter
from typing import List

from fastapi import APIRouter, Query

from mock_api.generation.order_and_delivery import generator_deliveries
from mock_api.models.deliveries import Delivery

import logging

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Delivery"])


@router.get(
    "/delivery",
    description="Get deliveries linked to orders by order_id",
    response_model=List[Delivery],
)
async def get_delivery(
    limit: int = Query(default=100, ge=1, le=10_000),
) -> List[Delivery]:
    start = perf_counter()
    logger.info("Starting generating deliveries")

    deliveries = generator_deliveries(n=limit)

    logger.info("Program finished in %.4f ms", (perf_counter() - start) * 1000)
    return deliveries
