## DELIVERIES (ENDPOINT)
# WE NEED TO CALL A FUNCTION GENERATOR FOR AIRFLOW 
from mock_api.generation.order_and_delivery import generator_deliveries
import logging, time
from mock_api.models.deliveries import Delivery
from typing import List
from fastapi import APIRouter

router = APIRouter(tags=['Delivery'])
logger = logging.getLogger(__name__)
@router.get("/delivery",description="The get method to get our deliveries(with same order_id)")
def get_delivery(limit:int=100) -> List[Delivery]:
    start = time.perf_counter()
    logger.info("Starting generating deliveries")
    l = generator_deliveries(n=limit)
    logger.info(f"Prgoram finished in: {round((time.perf_counter()-start)*1000,4)}")
    return l
