## ORDERS (ENDPOINT)
# WE NEED TO CALL THE FUNCTION GENERATOR TO MAKE AN ENDPOINT FOR AIRFLOW
from fastapi import APIRouter
import logging,time
logger = logging.getLogger(__name__)
#from dateutil.relativedelta import relativedelta
from mock_api.generation.order_and_delivery import generator_orders
from typing import List
from mock_api.models.orders import Order
router = APIRouter(tags=["Orders"])

@router.get("/orders")
def get_orders(limit:int=100)->List[Order]:
    start = time.perf_counter()
    logger.info("Starting generating orders")
    l = generator_orders(n=limit)
    logger.info(f"Program finished in: {round((time.perf_counter()-start)*1000,4)}")
    return l

