## ORDERS ENDPOINT
# WE NEED TO CALL THE FUNCTION GENERATOR TO MAKE AN ENDPOINT FOR AIRFLOW
from fastapi import APIRouter
import logging,time
logger = logging.getLogger(__name__)
#from dateutil.relativedelta import relativedelta
from mock_api.generation.orders import generate_orders
from typing import List
from mock_api.models.orders import Order
router = APIRouter(tags=["Orders"])

@router.get("/orders")
def get_orders(limit:int)->List[Order]:
    start = time.perf_counter()
    logging.info("Starting generating orders")
    l = generate_orders(n=limit)
    logging.info(f"Program finished in: {round((time.perf_counter()-start)*1000,4)}")
    return l

