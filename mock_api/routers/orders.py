## ORDERS
# We need to make syntethic of order_id, product_id, supplier_id, planned_quanity, order_date, excepted_delivery_date
from fastapi import APIRouter,Request
import faker
from typing import Any, Optional,Dict
import logging,time
from mock_api.models.orders import Order
logger = logging.getLogger(__name__)

router = APIRouter(tags=["Orders"])

@router.post("/make_order",response_model=Order,summary="Sending request to make a order")
def make_order(request:Request,) -> Dict[Order]:
    start = time.perf_counter()
    pass