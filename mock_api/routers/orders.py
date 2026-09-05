## ORDERS
# We need to make syntethic of order_id, product_id, supplier_id, planned_quanity, order_date, excepted_delivery_date
from fastapi import APIRouter
import faker, random
from typing import Any, Optional,List
import logging,time
from mock_api.models.orders import Order
logger = logging.getLogger(__name__)

router = APIRouter(tags=["Orders"])

@router.post("/make_order",response_model=List[Order],summary="Sending request to make a order")
def get_order(n:int) -> List[Order]:
    logging.info("Initializing the creation of data into List of objects(Order)")
    start = time.perf_counter()
    try:
        l:List[Order] = []
        for i in range(0,n):
            date = faker.providers.date_time.date_between(start_date="01-01-2000",end_date="01-01-2026")
            ends = date
            ends.year = ends.year + 3
            end_date = faker.providers.date_time.date_between(start_date=date,end_date=ends)
            order = Order(order_id=random.randint(1,10**6),product_id=random.randint(1,10**6),supplier_id=random.randint(1,10**6),planned_quanity=random.randint(1,10**3),order_date=date,excepted_delivery_date=faker.providers.date_time.data_between(start_date=date,end_date=end_date))
        
            l.append(order)
        logger.info(f"Everything succeed in {round((time.perf_counter()-start)*1000,4)}")
        return l
    except Exception as e:
        logging.error(f"Initialization failed due to error: {e}")
        raise e