## ORDERS GENERATOR
# WE NEED TO MAKE -> order_id, product_id, supplier_id, planned_quantity, order_date, excepted_delivery_date
import faker, random
from datetime import date
from typing import List
import logging
from mock_api.models.orders import Order


logger = logging.getLogger(__name__)

def create_order() -> Order: # CREATE ONLY 1 ORDER
    #start = time. perf_counter()
    try:
        logging.info("Starting creating order")
        fake = faker.Faker()
        order_date:date = fake.date_between(start_date="2000-01-01",end_date="2026-01-01")
        excepted_delivery_date:date = order_date
        ran:int = random.choice([0,1,2])
        if ran == 1 and excepted_delivery_date.month == 12:
            excepted_delivery_date = excepted_delivery_date.replace(year=excepted_delivery_date.year+1)
            excepted_delivery_date = excepted_delivery_date.replace(month=1)
        if ran == 2 and excepted_delivery_date.month >= 11:
            excepted_delivery_date = excepted_delivery_date.replace(year=excepted_delivery_date.year+1)
            excepted_delivery_date = excepted_delivery_date.replace(month=(excepted_delivery_date.month+2)%12)
        if excepted_delivery_date.month == 2:
            excepted_delivery_date = excepted_delivery_date.replace(day=random.randint(1,28))
        else:
            excepted_delivery_date = excepted_delivery_date.replace(day=random.randint(1,30))
        order_id:int = random.randint(1,10**6)
        product_id:int = random.randint(1,10**6)
        supplier_id:int = random.randint(1,10**6)
        planned_quantity:int = random.randint(1,10**3)
        
        order = Order(order_id=order_id,product_id=product_id,supplier_id=supplier_id,planned_quantity=planned_quantity,order_date=order_date,excepted_delivery_date=excepted_delivery_date)
        
        return order
    except Exception as e:
        logging.error(f"Found an Error: {e}")
        raise e


def generate_orders(n:int) -> List[Order]: # GENERATE A LIST OF ORDERS
    return [create_order() for _ in range(0,n)]