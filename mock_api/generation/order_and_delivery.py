## ORDERS GENERATOR
# WE NEED TO MAKE -> order_id, product_id, supplier_id, planned_quantity, order_date, expected_delivery_date

## DELIVERIES
# DATA -> delivery_id, order_id, actual_quanity, actual_delivery_date


import faker, random
from datetime import date,timedelta
import logging
from typing import List,Tuple
from mock_api.models.deliveries import Delivery
from mock_api.models.orders import Order
logger = logging.getLogger(__name__)
def create_order_and_delivery()-> Tuple[Order,Delivery]:
    logger.info("Initializa generation of order and delivery")
    try:
        fake = faker.Faker()
        # ORDER GENERATION START
        order_date:date = fake.date_between(start_date="2000-01-01",end_date="2026-01-01")
        expected_delivery_date:date = order_date
        ran:int = random.choice([0,1,2])
        if ran == 1 and expected_delivery_date.month == 12:
                expected_delivery_date = expected_delivery_date.replace(year=expected_delivery_date.year+1)
                expected_delivery_date = expected_delivery_date.replace(month=1)
        if ran == 2 and expected_delivery_date.month >= 11:
                expected_delivery_date = expected_delivery_date.replace(year=expected_delivery_date.year+1)
                expected_delivery_date = expected_delivery_date.replace(month=(expected_delivery_date.month+2)%12)
        if expected_delivery_date.month == 2:
                expected_delivery_date = expected_delivery_date.replace(day=random.randint(1,28))
        else:
                expected_delivery_date = expected_delivery_date.replace(day=random.randint(1,30))
        order_id:str = fake.uuid4()
        product_id:int = random.randint(1,10**3)
        supplier_id:int = random.randint(1,10**4)
        planned_quantity:int = random.randint(1,10**3)
        
        order:Order = Order(order_id=order_id,product_id=product_id,supplier_id=supplier_id,planned_quantity=planned_quantity,order_date=order_date,expected_delivery_date=expected_delivery_date)
        # ORDER GENERATION ENDS
        
        # DELIVERY GENERATION STARTS
        delivery_id:str = fake.uuid4()
        actual_quantity:int = random.randint(planned_quantity-1,planned_quantity+1)
        if expected_delivery_date > order_date+timedelta(weeks=2): 
            start:date = expected_delivery_date - random.choice([timedelta(weeks=random.randint(0,2)),timedelta()])
        else:
            start:date = expected_delivery_date - random.choice([timedelta(weeks=random.randint(0,1)),timedelta()])
        end:date = expected_delivery_date + random.choice([timedelta(weeks=random.randint(0,2)),timedelta()])
        actual_delivery_date:date=fake.date_between_dates(start,end)
        delivery = Delivery(delivery_id=delivery_id,order_id=order_id,actual_quantity=actual_quantity,actual_delivery_date=actual_delivery_date)
        
        
        return order, delivery
    except Exception as e:
        logger.error(f"An error accured: {e}")
        raise e


def generator_orders_deliveries(n:int) -> List[Tuple[Order,Delivery]]:
    return [create_order_and_delivery() for _ in range(0,n)]