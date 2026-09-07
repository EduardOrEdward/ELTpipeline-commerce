## DELIVERY
# WE NEED TO MAKE -> delivery_id, order_id, actual_quanity, actual_delivery_date 
from mock_api.models.deliveries import Delivery
import faker
import logging, time
from typing import List

def create_delivery() -> Delivery:
    


def generate_deliveries(n:int)->List[Delivery]:
    logging.info("Initializing generation of orders")
    return [create_delivery() for _ in range(0,n)]