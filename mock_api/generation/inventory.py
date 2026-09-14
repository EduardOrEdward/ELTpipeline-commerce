## INVENTORY(ENDPOINT)
# WE NEED -> QUANTITY, TODAY_QUANTITY, SNAPSHOT_DATE

import faker, random
import logging
from mock_api.generation.order_and_delivery import create_order_and_delivery
from mock_api.models.inventory import Inventory
from typing import Dict
from datetime import date, timedelta

logger = logging.getLogger(__name__)

def create_inventory()->Dict[]:
    logger.info("Starting the creation of our Inventory")
    try:
        
    except Exception as e:
        logger.error(f"An error accured: {e}")
        raise e