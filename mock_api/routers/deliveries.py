## DELIVERIES
# We need to make a sythetic data delivery_id, order_id, actual_quanity, actual_delivery_date
from fastapi import APIRouter
import faker
from mock_api.models.deliveries import Delivery
from typing import Optional, List
from fastapi import APIRouter
router = APIRouter(tags=['Delivery'])

@router.get(response_model=List[Delivery],description="The get method to get our deliveries(with same order_id)")
def get_delivery(limit:int=100):

