## DELIVERIES
# DATA -> delivery_id, order_id, actual_quanity, actual_delivery_date
from datetime import date
from pydantic import BaseModel, Field
from mock_api.models.orders import Order
class Delivery(BaseModel):
    delivery_id:int=Field(...,description="The ID of our order")
    