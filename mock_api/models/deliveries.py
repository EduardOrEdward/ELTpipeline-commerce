## DELIVERIES
# DATA -> delivery_id, order_id, actual_quanity, actual_delivery_date
from datetime import date
from pydantic import BaseModel, Field
#from mock_api.models.orders import Order
class Delivery(BaseModel):
    delivery_id:int=Field(...,description="The ID of our delivery of the order")
    order_id:int=Field(...,description="The ID of our order")
    actual_quantity:int=Field(...,description="The real amount of the thing we ordered")
    actual_delivery_date:date=Field(...,description="The date we actual get the order")