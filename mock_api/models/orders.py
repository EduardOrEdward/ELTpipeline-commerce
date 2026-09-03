## ORDERS
# Here we construct order pydantic scheme
## DATA -> order_id, product_id, supplier_id, planned_quanity, order_date, excepted_delivery_date
from pydantic import BaseModel, Field
from datetime import date
class Order(BaseModel):
    order_id:int=Field(...,ge=1,description="The ID of the order")
    product_id:int =Field(...,ge=1,description="The ID of ordered product")
    supplier_id:int=Field(...,ge=1,description="The ID of supplier, the one we order from")
    planned_quanity:int=Field(...,ge=1,description="The amount we planned to get after delivery")
    order_date:date=Field(...,description="The date of our order")
    excepted_delivery_date:date=Field(...,description="The date we except our order to be delivered")

