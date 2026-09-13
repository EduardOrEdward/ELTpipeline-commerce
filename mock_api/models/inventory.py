## INVENTORY
# DATA -> product_id, quantity
from pydantic import Field, BaseModel
#from mock_api.models.orders
from datetime import date
class Inventory(BaseModel):
    product_id:int=Field(...,description="The ID of the current product on warehouse")
    quantity:int=Field(...,description="The REAL quantity on warehouse")
    snapshot_date:date=Field(...,description="The moment we took a snapshot of our warehouse(basically the moment of last check)")
    