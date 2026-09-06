## INVENTORY
# DATA -> product_id, quantity
from pydantic import Field, BaseModel
#from mock_api.models.orders

class Inventory(BaseModel):
    