## ORDERS
# Pydantic schema for an order.
from datetime import date

from pydantic import BaseModel, Field


class Order(BaseModel):
    order_id: str = Field(..., min_length=1, description="The ID of the order")
    product_id: int = Field(..., ge=1, description="The ID of ordered product")
    supplier_id: int = Field(..., ge=1, description="The ID of supplier")
    planned_quantity: int = Field(
        ..., ge=1, description="The amount planned to be received"
    )
    order_date: date = Field(..., description="The date of the order")
    expected_delivery_date: date = Field(
        ..., description="The expected delivery date"
    )
