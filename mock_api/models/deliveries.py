## DELIVERIES
# Pydantic schema for a delivery.
from datetime import date

from pydantic import BaseModel, Field


class Delivery(BaseModel):
    delivery_id: str = Field(
        ..., min_length=1, description="The ID of the delivery"
    )
    order_id: str = Field(..., min_length=1, description="The ID of the related order")
    actual_quantity: int = Field(
        ..., ge=0, description="The actual amount received"
    )
    actual_delivery_date: date = Field(
        ..., description="The actual delivery date"
    )
