from datetime import date

from pydantic import BaseModel, Field


class Inventory(BaseModel):
    product_id: int = Field(
        ...,
        ge=1,
        description="The ID of the product currently tracked in the warehouse",
    )
    quantity: int = Field(
        ...,
        ge=0,
        description="Available quantity of the product in the warehouse",
    )
    snapshot_date: date = Field(
        ...,
        description="Date of the warehouse inventory snapshot",
    )
