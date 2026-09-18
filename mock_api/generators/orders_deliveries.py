## ORDERS AND DELIVERIES GENERATOR
# Orders and deliveries are generated as pairs so that order_id is consistent.

import logging
import random
from datetime import date, timedelta
from typing import List, Tuple
from uuid import uuid4

from faker import Faker

from mock_api.models.deliveries import Delivery
from mock_api.models.orders import Order

logger = logging.getLogger(__name__)
fake = Faker()

_orders: List[Order] = []
_deliveries: List[Delivery] = []


def _new_id() -> str:
    return uuid4().hex[:8]


def create_order_and_delivery() -> Tuple[Order, Delivery]:
    """Create one logically consistent order/delivery pair."""
    logger.info("Initializing generation of order and delivery")

    order_date: date = fake.date_between(
        start_date="2000-01-01",
        end_date="2025-12-31",
    )
    expected_delivery_date = order_date + timedelta(days=random.randint(1, 30))

    order = Order(
        order_id=_new_id(),
        product_id=random.randint(1, 500),
        supplier_id=random.randint(1, 800),
        planned_quantity=random.randint(1, 100),
        order_date=order_date,
        expected_delivery_date=expected_delivery_date,
    )

    actual_quantity = max(0, order.planned_quantity + random.choice([-1, 0, 1]))
    actual_delivery_date = expected_delivery_date + timedelta(
        days=random.randint(-7, 14)
    )

    delivery = Delivery(
        delivery_id=_new_id(),
        order_id=order.order_id,
        actual_quantity=actual_quantity,
        actual_delivery_date=actual_delivery_date,
    )

    return order, delivery


def _ensure_dataset(n: int) -> None:
    """Generate records until the shared in-memory dataset has n pairs."""
    if n < 1:
        raise ValueError("n must be greater than 0")

    while len(_orders) < n:
        order, delivery = create_order_and_delivery()
        _orders.append(order)
        _deliveries.append(delivery)


def generator_orders(n: int) -> List[Order]:
    _ensure_dataset(n)
    return _orders[:n]


def generator_deliveries(n: int) -> List[Delivery]:
    _ensure_dataset(n)
    return _deliveries[:n]