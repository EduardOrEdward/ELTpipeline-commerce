"""Contract tests for complete source API responses."""

import unittest

from ingestion.fetch import EXPECTED_INVENTORY_RECORDS, fetch_all


class FakeAPIClient:
    def __init__(
        self,
        orders_count=3,
        deliveries_count=3,
        inventory_count=EXPECTED_INVENTORY_RECORDS,
        orders_payload=None,
        deliveries_payload=None,
        inventory_payload=None,
    ):
        self.orders_count = orders_count
        self.deliveries_count = deliveries_count
        self.inventory_count = inventory_count
        self.orders_payload = orders_payload
        self.deliveries_payload = deliveries_payload
        self.inventory_payload = inventory_payload

    def get_orders(self, limit=100):
        if self.orders_payload is not None:
            return self.orders_payload
        return [{"order_id": index} for index in range(self.orders_count)]

    def get_deliveries(self, limit=100):
        if self.deliveries_payload is not None:
            return self.deliveries_payload
        return [{"delivery_id": index} for index in range(self.deliveries_count)]

    def get_inventory(self, snapshot_date=None):
        if self.inventory_payload is not None:
            return self.inventory_payload
        return [{"product_id": index} for index in range(self.inventory_count)]


class FetchContractTests(unittest.TestCase):
    def test_accepts_all_complete_source_responses(self):
        client = FakeAPIClient()
        result = fetch_all(client=client, limit=3)

        self.assertEqual(set(result), {"orders", "deliveries", "inventory"})
        self.assertEqual(len(result["orders"]), 3)
        self.assertEqual(len(result["deliveries"]), 3)
        self.assertEqual(len(result["inventory"]), EXPECTED_INVENTORY_RECORDS)

    def test_rejects_truncated_orders_response(self):
        with self.assertRaisesRegex(
            ValueError,
            r"orders: expected 3 records, received 2",
        ):
            fetch_all(client=FakeAPIClient(orders_count=2), limit=3)

    def test_rejects_truncated_deliveries_response(self):
        with self.assertRaisesRegex(
            ValueError,
            r"deliveries: expected 3 records, received 2",
        ):
            fetch_all(client=FakeAPIClient(deliveries_count=2), limit=3)

    def test_rejects_truncated_inventory_response(self):
        with self.assertRaisesRegex(
            ValueError,
            rf"inventory: expected {EXPECTED_INVENTORY_RECORDS} records, received 499",
        ):
            fetch_all(
                client=FakeAPIClient(inventory_count=EXPECTED_INVENTORY_RECORDS - 1),
                limit=3,
            )

    def test_rejects_non_array_response(self):
        with self.assertRaisesRegex(ValueError, "orders: expected a JSON array"):
            fetch_all(
                client=FakeAPIClient(orders_payload={"order_id": 1}),
                limit=3,
            )

    def test_rejects_non_object_record(self):
        with self.assertRaisesRegex(
            ValueError,
            "every record must be a JSON object",
        ):
            fetch_all(
                client=FakeAPIClient(orders_payload=[{"order_id": 1}, None, {"order_id": 3}]),
                limit=3,
            )


if __name__ == "__main__":
    unittest.main()
