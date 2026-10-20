"""Unit tests for publishing complete immutable raw snapshots."""

import unittest
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from ingestion.storage import (
    REQUIRED_DATASETS,
    ObjectStorage,
    manifest_key_for_snapshot,
    store_raw,
)


class FakeStorage:
    def __init__(self, fail_dataset=None, fail_catalog=False):
        self.fail_dataset = fail_dataset
        self.fail_catalog = fail_catalog
        self.events = []
        self.manifest = None
        self.catalog = []

    def write_dataset(self, dataset, data, snapshot_id, timestamp=None):
        self.events.append(("dataset", dataset))
        if dataset == self.fail_dataset:
            raise RuntimeError(f"simulated write failure for {dataset}")
        return f"{dataset}/2026-10-12/{snapshot_id}.json"

    def register_snapshot(self, snapshot_id, manifest_key, timestamp=None):
        self.events.append(("catalog", snapshot_id))
        if self.fail_catalog:
            raise RuntimeError("simulated catalog write failure")
        self.catalog.append({
            "snapshot_id": snapshot_id,
            "manifest_key": manifest_key,
        })

    def write_manifest(
        self,
        snapshot_id,
        object_keys_by_dataset,
        datasets,
        timestamp=None,
    ):
        self.events.append(("manifest", snapshot_id))
        self.manifest = {
            "snapshot_id": snapshot_id,
            "object_keys": dict(object_keys_by_dataset),
            "record_counts": {
                name: len(records) for name, records in datasets.items()
            },
        }
        return f"snapshots/{snapshot_id}/manifest.json"


class StoreRawTests(unittest.TestCase):
    def setUp(self):
        self.datasets = {
            "orders": [{"order_id": 1}],
            "deliveries": [{"delivery_id": 1}],
            "inventory": [{"product_id": 1, "quantity": 5}],
        }
        self.timestamp = datetime(2026, 10, 12, tzinfo=timezone.utc)

    def test_catalog_is_registered_and_manifest_is_published_last(self):
        storage = FakeStorage()
        object_keys = store_raw(
            self.datasets,
            storage=storage,
            timestamp=self.timestamp,
            snapshot_id="snapshot-test",
        )

        self.assertEqual(
            [event[0] for event in storage.events],
            ["dataset", "dataset", "dataset", "catalog", "manifest"],
        )
        self.assertEqual(set(object_keys), set(self.datasets))
        self.assertEqual(set(object_keys), REQUIRED_DATASETS)
        self.assertEqual(storage.manifest["snapshot_id"], "snapshot-test")
        self.assertEqual(storage.manifest["object_keys"], object_keys)
        self.assertEqual(
            storage.manifest["record_counts"],
            {"orders": 1, "deliveries": 1, "inventory": 1},
        )
        self.assertEqual(
            storage.catalog,
            [{
                "snapshot_id": "snapshot-test",
                "manifest_key": manifest_key_for_snapshot("snapshot-test"),
            }],
        )


    def test_s3_put_error_is_propagated_instead_of_treated_as_success(self):
        class FailingS3Client:
            def put_object(self, **kwargs):
                raise ClientError(
                    {
                        "Error": {
                            "Code": "InternalError",
                            "Message": "simulated S3 write failure",
                        },
                        "ResponseMetadata": {"HTTPStatusCode": 500},
                    },
                    "PutObject",
                )

        storage = ObjectStorage.__new__(ObjectStorage)
        storage.bucket = "elt-raw"
        storage.client = FailingS3Client()

        with self.assertRaisesRegex(ClientError, "simulated S3 write failure"):
            storage.put_json("orders/2026-10-12/snapshot-test.json", [{"order_id": 1}])

    def test_each_missing_required_dataset_is_rejected_before_any_write(self):
        for missing_dataset in REQUIRED_DATASETS:
            with self.subTest(missing_dataset=missing_dataset):
                storage = FakeStorage()
                incomplete = {
                    name: records
                    for name, records in self.datasets.items()
                    if name != missing_dataset
                }

                with self.assertRaisesRegex(ValueError, "Incomplete snapshot package"):
                    store_raw(
                        incomplete,
                        storage=storage,
                        timestamp=self.timestamp,
                        snapshot_id="snapshot-incomplete",
                    )

                self.assertEqual(storage.events, [])
                self.assertEqual(storage.catalog, [])
                self.assertIsNone(storage.manifest)

    def test_unexpected_dataset_is_rejected_before_any_write(self):
        storage = FakeStorage()
        invalid = {**self.datasets, "customers": [{"customer_id": 1}]}

        with self.assertRaisesRegex(ValueError, "unexpected datasets: customers"):
            store_raw(invalid, storage=storage, snapshot_id="snapshot-unexpected")

        self.assertEqual(storage.events, [])

    def test_non_object_record_is_rejected_before_any_write(self):
        storage = FakeStorage()
        invalid = {**self.datasets, "orders": [{"order_id": 1}, None]}

        with self.assertRaisesRegex(ValueError, "only JSON objects"):
            store_raw(invalid, storage=storage, snapshot_id="snapshot-invalid-record")

        self.assertEqual(storage.events, [])

    def test_failed_dataset_write_does_not_register_or_publish_snapshot(self):
        storage = FakeStorage(fail_dataset="deliveries")

        with self.assertRaisesRegex(RuntimeError, "simulated write failure"):
            store_raw(
                self.datasets,
                storage=storage,
                timestamp=self.timestamp,
                snapshot_id="snapshot-partial",
            )

        self.assertIsNone(storage.manifest)
        self.assertEqual(storage.catalog, [])
        self.assertNotIn("catalog", [event[0] for event in storage.events])
        self.assertNotIn("manifest", [event[0] for event in storage.events])

    def test_failed_catalog_write_does_not_publish_manifest(self):
        storage = FakeStorage(fail_catalog=True)

        with self.assertRaisesRegex(RuntimeError, "simulated catalog write failure"):
            store_raw(
                self.datasets,
                storage=storage,
                timestamp=self.timestamp,
                snapshot_id="snapshot-catalog-failure",
            )

        self.assertIsNone(storage.manifest)
        self.assertNotIn("manifest", [event[0] for event in storage.events])

    def test_invalid_dataset_is_rejected_before_any_write(self):
        storage = FakeStorage()
        invalid_datasets = {
            "orders": [{"order_id": 1}],
            "deliveries": {"delivery_id": 1},
            "inventory": [],
        }

        with self.assertRaisesRegex(ValueError, "JSON array"):
            store_raw(
                invalid_datasets,
                storage=storage,
                timestamp=self.timestamp,
                snapshot_id="snapshot-invalid",
            )

        self.assertEqual(storage.events, [])

    def test_empty_snapshot_is_rejected(self):
        storage = FakeStorage()

        with self.assertRaisesRegex(ValueError, "Incomplete snapshot package"):
            store_raw({}, storage=storage, snapshot_id="snapshot-empty")

        self.assertEqual(storage.events, [])


if __name__ == "__main__":
    unittest.main()
