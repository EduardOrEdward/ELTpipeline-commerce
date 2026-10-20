"""Unit tests for the split API-to-S3 and S3-to-Bronze stages."""

import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from ingestion import main


class IngestionStageTests(unittest.TestCase):
    def test_extract_to_s3_publishes_snapshot_without_postgres_dependency(self):
        datasets = {
            "orders": [{"order_id": 1}],
            "deliveries": [{"delivery_id": 1}],
            "inventory": [{"product_id": 1}],
        }
        object_keys = {
            "orders": "orders/2026-10-20/snapshot-test.json",
            "deliveries": "deliveries/2026-10-20/snapshot-test.json",
            "inventory": "inventory/2026-10-20/snapshot-test.json",
        }
        fake_storage = object()

        with (
            patch.object(main, "fetch_all", return_value=datasets) as fetch,
            patch.object(main, "store_raw", return_value=object_keys) as store,
        ):
            result = main.extract_to_s3(
                storage=fake_storage,
                snapshot_id="snapshot-test",
                limit=1,
            )

        fetch.assert_called_once_with(client=None, limit=1, snapshot_date=None)
        store.assert_called_once_with(
            datasets,
            storage=fake_storage,
            snapshot_id="snapshot-test",
        )
        self.assertEqual(result["snapshot_id"], "snapshot-test")
        self.assertEqual(
            result["manifest_key"],
            "snapshots/snapshot-test/manifest.json",
        )
        self.assertEqual(result["object_keys"], object_keys)

    def test_transient_s3_publish_error_retries_same_snapshot_without_refetch(self):
        datasets = {
            "orders": [{"order_id": 1}],
            "deliveries": [{"delivery_id": 1}],
            "inventory": [{"product_id": 1}],
        }
        object_keys = {
            "orders": "orders/2026-10-20/snapshot-test.json",
            "deliveries": "deliveries/2026-10-20/snapshot-test.json",
            "inventory": "inventory/2026-10-20/snapshot-test.json",
        }
        transient_error = ClientError(
            {
                "Error": {"Code": "InternalError", "Message": "temporary S3 error"},
                "ResponseMetadata": {"HTTPStatusCode": 500},
            },
            "PutObject",
        )

        with (
            patch.object(main, "fetch_all", return_value=datasets) as fetch,
            patch.object(
                main,
                "store_raw",
                side_effect=[transient_error, object_keys],
            ) as store,
            patch.object(main.time, "sleep") as sleep,
        ):
            result = main.extract_to_s3(snapshot_id="snapshot-test")

        self.assertEqual(result["snapshot_id"], "snapshot-test")
        self.assertEqual(store.call_count, 2)
        first_call, second_call = store.call_args_list
        self.assertEqual(first_call.kwargs["snapshot_id"], "snapshot-test")
        self.assertEqual(second_call.kwargs["snapshot_id"], "snapshot-test")
        self.assertEqual(first_call.kwargs["timestamp"], second_call.kwargs["timestamp"])
        fetch.assert_called_once_with(client=None, limit=100, snapshot_date=None)
        sleep.assert_called_once_with(1)

    def test_load_bronze_reads_existing_snapshot_without_fetching_api(self):
        snapshot_id = "snapshot-test"
        manifest_key = "snapshots/snapshot-test/manifest.json"
        datasets = {
            "orders": {
                "object_key": "orders/2026-10-20/snapshot-test.json",
                "record_count": 1,
            },
            "deliveries": {
                "object_key": "deliveries/2026-10-20/snapshot-test.json",
                "record_count": 1,
            },
            "inventory": {
                "object_key": "inventory/2026-10-20/snapshot-test.json",
                "record_count": 1,
            },
        }
        manifest = {
            "format_version": 1,
            "snapshot_id": snapshot_id,
            "status": "COMPLETED",
            "datasets": datasets,
        }
        payloads = {
            details["object_key"]: [{"dataset": dataset}]
            for dataset, details in datasets.items()
        }

        class FakeReader:
            def __init__(self):
                self.read_keys = []

            def get_json(self, key):
                self.read_keys.append(key)
                if key == manifest_key:
                    return manifest
                return payloads[key]

        reader = FakeReader()
        fake_connection = object()
        bronze_result = {
            "snapshot_id": snapshot_id,
            "already_loaded": False,
            "bronze_rows": {"orders": 1},
        }

        with (
            patch.object(main, "POSTGRES_PASSWORD", "test-password"),
            patch.object(main.psycopg, "connect") as connect,
            patch.object(main, "load_snapshot_to_bronze", return_value=bronze_result) as load,
            patch.object(main, "fetch_all") as fetch,
        ):
            connect.return_value.__enter__.return_value = fake_connection
            result = main.load_bronze_from_s3(snapshot_id, storage_reader=reader)

        fetch.assert_not_called()
        connect.assert_called_once()
        load.assert_called_once_with(
            fake_connection,
            snapshot_id=snapshot_id,
            manifest_key=manifest_key,
            manifest=manifest,
            raw_objects=payloads,
        )
        self.assertEqual(reader.read_keys, [manifest_key, *payloads.keys()])
        self.assertEqual(result["snapshot_id"], snapshot_id)
        self.assertEqual(result["bronze_rows"], {"orders": 1})
        self.assertFalse(result["already_loaded"])


if __name__ == "__main__":
    unittest.main()
