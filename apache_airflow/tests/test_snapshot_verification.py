"""Unit tests for the Airflow S3 snapshot verification task."""

import json
import os
import unittest
from io import BytesIO
from unittest.mock import patch

from apache_airflow.dags.snapshot_verification import verify_s3_snapshot


class FakeS3:
    def __init__(self, objects):
        self.objects = objects
        self.requested_keys = []

    def get_object(self, Bucket, Key):
        self.requested_keys.append((Bucket, Key))
        if Key not in self.objects:
            raise AssertionError(f"Unexpected S3 key requested: {Key}")
        payload = json.dumps(self.objects[Key]).encode("utf-8")
        return {"Body": BytesIO(payload)}


class VerifyS3SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.snapshot_id = "snapshot-test"
        self.manifest_key = f"snapshots/{self.snapshot_id}/manifest.json"
        self.objects = {
            self.manifest_key: {
                "format_version": 1,
                "snapshot_id": self.snapshot_id,
                "status": "COMPLETED",
                "created_at": "2026-10-22T00:00:00+00:00",
                "datasets": {
                    "orders": {
                        "object_key": "orders/2026-10-22/snapshot-test.json",
                        "record_count": 2,
                    },
                    "deliveries": {
                        "object_key": "deliveries/2026-10-22/snapshot-test.json",
                        "record_count": 1,
                    },
                    "inventory": {
                        "object_key": "inventory/2026-10-22/snapshot-test.json",
                        "record_count": 3,
                    },
                },
            },
            "orders/2026-10-22/snapshot-test.json": [{"id": 1}, {"id": 2}],
            "deliveries/2026-10-22/snapshot-test.json": [{"id": 1}],
            "inventory/2026-10-22/snapshot-test.json": [
                {"id": 1}, {"id": 2}, {"id": 3}
            ],
        }
        self.s3 = FakeS3(self.objects)
        self.environment = patch.dict(
            os.environ,
            {
                "S3_ENDPOINT": "http://seaweedfs:8333",
                "S3_BUCKET": "elt-raw",
                "S3_ACCESS_KEY": "test-access",
                "S3_SECRET_KEY": "test-secret",
            },
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.s3_client_patch = patch(
            "apache_airflow.dags.snapshot_verification.boto3.client",
            return_value=self.s3,
        )
        self.s3_client_patch.start()
        self.addCleanup(self.s3_client_patch.stop)

    def test_verifies_manifest_and_record_counts_without_inspecting_records(self):
        result = verify_s3_snapshot(self.snapshot_id)

        self.assertEqual(result["status"], "VERIFIED")
        self.assertEqual(result["snapshot_id"], self.snapshot_id)
        self.assertEqual(
            {name: details["record_count"] for name, details in result["datasets"].items()},
            {"deliveries": 1, "inventory": 3, "orders": 2},
        )
        self.assertEqual(len(self.s3.requested_keys), 4)

    def test_rejects_manifest_for_a_different_snapshot(self):
        self.objects[self.manifest_key]["snapshot_id"] = "another-snapshot"

        with self.assertRaisesRegex(ValueError, "does not match"):
            verify_s3_snapshot(self.snapshot_id)

    def test_rejects_manifest_missing_a_required_dataset(self):
        del self.objects[self.manifest_key]["datasets"]["inventory"]

        with self.assertRaisesRegex(ValueError, "exactly orders, deliveries, and inventory"):
            verify_s3_snapshot(self.snapshot_id)

    def test_rejects_record_count_mismatch(self):
        self.objects[self.manifest_key]["datasets"]["orders"]["record_count"] = 3

        with self.assertRaisesRegex(ValueError, "Record count mismatch"):
            verify_s3_snapshot(self.snapshot_id)

    def test_rejects_empty_dataset(self):
        self.objects["deliveries/2026-10-22/snapshot-test.json"] = []
        self.objects[self.manifest_key]["datasets"]["deliveries"]["record_count"] = 0

        with self.assertRaisesRegex(ValueError, "must not be empty"):
            verify_s3_snapshot(self.snapshot_id)

    def test_requires_snapshot_id(self):
        with self.assertRaisesRegex(ValueError, "snapshot_id"):
            verify_s3_snapshot("")


if __name__ == "__main__":
    unittest.main()
