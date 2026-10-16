"""Unit tests for the S3-to-Bronze snapshot contract."""

import unittest

from ingestion.bronze import _validate_completed_snapshot
from ingestion.storage import manifest_key_for_snapshot


SNAPSHOT_ID = "snapshot-001"
MANIFEST_KEY = manifest_key_for_snapshot(SNAPSHOT_ID)
OBJECT_KEYS = {
    "orders": f"orders/2026-10-16/{SNAPSHOT_ID}.json",
    "deliveries": f"deliveries/2026-10-16/{SNAPSHOT_ID}.json",
    "inventory": f"inventory/2026-10-16/{SNAPSHOT_ID}.json",
}


def make_snapshot():
    datasets = {
        dataset: {
            "object_key": object_key,
            "record_count": 1,
        }
        for dataset, object_key in OBJECT_KEYS.items()
    }
    manifest = {
        "format_version": 1,
        "snapshot_id": SNAPSHOT_ID,
        "status": "COMPLETED",
        "datasets": datasets,
    }
    raw_objects = {
        object_key: [{"source": dataset}]
        for dataset, object_key in OBJECT_KEYS.items()
    }
    return manifest, raw_objects


class CompletedSnapshotContractTests(unittest.TestCase):
    def test_accepts_complete_snapshot(self):
        manifest, raw_objects = make_snapshot()

        result = _validate_completed_snapshot(
            SNAPSHOT_ID, MANIFEST_KEY, manifest, raw_objects
        )

        self.assertEqual(result, OBJECT_KEYS)

    def test_rejects_unsupported_manifest_version(self):
        manifest, raw_objects = make_snapshot()
        manifest["format_version"] = 2

        with self.assertRaisesRegex(ValueError, "format version"):
            _validate_completed_snapshot(
                SNAPSHOT_ID, MANIFEST_KEY, manifest, raw_objects
            )

    def test_rejects_manifest_missing_required_dataset(self):
        manifest, raw_objects = make_snapshot()
        del manifest["datasets"]["inventory"]
        del raw_objects[OBJECT_KEYS["inventory"]]

        with self.assertRaisesRegex(ValueError, "orders, deliveries, and inventory"):
            _validate_completed_snapshot(
                SNAPSHOT_ID, MANIFEST_KEY, manifest, raw_objects
            )

    def test_rejects_object_belonging_to_another_snapshot(self):
        manifest, raw_objects = make_snapshot()
        old_key = OBJECT_KEYS["orders"]
        wrong_key = "orders/2026-10-15/another-snapshot.json"
        manifest["datasets"]["orders"]["object_key"] = wrong_key
        raw_objects[wrong_key] = raw_objects.pop(old_key)

        with self.assertRaisesRegex(ValueError, "does not belong to snapshot"):
            _validate_completed_snapshot(
                SNAPSHOT_ID, MANIFEST_KEY, manifest, raw_objects
            )

    def test_rejects_boolean_or_negative_record_count(self):
        for invalid_count in (True, -1):
            with self.subTest(record_count=invalid_count):
                manifest, raw_objects = make_snapshot()
                manifest["datasets"]["orders"]["record_count"] = invalid_count

                with self.assertRaisesRegex(ValueError, "Invalid record count"):
                    _validate_completed_snapshot(
                        SNAPSHOT_ID, MANIFEST_KEY, manifest, raw_objects
                    )

    def test_rejects_non_object_records(self):
        manifest, raw_objects = make_snapshot()
        raw_objects[OBJECT_KEYS["orders"]] = ["not-an-object"]

        with self.assertRaisesRegex(ValueError, "only JSON objects"):
            _validate_completed_snapshot(
                SNAPSHOT_ID, MANIFEST_KEY, manifest, raw_objects
            )

    def test_rejects_unlisted_extra_object(self):
        manifest, raw_objects = make_snapshot()
        raw_objects["orders/2026-10-16/unexpected.json"] = []

        with self.assertRaisesRegex(ValueError, "exactly match"):
            _validate_completed_snapshot(
                SNAPSHOT_ID, MANIFEST_KEY, manifest, raw_objects
            )


if __name__ == "__main__":
    unittest.main()
