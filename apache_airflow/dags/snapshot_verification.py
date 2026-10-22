"""Verify the integrity of one already-published S3 snapshot.

This DAG does not call the source API or inspect business fields. Trigger it
with {"snapshot_id": "<id returned by elt_pipeline.extract_to_s3>"}.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


REQUIRED_DATASETS = frozenset({"orders", "deliveries", "inventory"})


def _read_json_object(s3: Any, bucket: str, object_key: str) -> Any:
    """Read and decode one S3 JSON object, adding the key to parse errors."""
    response = s3.get_object(Bucket=bucket, Key=object_key)
    body = response["Body"].read()
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"S3 object is not valid UTF-8 JSON: s3://{bucket}/{object_key}") from error


def verify_s3_snapshot(snapshot_id: str) -> dict[str, Any]:
    """Validate a completed snapshot's manifest and stored record counts.

    The check only reads JSON structure and array lengths; it deliberately does
    not inspect individual records or call the source API.
    """
    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise ValueError("A non-empty snapshot_id must be supplied in dag_run.conf")

    snapshot_id = snapshot_id.strip()
    if "/" in snapshot_id or snapshot_id in {".", ".."}:
        raise ValueError("snapshot_id must be a single identifier, not an S3 path")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"].rstrip("/"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    bucket = os.environ["S3_BUCKET"]
    manifest_key = f"snapshots/{snapshot_id}/manifest.json"
    manifest = _read_json_object(s3, bucket, manifest_key)

    if not isinstance(manifest, dict):
        raise ValueError(f"Snapshot manifest must be a JSON object: {manifest_key}")
    if manifest.get("format_version") != 1 or isinstance(manifest.get("format_version"), bool):
        raise ValueError(f"Unsupported manifest format_version in {manifest_key}")
    if manifest.get("snapshot_id") != snapshot_id:
        raise ValueError(
            f"Manifest snapshot_id does not match requested snapshot {snapshot_id}"
        )
    if manifest.get("status") != "COMPLETED":
        raise ValueError(f"Snapshot {snapshot_id} is not marked COMPLETED")

    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict) or set(datasets) != REQUIRED_DATASETS:
        raise ValueError(
            "Snapshot manifest must contain exactly orders, deliveries, and inventory"
        )

    verified_datasets: dict[str, dict[str, Any]] = {}
    seen_object_keys: set[str] = set()
    for dataset in sorted(REQUIRED_DATASETS):
        details = datasets[dataset]
        if not isinstance(details, dict):
            raise ValueError(f"Invalid manifest entry for dataset {dataset!r}")

        object_key = details.get("object_key")
        if not isinstance(object_key, str) or not object_key.strip():
            raise ValueError(f"Manifest has no valid object_key for {dataset!r}")
        object_key = object_key.strip()
        if object_key.split("/", 1)[0] != dataset:
            raise ValueError(
                f"Manifest object for {dataset!r} points outside its dataset prefix: {object_key}"
            )
        if object_key in seen_object_keys:
            raise ValueError(f"Manifest reuses the same S3 object more than once: {object_key}")
        seen_object_keys.add(object_key)

        record_count = details.get("record_count")
        if isinstance(record_count, bool) or not isinstance(record_count, int):
            raise ValueError(f"Manifest record_count for {dataset!r} must be an integer")
        if record_count <= 0:
            raise ValueError(f"Manifest dataset {dataset!r} must not be empty")

        records = _read_json_object(s3, bucket, object_key)
        if not isinstance(records, list):
            raise ValueError(f"Dataset object must contain a JSON array: {object_key}")
        if not records:
            raise ValueError(f"Dataset object is empty: {object_key}")
        if len(records) != record_count:
            raise ValueError(
                f"Record count mismatch for {dataset!r}: "
                f"manifest={record_count}, object={len(records)} ({object_key})"
            )

        verified_datasets[dataset] = {
            "object_key": object_key,
            "record_count": len(records),
        }

    return {
        "snapshot_id": snapshot_id,
        "manifest_key": manifest_key,
        "status": "VERIFIED",
        "datasets": verified_datasets,
    }


with DAG(
    dag_id="elt_pipeline_verification",
    description="Verify the manifest and stored record counts for one S3 snapshot",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["elt", "verification", "s3"],
) as dag:
    verify_snapshot = PythonOperator(
        task_id="verify_s3_snapshot",
        python_callable=verify_s3_snapshot,
        op_kwargs={
            "snapshot_id": "{{ (dag_run.conf or {}).get('snapshot_id', '') }}",
        },
    )
