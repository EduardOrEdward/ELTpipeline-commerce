"""Verify that one successful ELT run is consistent from S3 through Metabase.

This DAG is triggered by elt_pipeline only after extraction, Bronze loading, and
`dbt build` have succeeded. It never calls the source API or retries the
operational pipeline; failures here are verification failures with their own
Airflow task logs.
"""

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

import boto3
import psycopg
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from run_logging import publish_run_logs


REQUIRED_DATASETS = frozenset({"orders", "deliveries", "inventory"})
BRONZE_TABLES = {
    "orders": "bronze.orders",
    "deliveries": "bronze.deliveries",
    "inventory": "bronze.inventory",
}
MART_TABLES = ("inventory", "supplier_reliability")


def _read_json_object(s3: Any, bucket: str, object_key: str) -> Any:
    """Read and decode one S3 JSON object, adding the key to parse errors."""
    response = s3.get_object(Bucket=bucket, Key=object_key)
    body = response["Body"].read()
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(
            f"S3 object is not valid UTF-8 JSON: s3://{bucket}/{object_key}"
        ) from error


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"].rstrip("/"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )


def _pipeline_timestamp(value: str | None) -> str:
    """Require the completion timestamp captured after dag1's dbt build."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("pipeline_completed_at must be supplied by elt_pipeline")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("pipeline_completed_at must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError("pipeline_completed_at must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def verify_s3_snapshot(
    snapshot_id: str,
    pipeline_completed_at: str | None = None,
) -> dict[str, Any]:
    """Validate manifest identity, required datasets, object presence and counts.

    This check intentionally validates snapshot structure and record counts
    without inspecting business fields or calling the source API.
    """
    if not isinstance(snapshot_id, str) or not snapshot_id.strip():
        raise ValueError("A non-empty snapshot_id must be supplied by elt_pipeline")

    snapshot_id = snapshot_id.strip()
    if "/" in snapshot_id or snapshot_id in {".", ".."}:
        raise ValueError("snapshot_id must be a single identifier, not an S3 path")
    completed_at = _pipeline_timestamp(pipeline_completed_at)

    s3 = _s3_client()
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
        key_parts = object_key.split("/")
        if (
            len(key_parts) != 3
            or key_parts[0] != dataset
            or not key_parts[1]
            or key_parts[2] != f"{snapshot_id}.json"
        ):
            raise ValueError(
                f"Manifest object for {dataset!r} does not belong to snapshot "
                f"{snapshot_id}: {object_key}"
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
        if any(not isinstance(record, dict) for record in records):
            raise ValueError(f"Dataset object must contain JSON objects: {object_key}")

        verified_datasets[dataset] = {
            "object_key": object_key,
            "record_count": len(records),
        }

    return {
        "snapshot_id": snapshot_id,
        "manifest_key": manifest_key,
        "pipeline_completed_at": completed_at,
        "status": "VERIFIED",
        "datasets": verified_datasets,
    }


def verify_bronze_snapshot(snapshot_result: dict[str, Any]) -> dict[str, Any]:
    """Compare this run's S3 objects with their exact Bronze rows and indexes."""
    if snapshot_result.get("status") != "VERIFIED":
        raise ValueError("Bronze verification requires a verified S3 snapshot")

    snapshot_id = snapshot_result["snapshot_id"]
    manifest_key = snapshot_result["manifest_key"]
    s3 = _s3_client()
    bucket = os.environ["S3_BUCKET"]

    connection_settings = {
        "host": os.environ["POSTGRES_HOST"],
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
    }
    verified: dict[str, dict[str, Any]] = {}

    with psycopg.connect(**connection_settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT manifest_key
                FROM bronze.loaded_snapshots
                WHERE snapshot_id = %s
                """,
                (snapshot_id,),
            )
            loaded_snapshot = cursor.fetchone()
        if loaded_snapshot is None:
            raise ValueError(f"Snapshot {snapshot_id} is not registered in bronze.loaded_snapshots")
        if loaded_snapshot[0] != manifest_key:
            raise ValueError(f"Bronze manifest key does not match S3 for snapshot {snapshot_id}")

        for dataset, details in snapshot_result["datasets"].items():
            object_key = details["object_key"]
            expected_records = _read_json_object(s3, bucket, object_key)
            table = BRONZE_TABLES[dataset]
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT record_index, raw_payload
                    FROM {table}
                    WHERE source_object_key = %s
                    ORDER BY record_index
                    """,
                    (object_key,),
                )
                bronze_rows = cursor.fetchall()

            expected_indexes = list(range(len(expected_records)))
            actual_indexes = [row[0] for row in bronze_rows]
            if actual_indexes != expected_indexes:
                raise ValueError(
                    f"Bronze record indexes do not match S3 for {dataset}: "
                    f"expected {expected_indexes}, got {actual_indexes}"
                )

            def canonical(record: Any) -> str:
                return json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

            actual_records = [row[1] for row in bronze_rows]
            if [canonical(record) for record in actual_records] != [
                canonical(record) for record in expected_records
            ]:
                raise ValueError(f"Bronze raw payload differs from S3 for {dataset}: {object_key}")

            verified[dataset] = {
                "object_key": object_key,
                "record_count": len(bronze_rows),
                "payload_match": True,
                "record_indexes_complete": True,
            }

    return {
        "snapshot_id": snapshot_id,
        "pipeline_completed_at": snapshot_result["pipeline_completed_at"],
        "status": "VERIFIED",
        "datasets": verified,
    }


def verify_mart_tables(snapshot_result: dict[str, Any]) -> dict[str, Any]:
    """Check Mart readiness and confirm inventory reflects this run's snapshot."""
    if snapshot_result.get("status") != "VERIFIED":
        raise ValueError("Mart verification requires a verified S3 snapshot")

    s3 = _s3_client()
    bucket = os.environ["S3_BUCKET"]
    inventory_key = snapshot_result["datasets"]["inventory"]["object_key"]
    orders_key = snapshot_result["datasets"]["orders"]["object_key"]
    inventory_records = _read_json_object(s3, bucket, inventory_key)
    order_records = _read_json_object(s3, bucket, orders_key)

    # Mirror the validity filters and latest-date choice used by stg_inventory
    # and mart.inventory. record_index breaks ties within one immutable object.
    expected_inventory: dict[int, tuple[int, str, int]] = {}
    for index, record in enumerate(inventory_records):
        try:
            product_id = int(record["product_id"])
            quantity = int(record["quantity"])
            snapshot_date = str(record["snapshot_date"])
        except (KeyError, TypeError, ValueError):
            continue
        if product_id <= 0 or quantity < 0 or not snapshot_date or snapshot_date == "None":
            continue
        previous = expected_inventory.get(product_id)
        if previous is None or (snapshot_date, index) > (previous[1], previous[2]):
            expected_inventory[product_id] = (quantity, snapshot_date, index)

    expected_suppliers = set()
    for record in order_records:
        try:
            order_id = record.get("order_id")
            product_id = int(record["product_id"])
            supplier_id = int(record["supplier_id"])
            planned_quantity = int(record["planned_quantity"])
        except (KeyError, TypeError, ValueError):
            continue
        if order_id is not None and product_id > 0 and supplier_id > 0 and planned_quantity > 0:
            expected_suppliers.add(supplier_id)

    connection_settings = {
        "host": os.environ["POSTGRES_HOST"],
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
    }
    with psycopg.connect(**connection_settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT product_id, quantity, snapshot_date FROM mart.inventory")
            inventory_rows = cursor.fetchall()
            cursor.execute("SELECT supplier_id FROM mart.supplier_reliability")
            supplier_rows = cursor.fetchall()

    if not inventory_rows:
        raise ValueError("mart.inventory exists but contains no rows")
    if not supplier_rows:
        raise ValueError("mart.supplier_reliability exists but contains no rows")

    actual_inventory = {
        int(product_id): (int(quantity), str(snapshot_date))
        for product_id, quantity, snapshot_date in inventory_rows
    }
    expected_inventory_values = {
        product_id: (quantity, snapshot_date)
        for product_id, (quantity, snapshot_date, _index) in expected_inventory.items()
    }
    # A product omitted from this snapshot may legitimately retain an older
    # known value in the Mart. Every product present in this snapshot must be
    # represented, and this run must win whenever its snapshot_date is newer
    # than or equal to the value currently published.
    for product_id, expected_value in expected_inventory_values.items():
        actual_value = actual_inventory.get(product_id)
        if actual_value is None:
            raise ValueError(
                f"mart.inventory is missing product {product_id} from this dag1 snapshot"
            )
        if expected_value[1] >= actual_value[1] and actual_value != expected_value:
            raise ValueError(
                f"mart.inventory is stale or inconsistent for product {product_id}"
            )

    actual_suppliers = {int(row[0]) for row in supplier_rows}
    if not expected_suppliers.issubset(actual_suppliers):
        missing_suppliers = sorted(expected_suppliers - actual_suppliers)
        raise ValueError(
            "mart.supplier_reliability is missing suppliers from this dag1 snapshot: "
            f"{missing_suppliers}"
        )

    return {
        "snapshot_id": snapshot_result["snapshot_id"],
        "pipeline_completed_at": snapshot_result["pipeline_completed_at"],
        "status": "VERIFIED",
        "mart": {
            "inventory_rows": len(inventory_rows),
            "inventory_matches_snapshot": True,
            "supplier_reliability_rows": len(supplier_rows),
            "supplier_coverage_matches_snapshot": True,
        },
    }


def _verify_bronze_task(ti) -> dict[str, Any]:
    """Pull the S3 verification result without stringifying its XCom value."""
    return verify_bronze_snapshot(ti.xcom_pull(task_ids="verify_s3_snapshot"))


def _verify_mart_task(ti) -> dict[str, Any]:
    """Pull the S3 verification result without stringifying its XCom value."""
    return verify_mart_tables(ti.xcom_pull(task_ids="verify_s3_snapshot"))


def _verify_metabase_task(
    ti,
    metabase_url: str,
    admin_email: str,
    admin_password: str,
) -> dict[str, Any]:
    """Pull the S3 verification result and check Metabase with configured credentials."""
    return verify_metabase_readiness(
        ti.xcom_pull(task_ids="verify_s3_snapshot"),
        metabase_url,
        admin_email,
        admin_password,
    )


def _metabase_request(
    base_url: str,
    path: str,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    session: str | None = None,
) -> tuple[int, dict[str, Any]]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else {}
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as error:
        raise RuntimeError(f"Metabase API {method} {path} failed: {error}") from error


def verify_metabase_readiness(
    snapshot_result: dict[str, Any],
    metabase_url: str,
    admin_email: str,
    admin_password: str,
) -> dict[str, Any]:
    """Authenticate to Metabase and query both required Mart tables."""
    if not metabase_url or not admin_email or not admin_password:
        raise ValueError(
            "Configure Airflow Variables METABASE_URL, METABASE_ADMIN_EMAIL, "
            "and METABASE_ADMIN_PASSWORD"
        )

    _, session_response = _metabase_request(
        metabase_url,
        "/api/session",
        method="POST",
        payload={"username": admin_email, "password": admin_password},
    )
    session_id = session_response.get("id")
    if not session_id:
        raise RuntimeError("Metabase authentication did not return a session")

    _, databases_response = _metabase_request(
        metabase_url, "/api/database", session=session_id
    )
    source = next(
        (
            item
            for item in databases_response.get("data", [])
            if item.get("name") == "ELT Pipeline"
        ),
        None,
    )
    if source is None:
        raise RuntimeError("Metabase data source 'ELT Pipeline' was not found")

    database_id = int(source["id"])
    queried_tables = {}
    for table in MART_TABLES:
        _, result = _metabase_request(
            metabase_url,
            "/api/dataset",
            method="POST",
            session=session_id,
            payload={
                "database": database_id,
                "type": "native",
                "native": {
                    "query": f"SELECT COUNT(*) AS row_count FROM mart.{table}",
                    "template-tags": {},
                },
                "parameters": [],
            },
        )
        rows = result.get("data", {}).get("rows", [])
        if not rows or not rows[0] or int(rows[0][0]) <= 0:
            raise RuntimeError(f"Metabase query returned no data for mart.{table}")
        queried_tables[table] = int(rows[0][0])

    return {
        "snapshot_id": snapshot_result["snapshot_id"],
        "pipeline_completed_at": snapshot_result["pipeline_completed_at"],
        "status": "VERIFIED",
        "metabase": {
            "data_source": "ELT Pipeline",
            "queried_tables": queried_tables,
        },
    }


with DAG(
    dag_id="elt_pipeline_verification",
    description="Verify one dag1 snapshot across S3, Bronze, Mart, and Metabase",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    on_success_callback=publish_run_logs,
    on_failure_callback=publish_run_logs,
    tags=["elt", "verification", "s3", "bronze", "metabase"],
) as dag:
    verify_snapshot = PythonOperator(
        task_id="verify_s3_snapshot",
        python_callable=verify_s3_snapshot,
        op_kwargs={
            "snapshot_id": "{{ (dag_run.conf or {}).get('snapshot_id', '') }}",
            "pipeline_completed_at": "{{ (dag_run.conf or {}).get('pipeline_completed_at', '') }}",
        },
    )

    verify_bronze = PythonOperator(
        task_id="verify_bronze_snapshot",
        python_callable=_verify_bronze_task,
    )

    verify_mart = PythonOperator(
        task_id="verify_mart_tables",
        python_callable=_verify_mart_task,
    )

    verify_metabase = PythonOperator(
        task_id="verify_metabase_readiness",
        python_callable=_verify_metabase_task,
        op_kwargs={
            "metabase_url": "{{ var.value.METABASE_URL }}",
            "admin_email": "{{ var.value.METABASE_ADMIN_EMAIL }}",
            "admin_password": "{{ var.value.METABASE_ADMIN_PASSWORD }}",
        },
    )

    verify_snapshot >> verify_bronze >> verify_mart >> verify_metabase
