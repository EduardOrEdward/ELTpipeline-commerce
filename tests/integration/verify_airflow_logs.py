"""Verify that both Airflow DAG callbacks publish usable diagnostics to S3."""

from __future__ import annotations

import io
import json
import os
import time
import zipfile
import zlib
from typing import Any

import boto3


DAGS = {
    "elt_pipeline": {
        "tasks": {
            "extract_to_s3",
            "load_bronze",
            "run_dbt_build",
            "capture_pipeline_completion",
            "trigger_verification",
        },
    },
    "elt_pipeline_verification": {
        "tasks": {
            "verify_s3_snapshot",
            "verify_bronze_snapshot",
            "verify_mart_tables",
            "verify_metabase_readiness",
        },
    },
}
LOG_START_MARKER = "Starting attempt"
LOG_SUCCESS_MARKERS = ("Marking task as SUCCESS", "Task instance finished: state=success")
ARTIFACT_WAIT_SECONDS = 60
ARTIFACT_POLL_SECONDS = 2


def list_keys(client: Any, bucket: str, prefix: str) -> set[str]:
    """List every object under a prefix, including results beyond 1,000 keys."""
    paginator = client.get_paginator("list_objects_v2")
    return {
        item["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
    }


def read_report(client: Any, bucket: str, key: str) -> dict[str, Any]:
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def find_report(
    client: Any,
    bucket: str,
    keys: set[str],
    dag_id: str,
    source_run_id: str | None = None,
) -> tuple[str, dict[str, Any]] | None:
    prefix = f"pipeline-logs/dag_id={dag_id}/"
    report_keys = sorted(
        key for key in keys
        if key.startswith(prefix) and key.endswith("/run-report.json")
    )
    matching = []
    for key in report_keys:
        report = read_report(client, bucket, key)
        if report.get("dag_id") != dag_id:
            continue
        if source_run_id is not None and report.get("source_run_id") != source_run_id:
            continue
        matching.append((key, report))

    if not matching:
        return None
    return max(
        matching,
        key=lambda item: str(item[1].get("created_at", "")),
    )


def wait_for_run_artifacts(
    client: Any,
    bucket: str,
) -> tuple[tuple[str, dict[str, Any]], tuple[str, dict[str, Any]], dict[str, str]]:
    """Wait briefly for both callbacks and their ZIP uploads to finish."""
    deadline = time.monotonic() + ARTIFACT_WAIT_SECONDS
    while True:
        dag1_keys = list_keys(
            client, bucket, "pipeline-logs/dag_id=elt_pipeline/"
        )
        dag2_keys = list_keys(
            client, bucket, "pipeline-logs/dag_id=elt_pipeline_verification/"
        )
        dag1 = find_report(client, bucket, dag1_keys, "elt_pipeline")
        dag2 = None
        archives: dict[str, str] = {}

        if dag1 is not None:
            dag1_report_key, dag1_report = dag1
            dag1_archive_key = dag1_report_key.rsplit("/", 1)[0] + "/task-logs.zip"
            if dag1_archive_key in dag1_keys:
                archives["elt_pipeline"] = dag1_archive_key
                dag2 = find_report(
                    client,
                    bucket,
                    dag2_keys,
                    "elt_pipeline_verification",
                    source_run_id=dag1_report.get("run_id"),
                )
                if dag2 is not None:
                    dag2_report_key, _ = dag2
                    dag2_archive_key = dag2_report_key.rsplit("/", 1)[0] + "/task-logs.zip"
                    if dag2_archive_key in dag2_keys:
                        archives["elt_pipeline_verification"] = dag2_archive_key
                        return dag1, dag2, archives

        if time.monotonic() >= deadline:
            missing = [
                dag_id for dag_id in DAGS
                if dag_id not in archives
            ]
            raise AssertionError(
                "Timed out waiting for run-report.json/task-logs.zip for: "
                f"{missing}. Check that each DAG callback completed and uploaded "
                "both diagnostic objects."
            )
        time.sleep(ARTIFACT_POLL_SECONDS)


def verify_report(
    dag_id: str,
    report_key: str,
    report: dict[str, Any],
) -> None:
    if report.get("state") != "success":
        raise AssertionError(
            f"{dag_id}: expected run state 'success', got {report.get('state')!r} "
            f"in s3 key {report_key}"
        )

    expected_tasks = DAGS[dag_id]["tasks"]
    tasks = {
        task["task_id"]: task
        for task in report.get("tasks", [])
        if "task_id" in task
    }
    missing_tasks = expected_tasks - tasks.keys()
    if missing_tasks:
        raise AssertionError(
            f"{dag_id}: run report is missing tasks {sorted(missing_tasks)}"
        )

    failed_tasks = {
        task_id: tasks[task_id].get("state")
        for task_id in expected_tasks
        if tasks[task_id].get("state") != "success"
    }
    if failed_tasks:
        raise AssertionError(
            f"{dag_id}: expected all required tasks to succeed, got {failed_tasks}"
        )


def verify_archive(
    client: Any,
    bucket: str,
    dag_id: str,
    archive_key: str,
) -> None:
    """Check ZIP integrity and validate one useful log for every expected task."""
    response = client.get_object(Bucket=bucket, Key=archive_key)
    archive_bytes = response["Body"].read()

    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            corrupt_member = archive.testzip()
            if corrupt_member is not None:
                raise AssertionError(
                    f"{dag_id}: ZIP CRC check failed for {corrupt_member!r} "
                    f"in s3 key {archive_key}"
                )

            files = {
                info.filename: info
                for info in archive.infolist()
                if not info.is_dir()
            }
            log_files = {
                name: info
                for name, info in files.items()
                if name.endswith(".log")
            }
            if not log_files:
                raise AssertionError(
                    f"{dag_id}: archive contains no .log files: {archive_key}"
                )

            for task_id in DAGS[dag_id]["tasks"]:
                task_logs = [
                    (name, info)
                    for name, info in log_files.items()
                    if f"task_id={task_id}" in name.split("/")
                ]
                if not task_logs:
                    raise AssertionError(
                        f"{dag_id}: ZIP has no task log for {task_id!r}: "
                        f"{archive_key}"
                    )

                task_log_text = []
                for name, info in task_logs:
                    if info.file_size <= 0:
                        raise AssertionError(
                            f"{dag_id}: task log is empty: {name!r}"
                        )
                    task_log_text.append(
                        archive.read(name).decode("utf-8", errors="replace")
                    )

                combined_text = "\n".join(task_log_text)
                missing_markers = [
                    marker
                    for marker in (LOG_START_MARKER,)
                    if marker not in combined_text
                ]
                if not any(marker in combined_text for marker in LOG_SUCCESS_MARKERS):
                    missing_markers.append("successful task completion marker")
                if missing_markers:
                    raise AssertionError(
                        f"{dag_id}: task log(s) for {task_id!r} do not contain "
                        f"expected marker(s) {missing_markers}: {archive_key}"
                    )
    except (zipfile.BadZipFile, OSError, EOFError, RuntimeError, zlib.error) as error:
        raise AssertionError(
            f"{dag_id}: could not read or validate ZIP s3://{bucket}/{archive_key}: "
            f"{error}"
        ) from error


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ADMIN_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_ADMIN_SECRET_KEY"],
    )
    bucket = os.environ["S3_BUCKET"]

    dag1, dag2, archives = wait_for_run_artifacts(client, bucket)
    (dag1_report_key, dag1_report), (dag2_report_key, dag2_report) = dag1, dag2

    if dag2_report.get("source_run_id") != dag1_report.get("run_id"):
        raise AssertionError(
            "Verification DAG report does not reference the tested ingestion run: "
            f"dag1={dag1_report.get('run_id')!r}, "
            f"dag2 source={dag2_report.get('source_run_id')!r}"
        )

    for dag_id, report_key, report in (
        ("elt_pipeline", dag1_report_key, dag1_report),
        ("elt_pipeline_verification", dag2_report_key, dag2_report),
    ):
        verify_report(dag_id, report_key, report)
        verify_archive(client, bucket, dag_id, archives[dag_id])
        print(f"Verified {dag_id} run report: s3://{bucket}/{report_key}")
        print(f"Verified {dag_id} task log archive: s3://{bucket}/{archives[dag_id]}")


if __name__ == "__main__":
    main()
