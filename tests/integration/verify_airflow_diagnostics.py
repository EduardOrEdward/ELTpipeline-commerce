"""Verify intentional Airflow failures and successful task logs are saved to S3."""

from __future__ import annotations

import io
import json
import os
import time
import zipfile
import zlib
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


DAG_ID = "airflow_logging_diagnostics"
EXPECTED_TASK_STATES = {
    "runtime_error": "failed",
    "airflow_timeout": "failed",
    "success": "success",
}
ARTIFACT_WAIT_SECONDS = 90
ARTIFACT_POLL_SECONDS = 2


def wait_for_artifacts(client: Any, bucket: str, run_id: str) -> tuple[str, str]:
    prefix = (
        f"pipeline-logs/diagnostics/dag_id={DAG_ID}/run_id={run_id}"
    )
    report_key = f"{prefix}/run-report.json"
    archive_key = f"{prefix}/task-logs.zip"
    deadline = time.monotonic() + ARTIFACT_WAIT_SECONDS

    while time.monotonic() < deadline:
        try:
            client.head_object(Bucket=bucket, Key=report_key)
            client.head_object(Bucket=bucket, Key=archive_key)
            return report_key, archive_key
        except ClientError as error:
            # Retry only for a missing object; credentials and endpoint failures
            # should fail immediately instead of looking like delayed uploads.
            code = error.response.get("Error", {}).get("Code")
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status != 404 and code not in ("404", "NoSuchKey", "NotFound"):
                raise
            time.sleep(ARTIFACT_POLL_SECONDS)

    raise AssertionError(
        "Airflow logging callback did not publish both diagnostic artifacts "
        f"within {ARTIFACT_WAIT_SECONDS}s: s3://{bucket}/{prefix}/"
    )


def verify_report(client: Any, bucket: str, report_key: str, run_id: str) -> None:
    response = client.get_object(Bucket=bucket, Key=report_key)
    report = json.loads(response["Body"].read().decode("utf-8"))

    if report.get("dag_id") != DAG_ID or report.get("run_id") != run_id:
        raise AssertionError(
            "Diagnostic report identity mismatch: "
            f"dag_id={report.get('dag_id')!r}, run_id={report.get('run_id')!r}"
        )
    if report.get("state") != "failed":
        raise AssertionError(
            "Expected the diagnostic DAG to finish in failed state because two "
            f"failures are intentional; got {report.get('state')!r}"
        )

    tasks = {
        task.get("task_id"): task
        for task in report.get("tasks", [])
        if isinstance(task, dict)
    }
    if tasks.keys() != EXPECTED_TASK_STATES.keys():
        raise AssertionError(
            "Diagnostic report task set mismatch: "
            f"expected {sorted(EXPECTED_TASK_STATES)}, got {sorted(tasks)}"
        )

    actual_states = {
        task_id: tasks[task_id].get("state")
        for task_id in EXPECTED_TASK_STATES
    }
    if actual_states != EXPECTED_TASK_STATES:
        raise AssertionError(
            f"Unexpected diagnostic task states: {actual_states}; "
            f"expected {EXPECTED_TASK_STATES}"
        )


def verify_archive(client: Any, bucket: str, archive_key: str) -> None:
    response = client.get_object(Bucket=bucket, Key=archive_key)
    archive_bytes = response["Body"].read()

    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            corrupt_member = archive.testzip()
            if corrupt_member is not None:
                raise AssertionError(
                    f"Diagnostic ZIP CRC check failed for {corrupt_member!r}"
                )

            log_names = [
                name
                for name in archive.namelist()
                if name.endswith(".log") and not name.endswith("/")
            ]
            if not log_names:
                raise AssertionError("Diagnostic ZIP contains no task .log files")

            task_logs: dict[str, str] = {}
            for task_id in EXPECTED_TASK_STATES:
                matching = [
                    name
                    for name in log_names
                    if f"task_id={task_id}" in name.split("/")
                ]
                if not matching:
                    raise AssertionError(
                        f"Diagnostic ZIP contains no log for task {task_id!r}"
                    )
                chunks = []
                for name in matching:
                    info = archive.getinfo(name)
                    if info.file_size <= 0:
                        raise AssertionError(f"Diagnostic task log is empty: {name}")
                    chunks.append(archive.read(name).decode("utf-8", errors="replace"))
                task_logs[task_id] = "\n".join(chunks)

            runtime_log = task_logs["runtime_error"]
            if (
                "EXPECTED_DIAGNOSTIC_RUNTIME_ERROR" not in runtime_log
                or "RuntimeError" not in runtime_log
            ):
                raise AssertionError(
                    "Runtime-error task log does not contain the expected marker "
                    "and RuntimeError traceback"
                )

            timeout_log = task_logs["airflow_timeout"]
            if "EXPECTED_DIAGNOSTIC_TIMEOUT_START" not in timeout_log:
                raise AssertionError(
                    "Timeout task log is missing its start marker"
                )
            timeout_text = timeout_log.lower()
            if "airflowtasktimeout" not in timeout_text and "timed out" not in timeout_text:
                raise AssertionError(
                    "Timeout task log does not show an Airflow execution timeout"
                )

            success_log = task_logs["success"]
            if "EXPECTED_DIAGNOSTIC_SUCCESS" not in success_log:
                raise AssertionError(
                    "Successful task log is missing its completion marker"
                )
    except (zipfile.BadZipFile, OSError, EOFError, RuntimeError, zlib.error) as error:
        raise AssertionError(
            f"Could not read or validate diagnostic ZIP s3://{bucket}/{archive_key}: "
            f"{error}"
        ) from error


def main() -> None:
    run_id = os.environ["DIAGNOSTIC_RUN_ID"]
    bucket = os.environ["S3_BUCKET"]
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ADMIN_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_ADMIN_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    report_key, archive_key = wait_for_artifacts(client, bucket, run_id)
    verify_report(client, bucket, report_key, run_id)
    verify_archive(client, bucket, archive_key)

    print(f"Verified diagnostic run report: s3://{bucket}/{report_key}")
    print(f"Verified diagnostic task log archive: s3://{bucket}/{archive_key}")
    print("Expected runtime error, Airflow timeout, and success are all logged.")


if __name__ == "__main__":
    main()
