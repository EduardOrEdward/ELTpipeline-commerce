"""Shared Airflow run logging to S3 for operational and verification DAGs."""

import json
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from airflow.configuration import conf


DIAGNOSTIC_DAG_ID = "airflow_logging_diagnostics"


def publish_run_logs(**context) -> None:
    """Publish a DAG run summary and task logs from a terminal Airflow task.

    Running this as a task gives Airflow 3 the Task SDK supervisor context
    required by the public task-state API. The task runs with ALL_DONE so it
    can publish diagnostics after upstream failures; it then re-raises a
    failure to preserve the DAG run's failed status.
    """
    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    log_root = Path(conf.get("logging", "BASE_LOG_FOLDER"))
    run_log_dir = log_root / f"dag_id={dag_id}" / f"run_id={run_id}"

    def isoformat(value):
        return value.isoformat() if value is not None else None

    # Airflow 3 task code must not query the metadata database directly.
    # This terminal PythonOperator runs inside the Task SDK supervisor context,
    # where RuntimeTaskInstance.get_task_states() is supported.
    task_instance = context.get("ti")
    if task_instance is None:
        raise RuntimeError("Logging task context did not include a task instance.")

    task_states_by_run = task_instance.get_task_states(
        dag_id=dag_id,
        run_ids=[run_id],
    )
    run_task_states = task_states_by_run.get(run_id)
    if run_task_states is None:
        raise RuntimeError(f"No task states returned for DAG run {dag_id}/{run_id}.")

    failed_tasks = {
        task_id: state
        for task_id, state in run_task_states.items()
        if task_id != task_instance.task_id and state not in {"success", "skipped"}
    }
    final_state = "failed" if failed_tasks else "success"
    report_tasks = [
        {"task_id": task_id, "state": state}
        for task_id, state in sorted(run_task_states.items())
    ]
    # The publisher is still running while it writes the report. Record its
    # expected terminal state, derived from the upstream tasks.
    report_tasks = [
        task for task in report_tasks if task["task_id"] != task_instance.task_id
    ]
    report_tasks.append({
        "task_id": task_instance.task_id,
        "state": "failed" if failed_tasks else "success",
    })

    report = {
        "dag_id": dag_id,
        "run_id": run_id,
        "run_type": str(dag_run.run_type),
        "state": final_state,
        "start_date": isoformat(dag_run.start_date),
        "end_date": isoformat(dag_run.end_date),
        "created_at": datetime.now(timezone.utc).isoformat(),
        # Verification reports retain the exact upstream run baseline without
        # copying arbitrary DAG configuration (which could contain secrets).
        "snapshot_id": (dag_run.conf or {}).get("snapshot_id"),
        "pipeline_completed_at": (dag_run.conf or {}).get("pipeline_completed_at"),
        "source_run_id": (dag_run.conf or {}).get("source_run_id"),
        "tasks": report_tasks,
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    )
    bucket = os.environ["S3_BUCKET"]
    # Keep diagnostic artifacts separate while using the exact same callback,
    # credentials, S3 service, and Airflow task-log directory as production DAGs.
    artifact_root = (
        "pipeline-logs/diagnostics"
        if dag_id == DIAGNOSTIC_DAG_ID
        else "pipeline-logs"
    )
    run_prefix = f"{artifact_root}/dag_id={dag_id}/run_id={run_id}"
    s3.put_object(
        Bucket=bucket,
        Key=f"{run_prefix}/run-report.json",
        Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )

    with tempfile.TemporaryDirectory() as temporary_directory:
        archive_path = Path(temporary_directory) / "task-logs.zip"
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            if run_log_dir.exists():
                for log_path in sorted(run_log_dir.rglob("*")):
                    if log_path.is_file():
                        archive.write(log_path, log_path.relative_to(log_root))
        s3.upload_file(str(archive_path), bucket, f"{run_prefix}/task-logs.zip")

    if failed_tasks:
        raise RuntimeError(
            "Run log artifacts were uploaded, but upstream tasks failed: "
            f"{failed_tasks}"
        )
