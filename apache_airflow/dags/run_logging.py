"""Shared Airflow run logging to S3 for operational and verification DAGs."""

import json
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from airflow.configuration import conf
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session


def publish_run_logs(context: dict) -> None:
    """Publish a DAG run summary and its available task logs to S3.

    This callback only records diagnostics; it does not retry tasks or change
    the DAG's task failure behavior.
    """
    dag_run = context["dag_run"]
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    log_root = Path(conf.get("logging", "BASE_LOG_FOLDER"))
    run_log_dir = log_root / f"dag_id={dag_id}" / f"run_id={run_id}"

    def isoformat(value):
        return value.isoformat() if value is not None else None

    # Airflow 3 exposes dag_run in callback context as an SDK/Pydantic
    # object, not the ORM object with get_task_instances(). Query the ORM
    # task-instance records by DAG/run identity instead.
    with create_session() as session:
        task_instances = sorted(
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
            )
            .all(),
            key=lambda task_instance: task_instance.task_id,
        )
    report = {
        "dag_id": dag_id,
        "run_id": run_id,
        "run_type": str(dag_run.run_type),
        "state": dag_run.state,
        "start_date": isoformat(dag_run.start_date),
        "end_date": isoformat(dag_run.end_date),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tasks": [
            {
                "task_id": task_instance.task_id,
                "state": task_instance.state,
                "try_number": task_instance.try_number,
                "start_date": isoformat(task_instance.start_date),
                "end_date": isoformat(task_instance.end_date),
                "duration_seconds": task_instance.duration,
                "log_url": task_instance.log_url,
            }
            for task_instance in task_instances
        ],
    }

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    )
    bucket = os.environ["S3_BUCKET"]
    run_prefix = f"pipeline-logs/dag_id={dag_id}/run_id={run_id}"
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
