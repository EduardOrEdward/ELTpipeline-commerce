"""Airflow orchestration for the existing ELT pipeline.

API extraction/publication and Bronze loading run as separate, short-lived
containers so a Bronze retry can reuse the same completed S3 snapshot.
"""

import json
import os
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from airflow import DAG
from airflow.configuration import conf
from airflow.providers.docker.operators.docker import DockerOperator




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

    task_instances = sorted(
        dag_run.get_task_instances(),
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


INGESTION_ENVIRONMENT = {
    "API_URL": "{{ var.value.API_URL }}",
    "S3_ENDPOINT": "{{ var.value.S3_ENDPOINT }}",
    "S3_BUCKET": "{{ var.value.S3_BUCKET }}",
    "S3_ACCESS_KEY": "{{ var.value.S3_ACCESS_KEY }}",
    "S3_SECRET_KEY": "{{ var.value.S3_SECRET_KEY }}",
    "POSTGRES_HOST": "{{ var.value.POSTGRES_HOST }}",
    "POSTGRES_PORT": "{{ var.value.POSTGRES_PORT }}",
    "POSTGRES_DB": "{{ var.value.POSTGRES_DB }}",
    "POSTGRES_USER": "{{ var.value.POSTGRES_USER }}",
    "POSTGRES_PASSWORD": "{{ var.value.POSTGRES_PASSWORD }}",
    "PYTHONUNBUFFERED": "1",
}


with DAG(
    dag_id="elt_pipeline",
    description="Extract to S3, load Bronze, then build and test dbt models",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["elt", "ingestion", "dbt"],
    on_success_callback=publish_run_logs,
    on_failure_callback=publish_run_logs,
) as dag:
    extract_to_s3 = DockerOperator(
        task_id="extract_to_s3",
        image="elt-pipeline-ingestion:local",
        command="python -m ingestion.main --extract-to-s3",
        docker_url="unix://var/run/docker.sock",
        network_mode="elt_pipeline_network",
        # The Docker daemon may not share the worker's temporary directory.
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=True,
        retries=2,
        retry_delay=timedelta(seconds=30),
        environment=INGESTION_ENVIRONMENT,
    )

    load_bronze = DockerOperator(
        task_id="load_bronze",
        image="elt-pipeline-ingestion:local",
        command="python -m ingestion.main --load-bronze {{ ti.xcom_pull(task_ids='extract_to_s3') }}",
        docker_url="unix://var/run/docker.sock",
        network_mode="elt_pipeline_network",
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=False,
        retries=2,
        retry_delay=timedelta(seconds=30),
        environment=INGESTION_ENVIRONMENT,
    )

    run_dbt_build = DockerOperator(
        task_id="run_dbt_build",
        image="elt-pipeline-dbt:local",
        command="build",
        docker_url="unix://var/run/docker.sock",
        network_mode="elt_pipeline_network",
        # dbt has its own image; no dbt packages are installed in Airflow.
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=False,
        environment={
            "DBT_PROFILES_DIR": "/app",
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "db"),
            "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "elt_pipeline"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "postgre_user"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "postgre_password"),
        },
    )

    extract_to_s3 >> load_bronze >> run_dbt_build
