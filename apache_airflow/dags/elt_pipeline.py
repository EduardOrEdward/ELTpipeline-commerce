"""Airflow orchestration for the existing ELT pipeline.

API extraction/publication and Bronze loading run as separate, short-lived
containers so a Bronze retry can reuse the same completed S3 snapshot. Once
all operational stages and dbt tests succeed, the verification DAG is triggered
for the exact snapshot produced by this run.
"""

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from run_logging import publish_run_logs


def capture_pipeline_completion() -> str:
    """Capture the operational pipeline completion baseline after dbt succeeds."""
    return datetime.now(timezone.utc).isoformat()


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
    description="Extract to S3, load Bronze, build/test dbt models, then trigger verification",
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

    capture_completion = PythonOperator(
        task_id="capture_pipeline_completion",
        python_callable=capture_pipeline_completion,
    )

    trigger_verification = TriggerDagRunOperator(
        task_id="trigger_verification",
        trigger_dag_id="elt_pipeline_verification",
        trigger_run_id="verify__{{ run_id }}",
        conf={
            "snapshot_id": "{{ ti.xcom_pull(task_ids='extract_to_s3') }}",
            "pipeline_completed_at": "{{ ti.xcom_pull(task_ids='capture_pipeline_completion') }}",
            "source_dag_id": "{{ dag.dag_id }}",
            "source_run_id": "{{ run_id }}",
        },
        wait_for_completion=False,
        reset_dag_run=False,
    )

    extract_to_s3 >> load_bronze >> run_dbt_build >> capture_completion >> trigger_verification
