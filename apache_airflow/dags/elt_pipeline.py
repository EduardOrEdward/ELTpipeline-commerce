"""Airflow orchestration for the existing ELT pipeline.

API extraction/publication and Bronze loading run as separate, short-lived
containers so a Bronze retry can reuse the same completed S3 snapshot.
"""

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


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
