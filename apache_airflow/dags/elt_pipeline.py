"""Airflow orchestration for the existing ELT pipeline.

Ingestion and dbt run in separate, short-lived containers so their runtime
dependencies remain separate from Airflow's environment.
"""

from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="elt_pipeline",
    description="Run ingestion, then build and test dbt models",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["elt", "ingestion", "dbt"],
) as dag:
    run_ingestion = DockerOperator(
        task_id="run_ingestion",
        image="elt-pipeline-ingestion:local",
        docker_url="unix://var/run/docker.sock",
        network_mode="elt_pipeline_network",
        # The Docker daemon may not share the worker's temporary directory.
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=False,
        environment={
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
        },
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
            "POSTGRES_HOST": "{{ env_var('POSTGRES_HOST') }}",
            "POSTGRES_PORT": "{{ env_var('POSTGRES_PORT') }}",
            "POSTGRES_DB": "{{ env_var('POSTGRES_DB') }}",
            "POSTGRES_USER": "{{ env_var('POSTGRES_USER') }}",
            "POSTGRES_PASSWORD": "{{ env_var('POSTGRES_PASSWORD') }}",
        },
    )

    run_ingestion >> run_dbt_build
