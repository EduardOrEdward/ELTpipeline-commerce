"""Airflow orchestration for the existing ELT pipeline.

Ingestion and dbt run in separate, short-lived containers so their runtime
dependencies remain separate from Airflow's environment.
"""

import os
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
            "API_URL": os.environ.get("API_URL", "http://mock_api:8000"),
            "S3_ENDPOINT": os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333"),
            "S3_BUCKET": os.environ.get("S3_BUCKET", "elt-raw"),
            "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY", ""),
            "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY", ""),
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "db"),
            "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "elt_pipeline"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "postgre_user"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "postgre_password"),
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
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "db"),
            "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "elt_pipeline"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "postgre_user"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "postgre_password"),
        },
    )

    run_ingestion >> run_dbt_build
