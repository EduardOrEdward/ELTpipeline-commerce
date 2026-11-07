"""Isolated diagnostic DAG for the shared Airflow-to-S3 logging path.

The DAG intentionally ends in a failed state: one task raises a controlled
runtime error and another exceeds Airflow's execution timeout. The CI verifier
treats those exact outcomes as success when their logs and S3 artifacts exist.
"""

import logging
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.standard.operators.python import PythonOperator

from run_logging import publish_run_logs


LOGGER = logging.getLogger("airflow.task")


def produce_runtime_error() -> None:
    """Raise a deterministic error whose traceback must be present in the log."""
    LOGGER.info("EXPECTED_DIAGNOSTIC_RUNTIME_ERROR: raising RuntimeError now")
    raise RuntimeError("EXPECTED_DIAGNOSTIC_RUNTIME_ERROR")


def exceed_airflow_timeout() -> None:
    """Run beyond execution_timeout so Airflow, not CI, terminates the task."""
    LOGGER.info("EXPECTED_DIAGNOSTIC_TIMEOUT_START: entering controlled wait")
    time.sleep(90)


def complete_successfully() -> None:
    """Emit a success marker for the positive logging path."""
    LOGGER.info("EXPECTED_DIAGNOSTIC_SUCCESS: task completed normally")


with DAG(
    dag_id="airflow_logging_diagnostics",
    description="Validate Airflow task logs and S3 callback artifacts independently of ELT",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    default_args={"retries": 0},
    tags=["diagnostics", "logging"],
) as dag:
    runtime_error = PythonOperator(
        task_id="runtime_error",
        python_callable=produce_runtime_error,
    )

    airflow_timeout = PythonOperator(
        task_id="airflow_timeout",
        python_callable=exceed_airflow_timeout,
        execution_timeout=timedelta(seconds=20),
    )

    success = PythonOperator(
        task_id="success",
        python_callable=complete_successfully,
    )

    publish_logs = PythonOperator(
        task_id="publish_run_logs",
        python_callable=publish_run_logs,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [runtime_error, airflow_timeout, success] >> publish_logs
