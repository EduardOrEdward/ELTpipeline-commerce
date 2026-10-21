"""Verify that the Airflow DAG callback publishes run diagnostics to S3."""

import json
import os

import boto3


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    )
    bucket = os.environ["S3_BUCKET"]
    prefix = "pipeline-logs/dag_id=elt_pipeline/"
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [item["Key"] for item in response.get("Contents", [])]

    reports = [key for key in keys if key.endswith("/run-report.json")]
    archives = [key for key in keys if key.endswith("/task-logs.zip")]
    if not reports or not archives:
        raise AssertionError(
            "Expected both run-report.json and task-logs.zip under "
            f"s3://{bucket}/{prefix}; found keys: {keys}"
        )

    report_key = reports[-1]
    report_response = client.get_object(Bucket=bucket, Key=report_key)
    report = json.loads(report_response["Body"].read().decode("utf-8"))

    expected_tasks = {"extract_to_s3", "load_bronze", "run_dbt_build"}
    tasks = {task["task_id"]: task for task in report.get("tasks", [])}
    missing_tasks = expected_tasks - tasks.keys()
    if missing_tasks:
        raise AssertionError(f"Run report is missing tasks: {sorted(missing_tasks)}")

    if report.get("state") != "success":
        raise AssertionError(f"Expected a successful smoke-test DAG run, got {report.get('state')!r}")

    failed_tasks = {
        task_id: task.get("state")
        for task_id, task in tasks.items()
        if task_id in expected_tasks and task.get("state") != "success"
    }
    if failed_tasks:
        raise AssertionError(f"Expected all pipeline tasks to succeed, got {failed_tasks}")

    print(f"Verified S3 run report: s3://{bucket}/{report_key}")
    print(f"Verified task log archive: s3://{bucket}/{archives[-1]}")


if __name__ == "__main__":
    main()
