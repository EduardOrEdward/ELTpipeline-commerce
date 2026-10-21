# Airflow integration (local development)

Airflow runs as a separate Compose project. Its worker joins the shared `elt_pipeline_network` and uses DockerOperator to launch the existing ingestion and dbt images as separate, short-lived containers.

## Start the services

Run commands from the repository root. Ensure the root `.env` file contains `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_ADMIN_ACCESS_KEY`, `S3_ADMIN_SECRET_KEY`, and `POSTGRES_PASSWORD`. Keep real credentials out of Git.

1. Start the ELT dependencies and build both task images:

   ```bash
   docker compose up -d db mock_api seaweedfs
   docker compose build ingestion dbt
   ```

2. Build and start Airflow:

   ```bash
   docker compose -f apache_airflow/docker-compose.yaml up --build -d
   ```

3. Open the Airflow UI at [http://localhost:8080](http://localhost:8080). The local Compose defaults create the user `airflow` with password `airflow`; change these defaults for any shared or non-local deployment.

## Logs

Airflow task logs are available centrally in the Airflow UI under each DAG run and task instance. The Compose setup persists the task log files on the host in `logs/airflow/` (relative to the repository root), so they remain available after Airflow containers are restarted. Runtime logs are ignored by Git through the root `.gitignore`.

The Airflow CI smoke test captures Airflow and ELT service logs and uploads them as the `airflow-smoke-logs` workflow artifact. The integration job similarly uploads `integration-service-logs`. Artifacts are retained for 14 days and are available from the corresponding GitHub Actions run, including when a job fails. The files include service status snapshots and Docker Compose output; they do not change task retry or failure behavior.

When a DAG run finishes, a logging-only DAG callback also writes a `run-report.json` and a `task-logs.zip` to the configured S3 bucket under `pipeline-logs/dag_id=<dag_id>/run_id=<run_id>/`. The report contains the run state, task states, attempt counts, timing, and task log URLs. These callbacks only publish diagnostics; they do not retry tasks or alter task execution.

4. In the UI, unpause `elt_pipeline` if needed and trigger it manually. The DAG runs `extract_to_s3`, then `load_bronze`, then `run_dbt_build`. Each task launches its own container. The first task fetches all API datasets and publishes a completed S3 snapshot; its snapshot ID is passed to the second task through XCom. If Bronze loading retries, it reads that same snapshot from S3 and does not call the API again. The final task executes `dbt build`, which builds the models and runs their configured tests.

### Docker socket access

Only `airflow-worker` mounts `/var/run/docker.sock`. Access to the socket grants broad control over the Docker daemon, so this setup is intended for local development, not an untrusted or production Airflow deployment.

On Linux, the worker must be a member of the host Docker socket's group. Set `DOCKER_GID` in the shell before starting Compose, for example:

```bash
export DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
docker compose -f apache_airflow/docker-compose.yaml up --build -d
```

On Docker Desktop, the default group setting may be sufficient; if the worker logs a Docker socket permission error, check the socket access configuration for your platform.

## Test the DAG

The CI smoke test starts the ELT dependencies and Airflow, then executes the real DAG with `airflow dags test elt_pipeline 2026-10-13`. It verifies that the worker launches the isolated extraction, Bronze-loading, and dbt containers, that ingestion writes rows into PostgreSQL Bronze, and that dbt creates populated `mart.inventory` and `mart.supplier_reliability` tables. Since the task uses `dbt build`, dbt model tests must also pass for the DAG run to succeed.

## Stop services

Stop Airflow separately from the ELT services:

```bash
docker compose -f apache_airflow/docker-compose.yaml down
docker compose down
```
