# Airflow integration (local development)

Airflow runs as a separate Compose project. Its worker joins the shared `elt_pipeline_network` and uses DockerOperator to launch the existing ingestion image as a separate, short-lived container.

## Start the services

Run commands from the repository root. Ensure the root `.env` file contains `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_ADMIN_ACCESS_KEY`, `S3_ADMIN_SECRET_KEY`, and `POSTGRES_PASSWORD`. Keep real credentials out of Git.

1. Start the ELT dependencies and build the ingestion image:

   ```bash
   docker compose up -d db mock_api seaweedfs
   docker compose build ingestion
   ```

2. Build and start Airflow:

   ```bash
   docker compose -f apache_airflow/docker-compose.yaml up --build -d
   ```

3. Open the Airflow UI at [http://localhost:8080](http://localhost:8080). The local Compose defaults create the user `airflow` with password `airflow`; change these defaults for any shared or non-local deployment.

4. In the UI, unpause `elt_pipeline` if needed and trigger it manually. Its first task, `run_ingestion`, starts `elt-pipeline-ingestion:local`, passes the runtime configuration and credentials, and waits for the container to finish.

### Docker socket access

Only `airflow-worker` mounts `/var/run/docker.sock`. Access to the socket grants broad control over the Docker daemon, so this setup is intended for local development, not an untrusted or production Airflow deployment.

On Linux, the worker must be a member of the host Docker socket's group. Set `DOCKER_GID` in the shell before starting Compose, for example:

```bash
export DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
docker compose -f apache_airflow/docker-compose.yaml up --build -d
```

On Docker Desktop, the default group setting may be sufficient; if the worker logs a Docker socket permission error, check the socket access configuration for your platform.

## Test the DAG

The CI smoke test starts the ELT dependencies and Airflow, then executes the real DAG with `airflow dags test elt_pipeline 2026-10-13`. This verifies that the worker can launch the separate ingestion container and that ingestion writes rows into PostgreSQL Bronze.

## Stop services

Stop Airflow separately from the ELT services:

```bash
docker compose -f apache_airflow/docker-compose.yaml down
docker compose down
```
