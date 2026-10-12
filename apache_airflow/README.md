# Airflow integration (local development)

Airflow runs as a separate Compose project, but its task containers need to reach the ELT services from the root `docker-compose.yml`. The root Compose file therefore creates the named network `elt_pipeline_network`; Airflow joins that network in addition to its own private network for the Airflow metadata database and Redis.

## Start the services

Run commands from the repository root. Ensure the root `.env` file contains the same S3 credentials used by the existing stack (`S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_ADMIN_ACCESS_KEY`, `S3_ADMIN_SECRET_KEY`). Keep real credentials out of Git.

1. Start the services Airflow will orchestrate:

   ```bash
   docker compose up -d db mock_api seaweedfs
   ```

2. Build and start Airflow:

   ```bash
   docker compose -f apache_airflow/docker-compose.yaml up --build -d
   ```

3. Open the Airflow UI at [http://localhost:8080](http://localhost:8080). The local Compose defaults create the user `airflow` with password `airflow`; change these defaults for any shared or non-local deployment.

The custom Airflow image includes the Python libraries needed by the existing `ingestion` package and the dbt CLI. The `ingestion/` and `dbt/` directories are mounted read-only into Airflow containers, and their service settings are passed through environment variables. The existing PostgreSQL service is reachable as `db:5432`, MockAPI as `mock_api:8000`, and SeaweedFS S3 as `seaweedfs:8333`.

## Stop Airflow

```bash
docker compose -f apache_airflow/docker-compose.yaml down
```

This leaves the root ELT services and their data volumes running. To stop the ELT services separately, use `docker compose down` from the repository root.
