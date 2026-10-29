#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

compose_file="apache_airflow/docker-compose.yaml"
compose=(docker compose -f "$compose_file")
env_file="$repo_root/.env.airflow-ci"

export S3_ACCESS_KEY="ci-ingestion"
export S3_SECRET_KEY="ci-secret"
export S3_ADMIN_ACCESS_KEY="ci-admin"
export S3_ADMIN_SECRET_KEY="ci-admin-secret"
export POSTGRES_PASSWORD="postgre_password"
export POSTGRES_HOST="db"
export POSTGRES_PORT="5432"
export POSTGRES_DB="elt_pipeline"
export POSTGRES_USER="postgre_user"
export S3_BUCKET="elt-raw"
export S3_ENDPOINT="http://seaweedfs:8333"
export METABASE_URL="http://metabase:3000"
export METABASE_ADMIN_EMAIL="ci-admin@example.com"
export METABASE_ADMIN_PASSWORD="CI-Mb!7qR2-vN9xK4p"
export API_URL="http://mock_api:8000"
export FAILURE_RATE="0"
export DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
export FERNET_KEY=""
export AIRFLOW__API_AUTH__JWT_SECRET="airflow-ci-jwt-secret"
export ENV_FILE_PATH="../.env.airflow-ci"

cleanup() {
  status=$?
  # Airflow containers may create host-mounted log directories as UID 50000.
  # Restore runner ownership before collecting/uploading artifacts.
  sudo chown -R "$(id -u):$(id -g)" "$repo_root/logs" 2>/dev/null || true
  mkdir -p "$repo_root/logs/ci"
  "${compose[@]}" ps > "$repo_root/logs/ci/airflow-services-status.txt" 2>&1 || true
  "${compose[@]}" logs --no-color > "$repo_root/logs/ci/airflow-services.log" 2>&1 || true
  docker compose ps > "$repo_root/logs/ci/elt-services-status.txt" 2>&1 || true
  docker compose logs --no-color > "$repo_root/logs/ci/elt-services.log" 2>&1 || true
  if [[ "$status" -ne 0 ]]; then
    "${compose[@]}" ps || true
    "${compose[@]}" logs --no-color || true
    docker compose ps || true
    docker compose logs --no-color || true
  fi
  if [[ "${KEEP_AIRFLOW_TEST_STACK:-0}" != "1" ]]; then
    "${compose[@]}" down --volumes --remove-orphans || true
    docker compose down --volumes --remove-orphans || true
    rm -f "$env_file"
  else
    echo "Keeping Airflow test stack for debugging (KEEP_AIRFLOW_TEST_STACK=1)."
  fi

  # GitHub Actions artifacts must be portable across filesystems such as NTFS.
  # Airflow run IDs contain colons in log directory names. Normalize the tree
  # after log collection, updating os.walk's directory list as names change.
  python - "$repo_root/logs" <<'PY'
import os
import re
import sys

root = sys.argv[1]
invalid_chars = re.compile(r'[<>:"|?*\r\n]')

def safe_destination(parent, name):
    safe_name = invalid_chars.sub("_", name)
    destination = os.path.join(parent, safe_name)
    suffix = 1
    while os.path.exists(destination) and destination != os.path.join(parent, name):
        stem, extension = os.path.splitext(safe_name)
        destination = os.path.join(parent, f"{stem}_{suffix}{extension}")
        suffix += 1
    return safe_name, destination

for current, directories, files in os.walk(root, topdown=True):
    for index, name in enumerate(directories):
        safe_name, destination = safe_destination(current, name)
        if safe_name != name:
            os.replace(os.path.join(current, name), destination)
            directories[index] = safe_name
            print(f"Sanitized artifact directory: {name!r} -> {safe_name!r}")

    for name in files:
        safe_name, destination = safe_destination(current, name)
        if safe_name != name:
            os.replace(os.path.join(current, name), destination)
            print(f"Sanitized artifact file: {name!r} -> {safe_name!r}")

# Fail before upload if any unsupported character remains in a path component.
remaining = []
for current, directories, files in os.walk(root):
    remaining.extend(
        os.path.join(current, name)
        for name in directories + files
        if invalid_chars.search(name)
    )
if remaining:
    print("Unsupported characters remain in artifact paths:", file=sys.stderr)
    print("\n".join(remaining), file=sys.stderr)
    raise SystemExit(1)
PY
}
trap cleanup EXIT

cat > "$env_file" <<'EOF'
S3_ACCESS_KEY=ci-ingestion
S3_SECRET_KEY=ci-secret
S3_ADMIN_ACCESS_KEY=ci-admin
S3_ADMIN_SECRET_KEY=ci-admin-secret
POSTGRES_PASSWORD=postgre_password
EOF

docker compose config --quiet
docker compose build ingestion dbt
docker compose up -d --wait --wait-timeout 300 db mock_api seaweedfs
docker compose run --rm -e S3_ADMIN_ACCESS_KEY -e S3_ADMIN_SECRET_KEY -v "$repo_root/tests:/tests:ro" ingestion python /tests/integration/bootstrap_s3.py

# dag2 verifies Metabase through its authenticated API, so prepare the same
# data source used by the regular integration test before starting Airflow.
docker compose up -d --wait --wait-timeout 180 metabase
docker compose run --rm -e METABASE_ADMIN_EMAIL -e METABASE_ADMIN_PASSWORD -v "$repo_root/tests:/tests:ro" ingestion python /tests/integration/configure_metabase.py

"${compose[@]}" config --quiet
"${compose[@]}" up --build -d --wait --wait-timeout 600 airflow-apiserver airflow-scheduler airflow-worker airflow-dag-processor
curl --fail --silent --show-error --retry 12 --retry-delay 5 --retry-connrefused \
  http://localhost:8080/api/v2/monitor/health

echo
echo "Airflow API server started and passed its health endpoint."

"${compose[@]}" exec -T airflow-worker env PYTHONPATH=/opt/airflow/dags python -m unittest discover -s /opt/airflow/tests -v
"${compose[@]}" exec -T airflow-worker airflow dags test elt_pipeline 2026-10-13

# The successful dag1 run triggers dag2 automatically. Wait for its own run
# to finish so this smoke test covers S3 -> Bronze -> Mart -> Metabase.
# Keep the DAG ID filter in the JSON parser instead of the deprecated shorthand
# CLI flag, and allow up to 15 minutes for the end-to-end verification.
verification_succeeded=0
for attempt in $(seq 1 180); do
  verification_runs="$("${compose[@]}" exec -T airflow-worker airflow dags list-runs --output json --limit 100 2>&1 || true)"
  verification_state="$(python -c '
import json
import sys

try:
    payload = json.load(sys.stdin)
except (json.JSONDecodeError, TypeError):
    print("")
    raise SystemExit(0)

if isinstance(payload, dict):
    runs = payload.get("dag_runs", payload.get("runs", []))
else:
    runs = payload

matches = [
    run for run in runs
    if run.get("dag_id") == "elt_pipeline_verification"
    and str(run.get("run_id", "")).startswith("verify__")
]
matches.sort(key=lambda run: str(run.get("start_date", "")), reverse=True)
print(matches[0].get("state", "") if matches else "")
' <<< "$verification_runs" 2>/dev/null || true)"
  if [[ "$verification_state" == "success" ]]; then
    echo "$verification_runs"
    verification_succeeded=1
    break
  fi
  if [[ "$verification_state" == "failed" ]]; then
    echo "$verification_runs" >&2
    "${compose[@]}" logs --no-color airflow-worker airflow-scheduler airflow-dag-processor >&2 || true
    exit 1
  fi
  sleep 5
done
if [[ "$verification_succeeded" -ne 1 ]]; then
  echo "dag2 did not complete successfully within 900 seconds." >&2
  "${compose[@]}" exec -T airflow-worker airflow dags list-runs --output json --limit 100 || true
  exit 1
fi

docker compose run --rm -e S3_ADMIN_ACCESS_KEY -e S3_ADMIN_SECRET_KEY -v "$repo_root/tests:/tests:ro" ingestion python /tests/integration/verify_airflow_logs.py
for table in orders deliveries inventory; do
  row_count="$(docker compose exec -T db psql -U postgre_user -d elt_pipeline -Atqc "SELECT COUNT(*) FROM bronze.$table")"
  if [[ ! "$row_count" =~ ^[0-9]+$ || "$row_count" -le 0 ]]; then
    echo "Expected ingestion to load rows into bronze.$table; got: $row_count" >&2
    exit 1
  fi
  echo "bronze.$table contains $row_count rows."
done

for table in inventory supplier_reliability; do
  row_count="$(docker compose exec -T db psql -U postgre_user -d elt_pipeline -Atqc "SELECT COUNT(*) FROM mart.$table")"
  if [[ ! "$row_count" =~ ^[0-9]+$ || "$row_count" -le 0 ]]; then
    echo "Expected dbt to create mart.$table with rows; got: $row_count" >&2
    exit 1
  fi
  echo "mart.$table contains $row_count rows."
done

echo "Airflow DAG ran isolated ingestion and dbt containers; Bronze and Mart contain rows."
