#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

compose_file="apache_airflow/docker-compose.yaml"
airflow_compose=(docker compose -f "$compose_file")
root_compose=(docker compose)
env_file="$repo_root/.env.airflow-diagnostics"

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
export DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
export FERNET_KEY=""
export AIRFLOW__API_AUTH__JWT_SECRET="airflow-diagnostics-ci-jwt-secret"
export ENV_FILE_PATH="../.env.airflow-diagnostics"

cleanup() {
  status=$?
  collection_tmp="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/airflow-diagnostics-logs.XXXXXX")"

  "${airflow_compose[@]}" ps > "$collection_tmp/airflow-services-status.txt" 2>&1 || true
  "${airflow_compose[@]}" logs --no-color > "$collection_tmp/airflow-services.log" 2>&1 || true
  "${root_compose[@]}" ps > "$collection_tmp/s3-services-status.txt" 2>&1 || true
  "${root_compose[@]}" logs --no-color seaweedfs > "$collection_tmp/seaweedfs.log" 2>&1 || true

  if [[ "$status" -ne 0 ]]; then
    cat "$collection_tmp/airflow-services-status.txt" || true
    cat "$collection_tmp/airflow-services.log" || true
    cat "$collection_tmp/s3-services-status.txt" || true
    cat "$collection_tmp/seaweedfs.log" || true
  fi

  "${airflow_compose[@]}" down --volumes --remove-orphans || true
  "${root_compose[@]}" down --remove-orphans || true
  rm -f "$env_file"

  if [[ -d "$repo_root/logs" ]]; then
    sudo chown -R "$(id -u):$(id -g)" "$repo_root/logs" 2>/dev/null || true
  fi

  mkdir -p "$repo_root/logs/ci"
  mv "$collection_tmp/airflow-services-status.txt" "$repo_root/logs/ci/diagnostics-airflow-services-status.txt"
  mv "$collection_tmp/airflow-services.log" "$repo_root/logs/ci/diagnostics-airflow-services.log"
  mv "$collection_tmp/s3-services-status.txt" "$repo_root/logs/ci/diagnostics-s3-services-status.txt"
  mv "$collection_tmp/seaweedfs.log" "$repo_root/logs/ci/diagnostics-seaweedfs.log"
  rmdir "$collection_tmp"

  # Airflow run IDs contain colons; normalize artifact paths for portability.
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

remaining = [
    os.path.join(current, name)
    for current, directories, files in os.walk(root)
    for name in directories + files
    if invalid_chars.search(name)
]
if remaining:
    print("Unsupported characters remain in artifact paths:", file=sys.stderr)
    print("\n".join(remaining), file=sys.stderr)
    raise SystemExit(1)
PY

  return "$status"
}
trap cleanup EXIT

cat > "$env_file" <<'EOF'
S3_ACCESS_KEY=ci-ingestion
S3_SECRET_KEY=ci-secret
S3_ADMIN_ACCESS_KEY=ci-admin
S3_ADMIN_SECRET_KEY=ci-admin-secret
POSTGRES_PASSWORD=postgre_password
EOF

mkdir -p "$repo_root/logs/airflow"
"${root_compose[@]}" config --quiet
"${root_compose[@]}" build ingestion
"${root_compose[@]}" up -d --wait --wait-timeout 300 seaweedfs
"${root_compose[@]}" run --rm --no-deps \
  -e S3_ADMIN_ACCESS_KEY -e S3_ADMIN_SECRET_KEY \
  -v "$repo_root/tests:/tests:ro" ingestion python /tests/integration/bootstrap_s3.py

"${airflow_compose[@]}" config --quiet
"${airflow_compose[@]}" up --build -d --wait --wait-timeout 600 \
  airflow-apiserver airflow-scheduler airflow-worker airflow-dag-processor

curl --fail --silent --show-error --retry 12 --retry-delay 5 --retry-connrefused \
  http://localhost:8080/api/v2/monitor/health

dag_id="airflow_logging_diagnostics"
run_id="diagnostics__${GITHUB_RUN_ID:-local}_${GITHUB_RUN_ATTEMPT:-1}"

# The API health endpoint can become ready before the DAG processor has
# registered the diagnostic DAG. Wait for registration before unpausing it.
dag_loaded=0
for attempt in $(seq 1 60); do
  if "${airflow_compose[@]}" exec -T airflow-worker airflow dags list 2>/dev/null \
    | grep -Fq "$dag_id"; then
    dag_loaded=1
    break
  fi
  sleep 2
done
if [[ "$dag_loaded" -ne 1 ]]; then
  echo "Airflow did not register DAG $dag_id within 120 seconds." >&2
  "${airflow_compose[@]}" exec -T airflow-worker airflow dags list || true
  exit 1
fi

"${airflow_compose[@]}" exec -T airflow-worker airflow dags unpause "$dag_id"
"${airflow_compose[@]}" exec -T airflow-worker airflow dags trigger "$dag_id" --run-id "$run_id"

diagnostic_state=""
for attempt in $(seq 1 150); do
  runs_json="$("${airflow_compose[@]}" exec -T airflow-worker \
    airflow dags list-runs "$dag_id" --output json 2>/dev/null || true)"
  diagnostic_state="$(python -c '
import json
import sys

raw = sys.stdin.read()
payload = None
for index, character in enumerate(raw):
    if character not in "[{":
        continue
    try:
        payload, _ = json.JSONDecoder().raw_decode(raw[index:])
        break
    except json.JSONDecodeError:
        continue
if payload is None:
    print("")
    raise SystemExit(0)

if isinstance(payload, dict):
    runs = payload.get("dag_runs", payload.get("runs", []))
else:
    runs = payload

matches = [run for run in runs if run.get("run_id") == sys.argv[1]
           and run.get("dag_id") == sys.argv[2]]
print(matches[0].get("state", "") if matches else "")
' "$run_id" "$dag_id" <<< "$runs_json" 2>/dev/null || true)"

  if [[ "$diagnostic_state" == "failed" ]]; then
    echo "Diagnostic DAG reached its expected failed state."
    break
  fi
  if [[ "$diagnostic_state" == "success" ]]; then
    echo "Diagnostic DAG unexpectedly succeeded; expected two controlled failures." >&2
    exit 1
  fi
  sleep 2
done

if [[ "$diagnostic_state" != "failed" ]]; then
  echo "Diagnostic DAG did not reach the expected failed state within 300 seconds." >&2
  "${airflow_compose[@]}" exec -T airflow-worker airflow dags list-runs "$dag_id" --output json || true
  exit 1
fi

docker compose run --rm --no-deps \
  -e S3_ENDPOINT -e S3_BUCKET -e S3_ADMIN_ACCESS_KEY -e S3_ADMIN_SECRET_KEY \
  -e DIAGNOSTIC_RUN_ID="$run_id" \
  -v "$repo_root/tests:/tests:ro" \
  ingestion python /tests/integration/verify_airflow_diagnostics.py

echo "Airflow logging diagnostics passed: expected runtime error, Airflow timeout, and success were recorded in S3."
