#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

compose_file="apache_airflow/docker-compose.yaml"
compose=(docker compose -f "$compose_file")
env_file="$repo_root/.env.airflow-ci"
network_name="elt_pipeline_network"
created_network=false

export S3_ACCESS_KEY="ci-ingestion"
export S3_SECRET_KEY="ci-secret"
export S3_ADMIN_ACCESS_KEY="ci-admin"
export S3_ADMIN_SECRET_KEY="ci-admin-secret"
export POSTGRES_PASSWORD="postgre_password"
export FERNET_KEY=""
export AIRFLOW__API_AUTH__JWT_SECRET="airflow-ci-jwt-secret"
export ENV_FILE_PATH="../.env.airflow-ci"

cleanup() {
  status=$?
  if [[ "$status" -ne 0 ]]; then
    "${compose[@]}" ps || true
    "${compose[@]}" logs --no-color || true
  fi
  if [[ "${KEEP_AIRFLOW_TEST_STACK:-0}" != "1" ]]; then
    "${compose[@]}" down --volumes --remove-orphans || true
    rm -f "$env_file"
    if [[ "$created_network" == "true" ]]; then
      docker network rm "$network_name" >/dev/null 2>&1 || true
    fi
  else
    echo "Keeping Airflow test stack for debugging (KEEP_AIRFLOW_TEST_STACK=1)."
  fi
}
trap cleanup EXIT

cat > "$env_file" <<'EOF'
S3_ACCESS_KEY=ci-ingestion
S3_SECRET_KEY=ci-secret
S3_ADMIN_ACCESS_KEY=ci-admin
S3_ADMIN_SECRET_KEY=ci-admin-secret
POSTGRES_PASSWORD=postgre_password
EOF

if ! docker network inspect "$network_name" >/dev/null 2>&1; then
  docker network create "$network_name" >/dev/null
  created_network=true
fi

"${compose[@]}" config --quiet
"${compose[@]}" up --build -d --wait --wait-timeout 600 airflow-apiserver
curl --fail --silent --show-error --retry 12 --retry-delay 5 --retry-connrefused \
  http://localhost:8080/api/v2/monitor/health

echo
echo "Airflow API server started and passed its health endpoint."
