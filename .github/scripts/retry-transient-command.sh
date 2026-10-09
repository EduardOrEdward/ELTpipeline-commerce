#!/usr/bin/env bash
# Retry Docker commands only for transient registry/network failures.
set -uo pipefail

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 COMMAND [ARG ...]" >&2
  exit 2
fi

max_attempts=4
log_file="$(mktemp)"
trap 'rm -f "$log_file"' EXIT

for ((attempt = 1; attempt <= max_attempts; attempt++)); do
  echo "::group::Attempt ${attempt}/${max_attempts}: $*"
  "$@" 2>&1 | tee "$log_file"
  status=${PIPESTATUS[0]}
  echo "::endgroup::"

  if [[ $status -eq 0 ]]; then
    exit 0
  fi

  if ! grep -Eiq '429 Too Many Requests|408 Request Timeout|500 Internal Server Error|502 Bad Gateway|503 Service Unavailable|504 Gateway Timeout|TLS handshake timeout|unexpected EOF|connection reset by peer|i/o timeout|temporary failure in name resolution' "$log_file"; then
    echo "Command failed with exit code $status; error does not look transient, so it will not be retried." >&2
    exit "$status"
  fi

  if [[ $attempt -eq $max_attempts ]]; then
    echo "Command still fails after $max_attempts attempts." >&2
    exit "$status"
  fi

  delay=$((5 * (2 ** (attempt - 1))))
  echo "Transient Docker registry/network error detected. Retrying in ${delay}s..." >&2
  sleep "$delay"
done
