#!/usr/bin/env bash
# Starts or stops the disposable integration-test PostgreSQL.
#   scripts/test-db.sh up    start and wait until it accepts connections
#   scripts/test-db.sh down  stop and discard all data
#   scripts/test-db.sh dsn   print the DSN to export as EPOCHFLOW_DSN
set -euo pipefail

cd "$(dirname "$0")/.."
compose=(docker compose -f docker-compose.test.yml)
dsn="host=127.0.0.1 port=15432 user=epochflow password=epochflow dbname=epochflow_test"

case "${1:-}" in
  up)
    "${compose[@]}" up -d --wait
    echo "export EPOCHFLOW_DSN='${dsn}'"
    ;;
  down)
    "${compose[@]}" down
    ;;
  dsn)
    echo "${dsn}"
    ;;
  *)
    echo "usage: $0 up|down|dsn" >&2
    exit 2
    ;;
esac
