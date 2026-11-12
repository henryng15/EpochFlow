#!/usr/bin/env bash
# Crash-and-pause demo on the Docker Compose deployment.
#   1. starts PostgreSQL, the coordinator, and three workers;
#   2. submits payments;
#   3. kills one worker container and freezes another (docker pause) for longer
#      than the lease, then brings both back;
#   4. waits for every job, runs the invariant checker, and prints a summary.
# Usage: scripts/demo-docker.sh [--jobs N] [--down]
#   --down  remove the deployment and its data afterwards
set -euo pipefail

cd "$(dirname "$0")/.."
jobs=30
down=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --jobs) jobs="$2"; shift 2 ;;
    --down) down=true; shift ;;
    *) echo "usage: $0 [--jobs N] [--down]" >&2; exit 2 ;;
  esac
done

compose=(docker compose)
ctl() { "${compose[@]}" run --rm --no-deps -T ctl "$@"; }
dsn="host=postgres user=epochflow password=epochflow dbname=epochflow"
psql_c() { "${compose[@]}" exec -T postgres psql -U epochflow -d epochflow -X -q -tA -F '|' "$@"; }
step() { printf '\n== %s\n' "$*"; }

step "Starting PostgreSQL, coordinator, and 3 workers"
"${compose[@]}" up -d --build --wait

step "Submitting $jobs payments"
mapfile -t ids < <(ctl submit --payload="task=payment;account=demo;amount_cents=100" --count="$jobs")
echo "job ids ${ids[0]}..${ids[-1]}"

sleep 1
workers=($("${compose[@]}" ps -q worker))
step "Killing worker container $(docker inspect -f '{{.Name}}' "${workers[0]}")"
docker kill "${workers[0]}" >/dev/null
step "Freezing worker container $(docker inspect -f '{{.Name}}' "${workers[1]}") for 5 s (lease is 3 s)"
docker pause "${workers[1]}" >/dev/null
sleep 5
docker unpause "${workers[1]}" >/dev/null
docker start "${workers[0]}" >/dev/null
echo "both workers are back"

step "Waiting for every job"
ctl wait "${ids[@]}" --timeout-ms=120000
sleep 4  # let the unfrozen worker's stale requests arrive

step "Invariant check (no rows = all invariants hold)"
violations=$("${compose[@]}" run --rm --no-deps -T --entrypoint psql ctl "$dsn" -X -q -tA -F '|' \
  -f /usr/local/share/epochflow/check-invariants.sql)
if [[ -n "$violations" ]]; then
  echo "$violations"
  status=1
else
  echo "no violations"
  status=0
fi

step "Summary"
psql_c -c "SELECT 'jobs completed', count(*) FROM jobs WHERE state = 'COMPLETED'
           UNION ALL SELECT 'attempts recovered after lease expiry', count(*) FROM job_attempts WHERE outcome = 'LEASE_EXPIRED'
           UNION ALL SELECT 'ledger entries', count(*) FROM ledger_entries
           UNION ALL SELECT 'duplicate ledger entries', count(*) - count(DISTINCT job_id) FROM ledger_entries" |
  awk -F'|' '{ printf "  %-40s %s\n", $1, $2 }'
log=$("${compose[@]}" logs --no-color coordinator)
printf '  %-40s %s\n' "reclaims logged" "$(grep -c 'reclaimed' <<<"$log" || true)"
printf '  %-40s %s\n' "stale requests rejected" "$(grep -cE 'rejected (completion|operation).*STALE' <<<"$log" || true)"
printf '  %-40s %s\n' "effects deduplicated" "$(grep -c 'deduplicated operation' <<<"$log" || true)"

if $down; then
  step "Removing the deployment"
  "${compose[@]}" down -v
else
  printf '\nThe deployment is still running; remove it with: docker compose down -v\n'
fi
exit "$status"
