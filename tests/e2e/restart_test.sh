#!/usr/bin/env bash
# End-to-end durability test with real processes and PostgreSQL (invariant I6):
# the coordinator and a worker holding a lease are both killed with SIGKILL;
# a restarted coordinator on the same database still knows every job, and the
# interrupted job is recovered on attempt 2.
# Usage: restart_test.sh <bin-dir>. Needs EPOCHFLOW_DSN and psql; exits 77
# (skipped) without them.
set -euo pipefail

bin="${1:?usage: restart_test.sh <bin-dir>}"
if [[ -z "${EPOCHFLOW_DSN:-}" ]] || ! command -v psql >/dev/null; then
  echo "SKIP: needs EPOCHFLOW_DSN and psql"
  exit 77
fi

work="$(mktemp -d)"
schema="e2e_restart_$$_$RANDOM"
dsn="$EPOCHFLOW_DSN options='-csearch_path=$schema'"
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  psql "$EPOCHFLOW_DSN" -qc "DROP SCHEMA IF EXISTS $schema CASCADE" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for f in "$work"/*.log; do echo "--- $f" >&2; tail -n 20 "$f" >&2; done
  exit 1
}

psql "$EPOCHFLOW_DSN" -qc "CREATE SCHEMA $schema" >/dev/null

start_coordinator() {  # listen address, log name
  : >"$work/$2.out"
  "$bin/epochflow-coordinator" --listen="$1" --dsn="$dsn" --lease-ms=3000 \
    --reclaim-interval-ms=100 >"$work/$2.out" 2>"$work/$2.log" &
  pids+=($!)
  coordinator=$!
  for _ in $(seq 200); do
    grep -q '^listening on ' "$work/$2.out" && break
    sleep 0.05
  done
  addr="$(sed -n 's/^listening on //p' "$work/$2.out")"
  [[ -n "$addr" ]] || fail "coordinator ($2) did not start"
}
ctl() { "$bin/epochflowctl" --coordinator="$addr" "$@"; }
field() { ctl get "$1" | sed -n "s/^$2=//p"; }

start_worker() {  # name, work-ms
  "$bin/epochflow-worker" --coordinator="$addr" --worker-id="$1" --work-ms="$2" \
    --idle-backoff-ms=20 2>"$work/$1.log" &
  pids+=($!)
  last_pid=$!
}

start_coordinator 127.0.0.1:0 coordinator1
grep -q 'store=postgres' "$work/coordinator1.log" || fail "coordinator is not using PostgreSQL"
mapfile -t ids < <(ctl submit --payload=durable --count=5)
[[ ${#ids[@]} -eq 5 ]] || fail "expected 5 job ids"

start_worker slow 60000; slow=$last_pid
for _ in $(seq 200); do
  [[ "$(field "${ids[0]}" state)" == RUNNING ]] && break
  sleep 0.05
done
[[ "$(field "${ids[0]}" worker_id)" == slow ]] || fail "slow worker never acquired job ${ids[0]}"

# Crash the coordinator and the lease holder together.
{ kill -9 "$coordinator" "$slow" && wait "$coordinator" "$slow"; } 2>/dev/null || true
start_coordinator "$addr" coordinator2

# Everything committed before the crash is still there.
[[ "$(field "${ids[0]}" state)" == RUNNING ]] || fail "lease holder's job lost its state"
[[ "$(field "${ids[0]}" generation)" == 1 ]] || fail "generation changed across restart"
for id in "${ids[@]:1}"; do
  [[ "$(field "$id" state)" == PENDING ]] || fail "job $id is not PENDING after restart"
done

start_worker w1 20
start_worker w2 20
ctl wait "${ids[@]}" --timeout-ms=30000 || fail "jobs did not complete after restart"
[[ "$(field "${ids[0]}" attempts)" == 2 ]] || fail "interrupted job should finish on attempt 2"
[[ "$(field "${ids[0]}" generation)" == 2 ]] || fail "interrupted job should finish at generation 2"
for id in "${ids[@]:1}"; do
  [[ "$(field "$id" attempts)" == 1 ]] || fail "job $id ran more than once"
done
[[ "$(psql "$dsn" -tAc 'SELECT count(*) FROM jobs')" == 5 ]] || fail "unexpected job count"

echo "PASS"
