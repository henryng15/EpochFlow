#!/usr/bin/env bash
# End-to-end smoke test with real processes:
#   1. a coordinator and two workers complete a batch of jobs;
#   2. workers stop gracefully on SIGTERM;
#   3. a worker killed with SIGKILL mid-job loses its lease, and another worker
#      completes the job on attempt 2.
# Usage: smoke_test.sh <directory containing the EpochFlow binaries>
set -euo pipefail

bin="${1:?usage: smoke_test.sh <bin-dir>}"
work="$(mktemp -d)"
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for f in "$work"/*.log; do echo "--- $f" >&2; tail -n 20 "$f" >&2; done
  exit 1
}

# Coordinator on a free port with a short lease so recovery is quick.
"$bin/epochflow-coordinator" --listen=127.0.0.1:0 --lease-ms=1000 --reclaim-interval-ms=100 \
  >"$work/coordinator.out" 2>"$work/coordinator.log" &
pids+=($!)
for _ in $(seq 100); do
  grep -q '^listening on ' "$work/coordinator.out" && break
  sleep 0.05
done
addr="$(sed -n 's/^listening on //p' "$work/coordinator.out")"
[[ -n "$addr" ]] || fail "coordinator did not report its address"
ctl() { "$bin/epochflowctl" --coordinator="$addr" "$@"; }

start_worker() {  # name, work-ms
  "$bin/epochflow-worker" --coordinator="$addr" --worker-id="$1" --work-ms="$2" \
    --idle-backoff-ms=20 2>"$work/$1.log" &
  pids+=($!)
  last_pid=$!
}

# 1. Batch through two workers.
start_worker w1 20; w1=$last_pid
start_worker w2 20; w2=$last_pid
mapfile -t ids < <(ctl submit --payload=hello --count=10)
[[ ${#ids[@]} -eq 10 ]] || fail "expected 10 job ids, got ${#ids[@]}"
ctl wait "${ids[@]}" --timeout-ms=20000 || fail "batch did not complete"
ctl get "${ids[0]}" | grep -qx 'result=processed:hello' || fail "unexpected result"

# 2. Graceful shutdown.
kill -TERM "$w1" "$w2"
wait "$w1" || fail "worker w1 exited with an error after SIGTERM"
wait "$w2" || fail "worker w2 exited with an error after SIGTERM"

# 3. Crash mid-job and recover.
start_worker slow 10000; slow=$last_pid
id="$(ctl submit --payload=crash-me)"
for _ in $(seq 200); do
  ctl get "$id" | grep -qx 'state=RUNNING' && break
  sleep 0.05
done
ctl get "$id" | grep -qx 'worker_id=slow' || fail "slow worker never acquired the job"
{ kill -9 "$slow" && wait "$slow"; } 2>/dev/null || true
start_worker rescuer 20
ctl wait "$id" --timeout-ms=20000 || fail "crashed job was not recovered"
ctl get "$id" | grep -qx 'attempts=2' || fail "expected recovery on attempt 2"
ctl get "$id" | grep -qx 'generation=2' || fail "expected generation 2"
grep -q 'reclaimed' "$work/coordinator.log" || fail "coordinator did not log a reclaim"

echo "PASS"
