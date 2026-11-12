#!/usr/bin/env python3
"""Chaos benchmark: payments through EpochFlow while workers are killed and paused.

Runs a coordinator and workers against PostgreSQL, submits payment jobs, and
injects chaos until every job finishes: SIGKILL of a random worker (restarted
shortly after) or SIGSTOP longer than the lease (resumed later, so it comes
back stale). Then it reports throughput, recovery latency, retries, rejected
stale requests, deduplicated effects, ledger correctness, and invariant
violations.

Modes:
  epochflow  payments carry a stable operation id (idempotent)
  naive      the same workload with a fresh operation id per attempt, as a
             client without idempotency keys would send: duplicates become real
             ledger rows. Fencing stays on; its rejections are reported as what a
             system without fencing would have accepted.
  compare    both, replaying the same seeded chaos schedule, side by side

Example:
  scripts/bench.py --bin build/bin --dsn "$(scripts/test-db.sh dsn)" --mode compare

Standard library only; needs psql. See docs/operations.md.
"""

import argparse
import json
import os
import random
import re
import signal
import statistics
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from epochflow_cluster import Cluster  # noqa: E402

AMOUNT_CENTS = 100


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    k = max(0, min(len(ordered) - 1, round(p / 100 * (len(ordered) - 1))))
    return ordered[k]


def chaos_schedule(args, rng):
    """Pre-draws (delay_s, action, worker) for the whole run from the seed, so
    every mode replays the same schedule regardless of timing."""
    schedule = []
    for _ in range(10_000):
        schedule.append((rng.uniform(0.5, 1.5) * args.chaos_interval_ms / 1000,
                         "kill" if rng.random() < args.kill_ratio else "pause",
                         f"w{rng.randrange(args.workers) + 1}"))
    return schedule


class Chaos(threading.Thread):
    """Replays a chaos schedule until stopped. An event is skipped (and still
    consumed) if its worker is already paused. Every worker may be down at
    once for a moment; killed workers restart immediately and paused ones
    resume after 1.5 leases, so progress always resumes (and a single-worker
    run still sees chaos)."""

    def __init__(self, cluster, args, schedule):
        super().__init__(daemon=True)
        self.cluster, self.args, self.schedule = cluster, args, schedule
        self.events = []  # (unix time, worker, action) actually applied
        self.paused = {}  # worker -> resume deadline (monotonic)
        self.stop = threading.Event()
        self.lock = threading.Lock()

    def start_worker(self, name):
        self.cluster.start_worker(name, work_ms=self.args.work_ms,
                                  work_after_effect_ms=self.args.work_after_effect_ms)

    def run(self):
        for delay, action, name in self.schedule:
            if self.stop.wait(delay):
                return
            with self.lock:
                self.resume_due()
                if name in self.paused or name not in self.cluster.processes:
                    continue
                proc = self.cluster.processes[name]
                self.events.append((time.time(), name, action))
                if action == "kill":
                    proc.send_signal(signal.SIGKILL)
                    proc.wait()
                    self.start_worker(name)
                else:
                    proc.send_signal(signal.SIGSTOP)
                    self.paused[name] = time.monotonic() + 1.5 * self.args.lease_ms / 1000

    def resume_due(self, everything=False):
        for name, deadline in list(self.paused.items()):
            if everything or time.monotonic() >= deadline:
                self.cluster.processes[name].send_signal(signal.SIGCONT)
                del self.paused[name]

    def finish(self):
        self.stop.set()
        self.join()
        with self.lock:
            self.resume_due(everything=True)


def run(args, mode, seed):
    schedule = chaos_schedule(args, random.Random(seed))
    c = Cluster(args.bin, args.dsn, prefix=f"bench_{mode}", lease_ms=args.lease_ms,
                reclaim_ms=args.reclaim_ms)
    try:
        c.start_coordinator()
        chaos = Chaos(c, args, schedule)
        for i in range(args.workers):
            chaos.start_worker(f"w{i + 1}")

        payload = f"task=payment;account=acct;amount_cents={AMOUNT_CENTS}"
        if mode == "naive":
            payload += ";idempotency=off"
        started = time.time()
        c.ctl("submit", f"--payload={payload}", f"--count={args.jobs}")
        chaos.start()
        c.wait_for(lambda: c.psql("SELECT count(*) FROM jobs WHERE state NOT IN "
                                  "('COMPLETED', 'FAILED')") == ["0"],
                   "all jobs to finish", timeout=args.timeout_s)
        chaos.finish()
        # Let resumed workers send their stale requests before counting.
        time.sleep(2 * args.lease_ms / 1000)
        return measure(c, mode, started, chaos.events)
    except Exception as e:
        raise RuntimeError(f"{mode} run failed: {e}\n{c.logs()}") from e
    finally:
        c.close()


def measure(c, mode, started, events):
    [jobs_line] = c.psql(
        "SELECT count(*) FILTER (WHERE state = 'COMPLETED'), count(*) FILTER (WHERE state = "
        "'FAILED'), coalesce(sum(attempts) - count(*), 0), "
        "extract(epoch FROM max(updated_at) FILTER (WHERE state = 'COMPLETED')) FROM jobs")
    completed, failed, retries, last_done = jobs_line.split("|")
    duration = float(last_done) - started if last_done else None

    [ledger_line] = c.psql("SELECT count(*), count(DISTINCT job_id), coalesce(sum(amount_cents), 0) "
                           "FROM ledger_entries")
    entries, jobs_with_entry, charged = (int(x) for x in ledger_line.split("|"))

    attempts = {}
    for line in c.psql("SELECT job_id, generation, worker_id, extract(epoch FROM started_at), "
                       "extract(epoch FROM finished_at), coalesce(outcome, '') FROM job_attempts"):
        job, gen, worker, start, finish, outcome = line.split("|")
        attempts[(int(job), int(gen))] = (worker, float(start), float(finish) if finish else None,
                                          outcome)

    # Recovery latency: from a chaos event to the next attempt of each job the
    # affected worker held at that moment.
    recoveries = []
    for at, worker, _ in events:
        for (job, gen), (w, start, finish, outcome) in attempts.items():
            held = w == worker and start <= at and outcome == "LEASE_EXPIRED" and finish >= at
            nxt = attempts.get((job, gen + 1))
            if held and nxt:
                recoveries.append(nxt[1] - at)

    log = c.log("coordinator")
    stale = len(re.findall(r"rejected (?:completion|operation)\S* .*status=STALE", log))
    deduplicated = log.count("deduplicated operation=")

    return {
        "mode": mode,
        "jobs_completed": int(completed),
        "jobs_failed": int(failed),
        "duration_s": round(duration, 2) if duration else None,
        "throughput_jobs_per_s": round(int(completed) / duration, 1) if duration else None,
        "chaos_events": len(events),
        "kills": sum(1 for e in events if e[2] == "kill"),
        "pauses": sum(1 for e in events if e[2] == "pause"),
        "retries": int(retries),
        "lease_expirations": sum(1 for a in attempts.values() if a[3] == "LEASE_EXPIRED"),
        "recovery_p50_s": round(statistics.median(recoveries), 2) if recoveries else None,
        "recovery_p95_s": round(percentile(recoveries, 95), 2) if recoveries else None,
        "recovery_max_s": round(max(recoveries), 2) if recoveries else None,
        "recovery_samples_s": [round(r, 3) for r in recoveries],
        "stale_requests_rejected": stale,
        "effects_deduplicated": deduplicated,
        "ledger_entries": entries,
        "duplicate_ledger_entries": entries - jobs_with_entry,
        "overcharged_cents": charged - jobs_with_entry * AMOUNT_CENTS,
        "invariant_violations": len(c.violations()),
    }


ROWS = [
    ("jobs_completed", "Jobs completed"),
    ("jobs_failed", "Jobs failed"),
    ("duration_s", "Duration (s)"),
    ("throughput_jobs_per_s", "Throughput (jobs/s)"),
    ("chaos_events", "Chaos events (kill / pause)"),
    ("retries", "Retries"),
    ("recovery_p50_s", "Recovery latency p50 (s)"),
    ("recovery_p95_s", "Recovery latency p95 (s)"),
    ("recovery_max_s", "Recovery latency max (s)"),
    ("stale_requests_rejected", "Stale requests rejected by fencing"),
    ("effects_deduplicated", "Effects deduplicated"),
    ("ledger_entries", "Ledger entries"),
    ("duplicate_ledger_entries", "Duplicate ledger entries"),
    ("overcharged_cents", "Overcharged (cents)"),
    ("invariant_violations", "Invariant violations"),
]


def cell(result, key):
    if key == "chaos_events":
        return f"{result['chaos_events']} ({result['kills']} / {result['pauses']})"
    value = result[key]
    return "n/a" if value is None else str(value)


def print_table(results):
    header = "| Metric | " + " | ".join(r["mode"] for r in results) + " |"
    print(header)
    print("| --- |" + " --- |" * len(results))
    for key, label in ROWS:
        print(f"| {label} | " + " | ".join(cell(r, key) for r in results) + " |")


def build_parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bin", required=True, help="directory with the EpochFlow binaries")
    p.add_argument("--dsn", required=True, help="PostgreSQL connection string")
    p.add_argument("--mode", choices=["epochflow", "naive", "compare"], default="compare")
    p.add_argument("--jobs", type=int, default=200)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--lease-ms", type=int, default=1000)
    p.add_argument("--reclaim-ms", type=int, default=100)
    p.add_argument("--work-ms", type=int, default=30, help="per job, before the effect")
    p.add_argument("--work-after-effect-ms", type=int, default=30, help="per job, after the effect")
    p.add_argument("--chaos-interval-ms", type=int, default=500)
    p.add_argument("--kill-ratio", type=float, default=0.6, help="share of kills vs pauses")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--timeout-s", type=int, default=300)
    p.add_argument("--json", help="also write results to this file")
    p.add_argument("--check", action="store_true",
                   help="exit 1 unless the epochflow run has every job completed, no duplicate "
                        "entries, no overcharge, and no invariant violations")
    return p


def main():
    args = build_parser().parse_args()

    modes = ["epochflow", "naive"] if args.mode == "compare" else [args.mode]
    results = [run(args, mode, args.seed) for mode in modes]
    print_table(results)
    if args.json:
        with open(args.json, "w") as f:
            json.dump({"settings": vars(args), "results": results}, f, indent=2)

    if args.check:
        for r in results:
            if r["mode"] != "epochflow":
                continue
            problems = [k for k in ("duplicate_ledger_entries", "overcharged_cents",
                                    "invariant_violations", "jobs_failed") if r[k] != 0]
            if r["jobs_completed"] != args.jobs:
                problems.append("jobs_completed")
            if problems:
                print(f"CHECK FAILED: {', '.join(problems)}", file=sys.stderr)
                return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
