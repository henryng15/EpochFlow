#!/usr/bin/env python3
"""Fault-injection scenarios with real EpochFlow processes and PostgreSQL.

Each scenario starts a coordinator and workers in a fresh database schema,
injects one failure (worker --fault, SIGSTOP/SIGCONT, SIGKILL of the
coordinator), and checks the documented outcome plus every invariant in
scripts/check-invariants.sql. See docs/testing.md.

Environment: EPOCHFLOW_BIN (directory with the binaries) and EPOCHFLOW_DSN.
Exits 77 (skipped) when EPOCHFLOW_DSN or psql is missing. Standard library only.
"""

import functools
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

BIN = os.environ.get("EPOCHFLOW_BIN", "")
DSN = os.environ.get("EPOCHFLOW_DSN", "")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CHECKER = os.path.join(REPO, "scripts", "check-invariants.sql")

LEASE_MS = 1500
RECLAIM_MS = 100
TIMEOUT_S = 30


class Cluster:
    """One coordinator, its workers, a private schema, and a provider log."""

    _counter = 0

    def __init__(self):
        Cluster._counter += 1
        self.schema = f"faults_{os.getpid()}_{Cluster._counter}"
        self.dir = tempfile.mkdtemp(prefix="epochflow-faults-")
        self.provider_log = os.path.join(self.dir, "provider.log")
        self.dsn = f"{DSN} options='-csearch_path={self.schema}'"
        self.processes = {}
        self.files = []
        self.coordinator = None
        self.address = None
        self.psql(f"CREATE SCHEMA {self.schema}", dsn=DSN)

    # --- processes -------------------------------------------------------

    def open(self, name, mode):
        f = open(os.path.join(self.dir, name), mode)
        self.files.append(f)
        return f

    def read(self, name):
        with open(os.path.join(self.dir, name)) as f:
            return f.read()

    def start_coordinator(self, listen="127.0.0.1:0"):
        self.coordinator = subprocess.Popen(
            [f"{BIN}/epochflow-coordinator", f"--listen={listen}", f"--dsn={self.dsn}",
             f"--lease-ms={LEASE_MS}", f"--reclaim-interval-ms={RECLAIM_MS}"],
            stdout=self.open("coordinator.out", "w"), stderr=self.open("coordinator.log", "a"))
        self.wait_for(lambda: "listening on" in self.read("coordinator.out"),
                      "coordinator to listen")
        self.address = re.search(r"listening on (\S+)", self.read("coordinator.out")).group(1)

    def kill_coordinator(self):
        self.coordinator.kill()
        self.coordinator.wait()

    def start_worker(self, name, fault=None, work_ms=0):
        args = [f"{BIN}/epochflow-worker", f"--coordinator={self.address}", f"--worker-id={name}",
                f"--provider-log={self.provider_log}", "--idle-backoff-ms=20",
                "--rpc-timeout-ms=500", f"--work-ms={work_ms}"]
        if fault:
            args.append(f"--fault={fault}")
        proc = subprocess.Popen(args, stderr=self.open(f"{name}.log", "a"))
        self.processes[name] = proc
        return proc

    def log(self, name):
        return self.read(f"{name}.log")

    def state(self, name):
        """Kernel process state letter: 'T' when stopped by SIGSTOP."""
        with open(f"/proc/{self.processes[name].pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0]

    def close(self):
        procs = list(self.processes.values()) + ([self.coordinator] if self.coordinator else [])
        for p in procs:
            if p.poll() is None:
                os.kill(p.pid, signal.SIGCONT)
                p.kill()
            p.wait()
        for f in self.files:
            f.close()
        self.psql(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE", dsn=DSN)
        shutil.rmtree(self.dir, ignore_errors=True)

    def logs(self):
        out = []
        for name in sorted(os.listdir(self.dir)):
            if name.endswith(".log"):
                with open(os.path.join(self.dir, name)) as f:
                    out.append(f"--- {name}\n{f.read()[-4000:]}")
        return "\n".join(out)

    # --- client and database ----------------------------------------------

    def ctl(self, *args, check=True):
        r = subprocess.run([f"{BIN}/epochflowctl", f"--coordinator={self.address}", *args],
                           capture_output=True, text=True, timeout=TIMEOUT_S)
        if check and r.returncode != 0:
            raise AssertionError(f"epochflowctl {args} failed ({r.returncode}): {r.stderr}")
        return r

    def submit(self, payload, count=1):
        return [int(x) for x in self.ctl("submit", f"--payload={payload}",
                                         f"--count={count}").stdout.split()]

    def job(self, job_id):
        fields = {}
        for line in self.ctl("get", str(job_id)).stdout.splitlines():
            key, _, value = line.partition("=")
            fields[key] = value
        return fields

    def wait_states(self, ids, states):
        self.wait_for(lambda: all(self.job(i)["state"] in states for i in ids),
                      f"jobs {ids} to reach {states}")

    def psql(self, sql, dsn=None):
        r = subprocess.run(["psql", dsn or self.dsn, "-X", "-q", "-tA", "-F", "|", "-c", sql],
                           capture_output=True, text=True, timeout=TIMEOUT_S)
        if r.returncode != 0:
            raise AssertionError(f"psql failed: {r.stderr}")
        return [line for line in r.stdout.splitlines() if line]

    def violations(self):
        r = subprocess.run(["psql", self.dsn, "-X", "-q", "-tA", "-F", "|", "-f", CHECKER],
                           capture_output=True, text=True, timeout=TIMEOUT_S)
        if r.returncode != 0:
            raise AssertionError(f"invariant checker failed: {r.stderr}")
        return [line for line in r.stdout.splitlines() if line]

    def ledger_entries(self, operation_id):
        return self.psql(
            f"SELECT entry_id, generation FROM ledger_entries WHERE operation_id = '{operation_id}'")

    def trace(self, job_id):
        return self.psql(f"SELECT generation, worker_id, outcome FROM job_attempts "
                         f"WHERE job_id = {job_id} ORDER BY generation")

    def provider_charges(self, reference):
        if not os.path.exists(self.provider_log):
            return []
        with open(self.provider_log) as f:
            return [l.rstrip("\n").split("\t") for l in f if l.startswith(reference + "\t")]

    @staticmethod
    def wait_for(predicate, what, timeout=TIMEOUT_S):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for {what}")


def with_cluster(test):
    """Runs a scenario with a fresh cluster; on failure, appends process logs."""

    @functools.wraps(test)
    def run(self):
        cluster = Cluster()
        try:
            test(self, cluster)
            self.assertEqual(cluster.violations(), [], "invariant violations")
        except Exception as e:
            raise AssertionError(f"{e}\n{cluster.logs()}") from e
        finally:
            cluster.close()

    return run


def payment(op, cents=500, account="alice"):
    return f"task=payment;op={op};account={account};amount_cents={cents}"


def external(op, cents=500, account="alice"):
    return f"task=external;op={op};account={account};amount_cents={cents}"


class FaultScenarios(unittest.TestCase):

    @with_cluster
    def test_crash_before_work_is_reassigned(self, c):
        c.start_coordinator()
        [job] = c.submit(payment("order-1"))
        crashed = c.start_worker("crashed", fault="crash-before-work")
        self.assertEqual(crashed.wait(timeout=TIMEOUT_S), -signal.SIGKILL)

        c.start_worker("rescuer")
        c.wait_states([job], {"COMPLETED"})
        j = c.job(job)
        self.assertEqual((j["attempts"], j["generation"]), ("2", "2"))
        self.assertEqual(j["result"], "ledger_entry=1")
        self.assertEqual(c.trace(job), ["1|crashed|LEASE_EXPIRED", "2|rescuer|COMPLETED"])
        self.assertEqual(len(c.ledger_entries("order-1")), 1)

    @with_cluster
    def test_crash_after_effect_is_deduplicated(self, c):
        c.start_coordinator()
        [job] = c.submit(payment("order-1"))
        crashed = c.start_worker("crashed", fault="crash-after-effect")
        self.assertEqual(crashed.wait(timeout=TIMEOUT_S), -signal.SIGKILL)
        self.assertEqual(c.ledger_entries("order-1"), ["1|1"])  # effect happened

        c.start_worker("rescuer")
        c.wait_states([job], {"COMPLETED"})
        self.assertEqual(c.job(job)["result"], "ledger_entry=1;deduplicated=true")
        self.assertEqual(c.ledger_entries("order-1"), ["1|1"])  # and only once
        self.assertIn("deduplicated operation=order-1", c.log("coordinator"))

    @with_cluster
    def test_paused_worker_cannot_apply_effect_after_reassignment(self, c):
        c.start_coordinator()
        [job] = c.submit(payment("order-1"))
        c.start_worker("paused", fault="pause-before-effect")
        c.wait_for(lambda: c.state("paused") == "T", "worker to stop before its effect")

        c.start_worker("rescuer")
        c.wait_states([job], {"COMPLETED"})
        self.assertEqual(c.ledger_entries("order-1"), ["1|2"])  # applied by generation 2

        os.kill(c.processes["paused"].pid, signal.SIGCONT)  # the stale worker resumes
        c.wait_for(lambda: f"rejected operation=order-1 job={job} generation=1 status=STALE"
                   in c.log("coordinator"), "the stale effect to be rejected")
        self.assertEqual(c.ledger_entries("order-1"), ["1|2"])
        j = c.job(job)
        self.assertEqual((j["state"], j["generation"], j["result"]),
                         ("COMPLETED", "2", "ledger_entry=1"))
        self.assertIsNone(c.processes["paused"].poll(), "resumed worker should keep running")

    @with_cluster
    def test_paused_worker_cannot_commit_after_reassignment(self, c):
        c.start_coordinator()
        [job] = c.submit(payment("order-1"))
        c.start_worker("paused", fault="pause-before-report")
        c.wait_for(lambda: c.state("paused") == "T", "worker to stop before reporting")

        c.start_worker("rescuer")
        c.wait_states([job], {"COMPLETED"})
        os.kill(c.processes["paused"].pid, signal.SIGCONT)
        c.wait_for(lambda: f"rejected completion job={job} generation=1 status=STALE"
                   in c.log("coordinator"), "the stale completion to be rejected")
        self.assertIn(f"reported success job={job} generation=1 status=STALE", c.log("paused"))
        j = c.job(job)
        self.assertEqual((j["generation"], j["result"]), ("2", "ledger_entry=1;deduplicated=true"))
        self.assertEqual(c.trace(job), ["1|paused|LEASE_EXPIRED", "2|rescuer|COMPLETED"])
        self.assertEqual(len(c.ledger_entries("order-1")), 1)

    @with_cluster
    def test_lost_report_acknowledgement_is_retried_idempotently(self, c):
        c.start_coordinator()
        [job] = c.submit(payment("order-1"))
        c.start_worker("lossy", fault="drop-report-ack")
        c.wait_states([job], {"COMPLETED"})
        c.wait_for(lambda: "status=ACCEPTED" in c.log("lossy"), "the retried report")
        log = c.log("lossy")
        self.assertIn("fault=drop-report-ack firing", log)
        self.assertIn("report attempt 1/3 failed", log)
        self.assertIn(f"reported success job={job} generation=1 status=ACCEPTED", log)
        self.assertEqual(c.trace(job), ["1|lossy|COMPLETED"])

    @with_cluster
    def test_duplicate_requests_create_one_effect(self, c):
        c.start_coordinator()
        first, second = c.submit(payment("order-dup"), count=2)
        [conflicting] = c.submit(payment("order-dup", cents=999))
        c.start_worker("w1")
        c.wait_states([first, second, conflicting], {"COMPLETED", "FAILED"})

        results = sorted(c.job(i)["result"] for i in (first, second))
        self.assertEqual(results, ["ledger_entry=1", "ledger_entry=1;deduplicated=true"])
        bad = c.job(conflicting)
        self.assertEqual(bad["state"], "FAILED")
        self.assertIn("conflicts with an earlier request", bad["error"])
        self.assertEqual(bad["attempts"], "1")  # not retried
        self.assertEqual(len(c.ledger_entries("order-dup")), 1)

    @with_cluster
    def test_coordinator_crash_mid_run_loses_and_duplicates_nothing(self, c):
        c.start_coordinator()
        listen = c.address
        ids = [c.submit(payment(f"bulk-{i}", cents=i + 1))[0] for i in range(60)]
        for name in ("w1", "w2", "w3"):
            c.start_worker(name, work_ms=40)  # ~0.8 s of work in total
        c.wait_for(lambda: c.psql("SELECT count(*) FROM jobs WHERE state = 'COMPLETED'") != ["0"],
                   "work to start")

        c.kill_coordinator()
        done_at_crash = int(c.psql("SELECT count(*) FROM jobs WHERE state = 'COMPLETED'")[0])
        self.assertLess(done_at_crash, 60, "the crash must interrupt work in progress")
        time.sleep(0.5)  # workers lose their connection mid-flight
        c.start_coordinator(listen=listen)
        c.wait_states(ids, {"COMPLETED"})

        self.assertEqual(c.psql("SELECT count(*), count(DISTINCT operation_id), sum(amount_cents) "
                                "FROM ledger_entries"), [f"60|60|{sum(range(1, 61))}"])

    @with_cluster
    def test_external_timeout_is_unknown_until_reconciled(self, c):
        c.start_coordinator()
        [job] = c.submit(external("charge-1"))
        c.start_worker("w1", fault="external-timeout")
        c.wait_states([job], {"UNKNOWN"})
        charges = c.provider_charges("charge-1")
        self.assertEqual(len(charges), 1)  # the provider did charge

        # Never retried, never reclaimed, however long we wait.
        time.sleep(2 * LEASE_MS / 1000)
        j = c.job(job)
        self.assertEqual((j["state"], j["attempts"]), ("UNKNOWN", "1"))
        self.assertIn("timed out", j["error"])
        self.assertEqual(len(c.provider_charges("charge-1")), 1)

        # The operator checks the provider's record and resolves the job.
        confirmation = charges[0][2]
        c.ctl("reconcile", str(job), "--outcome=completed", f"--detail={confirmation}")
        c.ctl("reconcile", str(job), "--outcome=completed", f"--detail={confirmation}")
        self.assertEqual(c.ctl("reconcile", str(job), "--outcome=failed", "--detail=x",
                               check=False).returncode, 1)
        j = c.job(job)
        self.assertEqual((j["state"], j["result"]), ("COMPLETED", confirmation))

    @with_cluster
    def test_crash_during_external_call_is_never_charged_twice(self, c):
        c.start_coordinator()
        [job] = c.submit(external("charge-2"))
        crashed = c.start_worker("crashed", fault="crash-after-effect")
        self.assertEqual(crashed.wait(timeout=TIMEOUT_S), -signal.SIGKILL)
        self.assertEqual(len(c.provider_charges("charge-2")), 1)  # charged, never confirmed

        c.start_worker("rescuer")
        c.wait_states([job], {"UNKNOWN"})
        j = c.job(job)
        self.assertEqual((j["attempts"], j["generation"]), ("2", "2"))
        self.assertIn("never confirmed", j["error"])
        self.assertEqual(len(c.provider_charges("charge-2")), 1)  # no blind retry
        self.assertIn("in doubt operation=charge-2", c.log("coordinator"))


if __name__ == "__main__":
    if not DSN or not BIN or shutil.which("psql") is None:
        print("SKIP: needs EPOCHFLOW_DSN, EPOCHFLOW_BIN and psql")
        sys.exit(77)
    unittest.main(verbosity=2)
