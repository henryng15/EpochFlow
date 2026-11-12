"""Process-level control of an EpochFlow deployment for tests and benchmarks.

A Cluster owns one PostgreSQL schema, a coordinator, named workers, a provider
log, and their process logs, all removed by close(). Standard library only;
requires psql on PATH.
"""

import os
import re
import shutil
import signal
import subprocess
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHECKER = os.path.join(REPO, "scripts", "check-invariants.sql")
TIMEOUT_S = 30


class Cluster:
    """One coordinator, its workers, a private schema, and a provider log."""

    _counter = 0

    def __init__(self, bin_dir, dsn, prefix="cluster", lease_ms=1500, reclaim_ms=100):
        Cluster._counter += 1
        self.bin = bin_dir
        self.base_dsn = dsn
        self.lease_ms = lease_ms
        self.reclaim_ms = reclaim_ms
        self.schema = f"{prefix}_{os.getpid()}_{Cluster._counter}"
        self.dir = tempfile.mkdtemp(prefix="epochflow-faults-")
        self.provider_log = os.path.join(self.dir, "provider.log")
        self.dsn = f"{dsn} options='-csearch_path={self.schema}'"
        self.processes = {}
        self.retired = []  # replaced worker processes, still to be reaped
        self.files = []
        self.coordinator = None
        self.address = None
        self.psql(f"CREATE SCHEMA {self.schema}", dsn=dsn)

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
            [f"{self.bin}/epochflow-coordinator", f"--listen={listen}", f"--dsn={self.dsn}",
             f"--lease-ms={self.lease_ms}", f"--reclaim-interval-ms={self.reclaim_ms}"],
            stdout=self.open("coordinator.out", "w"), stderr=self.open("coordinator.log", "a"))
        self.wait_for(lambda: "listening on" in self.read("coordinator.out"),
                      "coordinator to listen")
        self.address = re.search(r"listening on (\S+)", self.read("coordinator.out")).group(1)

    def kill_coordinator(self):
        self.coordinator.kill()
        self.coordinator.wait()

    def start_worker(self, name, fault=None, work_ms=0, work_after_effect_ms=0):
        """Starts (or restarts, appending to the same log) worker `name`."""
        args = [f"{self.bin}/epochflow-worker", f"--coordinator={self.address}",
                f"--worker-id={name}", f"--provider-log={self.provider_log}",
                "--idle-backoff-ms=20", "--rpc-timeout-ms=500", f"--work-ms={work_ms}",
                f"--work-after-effect-ms={work_after_effect_ms}"]
        if fault:
            args.append(f"--fault={fault}")
        proc = subprocess.Popen(args, stderr=self.open(f"{name}.log", "a"))
        if name in self.processes:
            self.retired.append(self.processes[name])
        self.processes[name] = proc
        return proc

    def log(self, name):
        return self.read(f"{name}.log")

    def state(self, name):
        """Kernel process state letter: 'T' when stopped by SIGSTOP."""
        with open(f"/proc/{self.processes[name].pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0]

    def close(self):
        procs = list(self.processes.values()) + self.retired + (
            [self.coordinator] if self.coordinator else [])
        for p in procs:
            if p.poll() is None:
                os.kill(p.pid, signal.SIGCONT)
                p.kill()
            p.wait()
        for f in self.files:
            f.close()
        self.psql(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE", dsn=self.base_dsn)
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
        r = subprocess.run([f"{self.bin}/epochflowctl", f"--coordinator={self.address}", *args],
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
