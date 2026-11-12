#!/usr/bin/env python3
"""Aggregation and resume logic of scripts/bench_matrix.py (no database)."""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
import bench_matrix  # noqa: E402


def row(seed, mode, workers=3, jobs=200, **metrics):
    base = {"seed": seed, "mode": mode, "workers": workers, "jobs": jobs,
            "jobs_completed": jobs, "throughput_jobs_per_s": 10.0, "recovery_samples_s": [],
            "retries": 0, "stale_requests_rejected": 0, "effects_deduplicated": 0,
            "duplicate_ledger_entries": 0, "overcharged_cents": 0, "invariant_violations": 0}
    base.update(metrics)
    return base


class PlanTest(unittest.TestCase):
    def test_plan_covers_every_cell_and_mode_seed_by_seed(self):
        plan = bench_matrix.plan(seeds=[1, 2], workers=[1, 3], jobs=[200])
        self.assertEqual(len(plan), 2 * 2 * 1 * 2)
        self.assertEqual(plan[0], (1, 1, 200, "epochflow"))
        self.assertEqual(plan[1], (1, 1, 200, "naive"))  # pairs run back to back
        self.assertEqual({p[0] for p in plan[:4]}, {1})  # one seed at a time

    def test_runs_already_recorded_are_skipped(self):
        plan = bench_matrix.plan(seeds=[1, 2], workers=[3], jobs=[200])
        done = [row(1, "epochflow"), row(1, "naive")]
        self.assertEqual(bench_matrix.pending(plan, done),
                         [(2, 3, 200, "epochflow"), (2, 3, 200, "naive")])

    def test_failed_runs_are_retried_on_resume(self):
        plan = bench_matrix.plan(seeds=[1], workers=[3], jobs=[200])
        done = [row(1, "epochflow"), {**row(1, "naive"), "error": "timed out"}]
        self.assertEqual(bench_matrix.pending(plan, done), [(1, 3, 200, "naive")])


class SummarizeTest(unittest.TestCase):
    def test_statistics_per_cell_and_mode(self):
        rows = [row(s, "epochflow", throughput_jobs_per_s=t, recovery_samples_s=r)
                for s, t, r in [(1, 10.0, [1.0, 2.0]), (2, 20.0, [3.0]), (3, 30.0, [])]]
        rows += [row(1, "naive", duplicate_ledger_entries=4, overcharged_cents=400)]
        summary = bench_matrix.summarize(rows)

        e = summary[(3, 200, "epochflow")]
        self.assertEqual(e["runs"], 3)
        self.assertEqual(e["metrics"]["throughput_jobs_per_s"],
                         {"median": 20.0, "p95": 30.0, "min": 10.0, "max": 30.0})
        # Recovery latency pools every sample of every run.
        self.assertEqual(e["metrics"]["recovery_latency_s"],
                         {"median": 2.0, "p95": 3.0, "min": 1.0, "max": 3.0})
        n = summary[(3, 200, "naive")]
        self.assertEqual(n["metrics"]["duplicate_ledger_entries"]["max"], 4)
        self.assertEqual(n["runs_with_duplicates"], 1)
        self.assertEqual(e["runs_with_duplicates"], 0)

    def test_incomplete_jobs_and_failed_runs_are_counted(self):
        rows = [row(1, "epochflow", jobs_completed=198), {**row(2, "epochflow"), "error": "x"}]
        e = bench_matrix.summarize(rows)[(3, 200, "epochflow")]
        self.assertEqual(e["runs"], 1)
        self.assertEqual(e["failed_runs"], 1)
        self.assertEqual(e["metrics"]["incomplete_jobs"]["max"], 2)

    def test_markdown_report_states_environment_and_command(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "raw.jsonl"), "w") as f:
                for r in (row(1, "epochflow"), row(1, "naive", duplicate_ledger_entries=2)):
                    f.write(json.dumps(r) + "\n")
            with open(os.path.join(d, "environment.json"), "w") as f:
                json.dump({"command": "scripts/bench_matrix.py run ...", "postgres": "16.4",
                           "cpu": "test cpu", "os": "Linux", "git": "abc123",
                           "settings": {"lease_ms": 1000}}, f)
            report = bench_matrix.render(d)
        for text in ("scripts/bench_matrix.py run ...", "16.4", "test cpu", "abc123",
                     "| 3 | 200 |", "lease_ms"):
            self.assertIn(text, report)


if __name__ == "__main__":
    unittest.main()
