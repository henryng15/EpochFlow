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


class DesignTest(unittest.TestCase):
    DESIGN = {"main": {"seeds": [1, 2], "cells": [[1, 200], [3, 200]]},
              "stress": [{"workers": 1, "jobs": 1000, "seeds": [1]}]}

    def test_excluded_cell_runs_only_as_a_stress_test(self):
        design = bench_matrix.design(seeds=[1, 2], workers=[1, 3], jobs=[200, 1000],
                                     exclude=[(1, 1000)], stress=[(1, 1000, [1])])
        self.assertEqual(design["main"]["cells"], [[1, 200], [3, 200], [3, 1000]])
        planned = bench_matrix.plan_design(design)
        self.assertEqual(len(planned), (2 * 3 + 1) * 2)
        stress = [p for p in planned if (p[1], p[2]) == (1, 1000)]
        self.assertEqual(stress, [(1, 1, 1000, "epochflow"), (1, 1, 1000, "naive")])

    def test_coverage_reports_every_planned_cell_and_runs_outside_the_design(self):
        rows = [row(1, m, workers=1) for m in ("epochflow", "naive")]
        rows += [row(1, "epochflow", workers=3), {**row(1, "naive", workers=3), "error": "x"}]
        rows += [row(5, m, workers=1, jobs=1000) for m in ("epochflow", "naive")]  # seed 5
        cov = {(c["group"], c["workers"], c["jobs"]): c
               for c in bench_matrix.coverage(self.DESIGN, rows)}
        self.assertEqual(cov[("main", 1, 200)]["complete_pairs"], 1)
        self.assertEqual(cov[("main", 1, 200)]["planned_pairs"], 2)
        self.assertEqual(cov[("main", 3, 200)]["complete_pairs"], 0)
        self.assertEqual(cov[("main", 3, 200)]["failed_runs"], 1)
        self.assertEqual(cov[("stress", 1, 1000)]["complete_pairs"], 0)
        self.assertEqual(cov[("outside design", 1, 1000)]["runs"], 2)

    def test_stress_rows_never_enter_the_main_statistics(self):
        rows = [row(1, "epochflow", workers=1, jobs=1000, throughput_jobs_per_s=1.0),
                row(1, "epochflow", workers=1, throughput_jobs_per_s=9.0)]
        main = bench_matrix.summarize(bench_matrix.select(rows, self.DESIGN, "main"))
        self.assertEqual(list(main), [(1, 200, "epochflow")])
        stress = bench_matrix.select(rows, self.DESIGN, "stress")
        self.assertEqual([r["jobs"] for r in stress], [1000])

    def test_paired_comparison_uses_only_complete_pairs(self):
        rows = [row(1, "epochflow", throughput_jobs_per_s=30.0),
                row(1, "naive", throughput_jobs_per_s=20.0, duplicate_ledger_entries=3),
                row(2, "epochflow", throughput_jobs_per_s=10.0)]  # naive missing
        pairs = bench_matrix.paired(rows)[(3, 200)]
        self.assertEqual(pairs["pairs"], 1)
        self.assertEqual(pairs["throughput_ratio"]["median"], 1.5)
        self.assertEqual(pairs["naive_duplicates_total"], 3)
        self.assertEqual(pairs["epochflow_duplicates_total"], 0)


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
                f.write(json.dumps(row(2, "epochflow", workers=1, jobs=1000)) + "\n")
            with open(os.path.join(d, "environment.json"), "w") as f:
                json.dump({"command": "scripts/bench_matrix.py run ...", "postgres": "16.4",
                           "cpu": "test cpu", "os": "Linux", "git": "abc123",
                           "settings": {"lease_ms": 1000},
                           "design": {"main": {"seeds": [1], "cells": [[3, 200]]},
                                      "stress": [{"workers": 1, "jobs": 1000, "seeds": [2]}]},
                           "revisions": [{"at": "t0", "command": "first ..."}]}, f)
            report = bench_matrix.render(d)
        for text in ("scripts/bench_matrix.py run ...", "16.4", "test cpu", "abc123",
                     "| 3 | 200 | epochflow |", "lease_ms", "first ...", "## Coverage",
                     "## Single-worker stress test", "## Limitations"):
            self.assertIn(text, report)
        main, stress = report.split("## Single-worker stress test")
        self.assertNotIn("| 1 | 1000 |", main.split("## Coverage")[1].split("## Correctness")[1])
        self.assertIn("| 2 | epochflow |", stress)

    def test_count_medians_are_not_shown_as_measurements(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "raw.jsonl"), "w") as f:
                for seed, retries in ((1, 2), (2, 3)):
                    for mode in ("epochflow", "naive"):
                        f.write(json.dumps(row(seed, mode, retries=retries)) + "\n")
            with open(os.path.join(d, "environment.json"), "w") as f:
                json.dump({"design": {"main": {"seeds": [1, 2], "cells": [[3, 200]]},
                                      "stress": []}}, f)
            report = bench_matrix.render(d)
        retries = report.split("### Retries")[1].split("###")[0]
        self.assertIn("| 3 | 200 | epochflow | 2.5 | 3 | 2 | 3 |", retries)
        dedup = report.split("### Effects deduplicated")[1].split("###")[0]
        self.assertIn("| 3 | 200 | epochflow | 0 | 0 | 0 | 0 |", dedup)
        throughput = report.split("### Throughput")[1].split("###")[0]
        self.assertIn("| 3 | 200 | epochflow | 10.00 | 10.00 | 10.00 | 10.00 |", throughput)


if __name__ == "__main__":
    unittest.main()
