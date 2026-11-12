#!/usr/bin/env python3
"""Seeded benchmark matrix: scripts/bench.py over many seeds, worker counts,
and job counts, with raw per-run output and an aggregate report.

Every cell (workers x jobs) runs each seed in both modes back to back, so each
EpochFlow/naive pair replays the same job set and chaos schedule. The design
has a main grid (--seeds x --workers x --jobs, minus --exclude cells), which
gets statistical summaries, and optional stress cells (--stress), each with
its own seeds, reported run by run and never mixed into the main statistics.

  run        execute the planned runs not yet recorded in OUT/raw.jsonl
             (resumable; failed runs are retried), recording the design and
             every revision of it in OUT/environment.json
  aggregate  write OUT/summary.md from OUT/raw.jsonl

Example (the Phase 2 design, about 1.5 hours on WSL2):
  scripts/bench_matrix.py run --bin build/bin --dsn "$(scripts/test-db.sh dsn)" \\
      --out docs/benchmarks/phase-2 --seeds 1-20 --workers 1,3,6 --jobs 200,1000 \\
      --exclude 1:1000 --stress 1:1000:1-3
  scripts/bench_matrix.py aggregate --out docs/benchmarks/phase-2

Standard library only; see docs/testing.md.
"""

import argparse
import copy
import datetime
import json
import os
import platform
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench  # noqa: E402

MODES = ("epochflow", "naive")

# Per-run metrics summarized across the runs of a cell (key, label).
METRICS = [
    ("throughput_jobs_per_s", "Throughput (jobs/s)"),
    ("recovery_latency_s", "Recovery latency (s), all events pooled"),
    ("retries", "Retries"),
    ("stale_requests_rejected", "Stale requests rejected by fencing"),
    ("effects_deduplicated", "Effects deduplicated"),
    ("duplicate_ledger_entries", "Duplicate ledger entries"),
    ("overcharged_cents", "Overcharged (cents)"),
    ("invariant_violations", "Invariant violations"),
    ("incomplete_jobs", "Jobs not completed (FAILED after max attempts)"),
]

# Settings shared by every run, recorded with the results.
SETTINGS = ("lease_ms", "reclaim_ms", "work_ms", "work_after_effect_ms", "chaos_interval_ms",
            "kill_ratio", "timeout_s")

MIN_AVAILABLE_KIB = 1024 * 1024  # do not start a run with less than 1 GiB free


def plan(seeds, workers, jobs):
    """Every (seed, workers, jobs, mode) of a full grid, one seed at a time,
    pairs adjacent."""
    return plan_design(design(seeds, workers, jobs))


def design(seeds, workers, jobs, exclude=(), stress=()):
    """The experiment: a main grid without the excluded cells, plus stress
    cells with their own seeds. Excluded cells are listed, never dropped
    silently."""
    excluded = {tuple(c) for c in exclude}
    return {
        "main": {"seeds": list(seeds),
                 "cells": [[w, j] for w in workers for j in jobs if (w, j) not in excluded],
                 "excluded": [list(c) for c in exclude]},
        "stress": [{"workers": w, "jobs": j, "seeds": list(ss)} for w, j, ss in stress],
    }


def plan_design(d):
    """Main runs seed by seed (pairs adjacent), then each stress cell."""
    runs = [(s, w, j, m) for s in d["main"]["seeds"] for w, j in d["main"]["cells"]
            for m in MODES]
    for c in d["stress"]:
        runs += [(s, c["workers"], c["jobs"], m) for s in c["seeds"] for m in MODES]
    return runs


def group_of(d, seed, workers, jobs):
    """"main", "stress", or None for a run outside the design."""
    if [workers, jobs] in d["main"]["cells"] and seed in d["main"]["seeds"]:
        return "main"
    for c in d["stress"]:
        if (c["workers"], c["jobs"]) == (workers, jobs) and seed in c["seeds"]:
            return "stress"
    return None


def select(rows, d, group):
    return [r for r in rows if group_of(d, r["seed"], r["workers"], r["jobs"]) == group]


def coverage(d, rows):
    """Per planned cell: planned and complete pairs (both modes succeeded) and
    failed runs; plus recorded runs outside the design, per cell."""
    ok = {run_key(r) for r in rows if "error" not in r}
    bad = {run_key(r) for r in rows if "error" in r} - ok
    cells = [("main", w, j, d["main"]["seeds"]) for w, j in d["main"]["cells"]]
    cells += [("stress", c["workers"], c["jobs"], c["seeds"]) for c in d["stress"]]
    out = []
    for group, w, j, seeds in cells:
        out.append({
            "group": group, "workers": w, "jobs": j, "seeds": seeds,
            "planned_pairs": len(seeds),
            "complete_pairs": sum(1 for s in seeds if all((s, w, j, m) in ok for m in MODES)),
            "failed_runs": sum(1 for s in seeds for m in MODES if (s, w, j, m) in bad),
        })
    outside = {}
    for r in rows:
        if "error" not in r and group_of(d, r["seed"], r["workers"], r["jobs"]) is None:
            outside.setdefault((r["workers"], r["jobs"]), []).append(r["seed"])
    for (w, j), seeds in sorted(outside.items()):
        out.append({"group": "outside design", "workers": w, "jobs": j,
                    "seeds": sorted(set(seeds)), "runs": len(seeds)})
    return out


def paired(rows):
    """Per cell, over seeds where both modes succeeded: EpochFlow/naive
    throughput ratio and duplicate totals."""
    ok = {run_key(r): r for r in rows if "error" not in r}
    out = {}
    for (seed, w, j, mode), e in sorted(ok.items()):
        n = ok.get((seed, w, j, "naive"))
        if mode != "epochflow" or n is None:
            continue
        cell = out.setdefault((w, j), {"ratios": [], "naive_duplicates_total": 0,
                                       "epochflow_duplicates_total": 0})
        if e["throughput_jobs_per_s"] and n["throughput_jobs_per_s"]:
            cell["ratios"].append(e["throughput_jobs_per_s"] / n["throughput_jobs_per_s"])
        cell["naive_duplicates_total"] += n["duplicate_ledger_entries"]
        cell["epochflow_duplicates_total"] += e["duplicate_ledger_entries"]
    for cell in out.values():
        cell["pairs"] = len(cell["ratios"])
        cell["throughput_ratio"] = stats(cell.pop("ratios"))
    return out


def run_key(row):
    return (row["seed"], row["workers"], row["jobs"], row["mode"])


def pending(planned, done):
    """Planned runs without a successful row; failed runs are tried again."""
    succeeded = {run_key(r) for r in done if "error" not in r}
    return [p for p in planned if p not in succeeded]


def stats(values):
    if not values:
        return None
    return {"median": statistics.median(values), "p95": bench.percentile(values, 95),
            "min": min(values), "max": max(values)}


def summarize(rows):
    """{(workers, jobs, mode): {"runs", "failed_runs", "runs_with_duplicates",
    "metrics": {metric: stats}}}. The last successful row of a run wins; a run
    counts as failed only if it never succeeded."""
    succeeded, failed = {}, set()
    for r in rows:
        if "error" in r:
            failed.add(run_key(r))
        else:
            succeeded[run_key(r)] = r
    failed -= set(succeeded)

    cells = {}
    for (seed, workers, jobs, mode), r in sorted(succeeded.items()):
        cell = cells.setdefault((workers, jobs, mode), {"rows": []})
        cell["rows"].append(r)
    for (seed, workers, jobs, mode) in failed:
        cells.setdefault((workers, jobs, mode), {"rows": []})

    summary = {}
    for key, cell in cells.items():
        runs = cell["rows"]
        values = {m: [] for m, _ in METRICS}
        for r in runs:
            for m, _ in METRICS:
                if m == "recovery_latency_s":
                    values[m].extend(r.get("recovery_samples_s", []))
                elif m == "incomplete_jobs":
                    values[m].append(r["jobs"] - r["jobs_completed"])
                elif r.get(m) is not None:
                    values[m].append(r[m])
        summary[key] = {
            "runs": len(runs),
            "failed_runs": sum(1 for f in failed if (f[1], f[2], f[3]) == key),
            "runs_with_duplicates": sum(1 for r in runs if r["duplicate_ledger_entries"] > 0),
            "metrics": {m: stats(v) for m, v in values.items()},
        }
    return summary


def fmt(value):
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def load_rows(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


LIMITATIONS = """\
- One machine (WSL2), with PostgreSQL in Docker on the same host and every
  process sharing its CPUs. Numbers describe this environment only; they are
  not production, cloud-network, or provider performance.
- The chaos schedule is replayed until every job finishes, so slower runs
  receive more events; event counts differ between runs and cells.
- Recovery latency is measured from a chaos event (host clock) to the next
  attempt of each job the affected worker held (database clock, same host).
  It includes the 1 s lease and the reclaim interval by design.
- Jobs keep the default retry budget (max_attempts = 3): a job interrupted on
  all three attempts ends FAILED, in either mode, and counts as not
  completed. The benchmark does not record whether such a job's ledger effect
  had been applied before its last attempt was lost.
- The naive baseline differs only in sending a fresh operation id per attempt;
  fencing stays on, so it shows duplicate effects, not stale overwrites.
- Medians and percentiles summarize 20 seeds per main cell; with that sample,
  p95 is close to the maximum. No significance test is claimed.
- The single-worker stress cell has 3 seeds and is reported run by run; it is
  not part of any statistical summary. With one worker, every kill or pause
  stops all progress until recovery, so its throughput is dominated by
  chaos, not by EpochFlow overhead.
"""


def design_from_env(env):
    if "design" in env:
        return env["design"]
    return design(env.get("seeds", []), env.get("workers", []), env.get("jobs", []))


def render(out):
    rows = load_rows(os.path.join(out, "raw.jsonl"))
    with open(os.path.join(out, "environment.json")) as f:
        env = json.load(f)
    d = design_from_env(env)
    main_rows = select(rows, d, "main")
    summary = summarize(main_rows)
    keys = sorted(summary, key=lambda k: (k[0], k[1], MODES.index(k[2])))
    pairs = paired(main_rows)

    lines = ["# Benchmark matrix", "",
             "Generated by `scripts/bench_matrix.py aggregate` from `raw.jsonl` (one JSON "
             "object per run). Each cell runs every seed in both modes with the same job set "
             "and chaos schedule. These numbers describe this machine only.", "",
             "## Environment", "",
             f"- Command: `{env.get('command')}`",
             f"- Git commit: `{env.get('git')}`",
             f"- OS: {env.get('os')}",
             f"- CPU: {env.get('cpu')}",
             f"- Memory: {env.get('memory')}",
             f"- PostgreSQL: {env.get('postgres')}",
             f"- Settings: {', '.join(f'{k}={v}' for k, v in env.get('settings', {}).items())}"]
    notes = env.get("notes", [])
    if notes:
        lines += ["- Notes:"] + [f"  - {n}" for n in notes]
    revisions = env.get("revisions", [])
    if revisions:
        lines += ["- Design revisions (completed runs are kept; nothing was rerun):"]
        lines += [f"  - {r.get('at')}: `{r.get('command')}`" for r in revisions]

    lines += ["", "## Coverage", "",
              "| Group | Workers | Jobs | Seeds | Planned pairs | Complete pairs | Failed runs |",
              "| --- | --- | --- | --- | --- | --- | --- |"]
    for c in coverage(d, rows):
        if c["group"] == "outside design":
            lines.append(f"| outside design (not reported) | {c['workers']} | {c['jobs']} | "
                         f"{seed_text(c['seeds'])} | n/a | {c['runs']} runs | n/a |")
        else:
            lines.append(f"| {c['group']} | {c['workers']} | {c['jobs']} | "
                         f"{seed_text(c['seeds'])} | {c['planned_pairs']} | "
                         f"{c['complete_pairs']} | {c['failed_runs']} |")
    if d["main"].get("excluded"):
        cells = ", ".join(f"workers={w} jobs={j}" for w, j in d["main"]["excluded"])
        lines += ["", f"Excluded from the main grid: {cells} (see the stress test below)."]

    lines += ["", "## Correctness (main grid)", "",
              "| Workers | Jobs | Mode | Runs | Failed runs | Runs with duplicates | "
              "Duplicate entries (max) | Overcharged cents (max) | Invariant violations (max) | "
              "Jobs not completed (max) |",
              "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    for k in keys:
        s = summary[k]
        mx = lambda m: fmt(s["metrics"][m]["max"]) if s["metrics"][m] else "n/a"  # noqa: E731
        lines.append(f"| {k[0]} | {k[1]} | {k[2]} | {s['runs']} | {s['failed_runs']} | "
                     f"{s['runs_with_duplicates']} | {mx('duplicate_ledger_entries')} | "
                     f"{mx('overcharged_cents')} | {mx('invariant_violations')} | "
                     f"{mx('incomplete_jobs')} |")

    lines += ["", "## Paired comparison (main grid)", "",
              "Per seed, EpochFlow throughput divided by naive throughput on the same chaos "
              "schedule (1.00 = equal), and total duplicate ledger entries over all pairs.", "",
              "| Workers | Jobs | Pairs | Throughput ratio median | Min | Max | "
              "Duplicates, EpochFlow | Duplicates, naive |",
              "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for (w, j), c in sorted(pairs.items()):
        r = c["throughput_ratio"]
        cols = [fmt(r[x]) for x in ("median", "min", "max")] if r else ["n/a"] * 3
        lines.append(f"| {w} | {j} | {c['pairs']} | " + " | ".join(cols) +
                     f" | {c['epochflow_duplicates_total']} | {c['naive_duplicates_total']} |")

    lines += ["", "## Statistics per metric (main grid)", "",
              "Median (= p50), p95 (nearest rank), minimum, and maximum over the runs of each "
              "cell. Recovery latency pools every chaos event of every run: the time from the "
              "event to the next attempt of each job the affected worker held.", ""]
    for m, label in METRICS:
        lines += [f"### {label}", "", "| Workers | Jobs | Mode | Median | p95 | Min | Max |",
                  "| --- | --- | --- | --- | --- | --- | --- |"]
        for k in keys:
            st = summary[k]["metrics"][m]
            cols = [fmt(st[c]) for c in ("median", "p95", "min", "max")] if st else ["n/a"] * 4
            lines.append(f"| {k[0]} | {k[1]} | {k[2]} | " + " | ".join(cols) + " |")
        lines.append("")

    lines += ["## Single-worker stress test", ""]
    for c in d["stress"]:
        lines += [f"workers={c['workers']}, jobs={c['jobs']}, seeds {seed_text(c['seeds'])}. "
                  "Reported run by run; too few seeds for summary statistics.", "",
                  "| Seed | Mode | Throughput (jobs/s) | Recovery p50 / p95 / max (s) | "
                  "Chaos events | Retries | Stale rejected | Deduplicated | Duplicate entries | "
                  "Overcharged cents | Violations | Jobs not completed |",
                  "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
        stress_rows = {run_key(r): r for r in select(rows, d, "stress") if "error" not in r}
        for seed in c["seeds"]:
            for mode in MODES:
                r = stress_rows.get((seed, c["workers"], c["jobs"], mode))
                if r is None:
                    lines.append(f"| {seed} | {mode} | not run |" + " |" * 9)
                    continue
                g = lambda k: fmt(r.get(k))  # noqa: E731
                lines.append(
                    f"| {seed} | {mode} | {g('throughput_jobs_per_s')} | "
                    f"{g('recovery_p50_s')} / {g('recovery_p95_s')} / {g('recovery_max_s')} | "
                    f"{g('chaos_events')} | {g('retries')} | {g('stale_requests_rejected')} | "
                    f"{g('effects_deduplicated')} | {g('duplicate_ledger_entries')} | "
                    f"{g('overcharged_cents')} | {g('invariant_violations')} | "
                    f"{fmt(r['jobs'] - r['jobs_completed'])} |")
        lines.append("")

    lines += ["## Limitations", "", LIMITATIONS]
    return "\n".join(lines)


def seed_text(seeds):
    seeds = sorted(seeds)
    if seeds and seeds == list(range(seeds[0], seeds[-1] + 1)) and len(seeds) > 2:
        return f"{seeds[0]}-{seeds[-1]}"
    return ", ".join(map(str, seeds))


def parse_list(text):
    out = []
    for part in text.split(","):
        lo, _, hi = part.partition("-")
        out.extend(range(int(lo), int(hi) + 1) if hi else [int(lo)])
    return out


def parse_cell(text):
    w, j = text.split(":")
    return (int(w), int(j))


def parse_stress(text):
    w, j, seeds = text.split(":")
    return (int(w), int(j), parse_list(seeds))


def proc_field(path, name):
    """First "name: value" field of a /proc file, or None."""
    with open(path) as f:
        for line in f:
            if line.startswith(name):
                return line.split(":", 1)[1].strip()
    return None


def available_kib():
    value = proc_field("/proc/meminfo", "MemAvailable")
    return int(value.split()[0]) if value else MIN_AVAILABLE_KIB


def wait_for_memory():
    for _ in range(20):
        if available_kib() >= MIN_AVAILABLE_KIB:
            return
        print("waiting for memory (MemAvailable below 1 GiB)", flush=True)
        time.sleep(30)
    raise SystemExit("not enough free memory to continue; rerun to resume")


def environment(a, settings):
    def sh(*cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
        except OSError:
            return "unknown"
    cpu = proc_field("/proc/cpuinfo", "model name") or platform.processor()
    mem_kib = int(proc_field("/proc/meminfo", "MemTotal").split()[0])
    # Uncommitted tracked or new files, ignoring the results directory itself.
    out_dir = os.path.relpath(os.path.abspath(a.out))
    changed = [line[3:] for line in sh("git", "status", "--porcelain", "--", ".",
                                       f":(exclude){out_dir}").splitlines()]
    dirty = f" (uncommitted: {', '.join(changed)})" if changed else ""
    command = " ".join(["scripts/bench_matrix.py"] +
                       [('--dsn="$(scripts/test-db.sh dsn)"' if x.startswith("--dsn=") else x)
                        for x in _redacted_argv()])
    return {
        "command": command,
        "git": sh("git", "rev-parse", "--short", "HEAD") + dirty,
        "os": platform.platform(),
        "cpu": f"{cpu}, {os.cpu_count()} logical CPUs",
        "memory": f"{mem_kib // 1024} MiB",
        "postgres": sh("psql", a.dsn, "-X", "-tAc", "SHOW server_version"),
        "python": platform.python_version(),
        "settings": settings,
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    }


def _redacted_argv():
    argv, out, skip = sys.argv[1:], [], False
    for i, x in enumerate(argv):
        if skip:
            skip = False
            continue
        if x == "--dsn":
            out.append('--dsn="$(scripts/test-db.sh dsn)"')
            skip = True
        else:
            out.append(x)
    return out


def cmd_run(a):
    os.makedirs(a.out, exist_ok=True)
    raw = os.path.join(a.out, "raw.jsonl")
    base = bench.build_parser().parse_args(
        ["--bin", a.bin, "--dsn", a.dsn, "--timeout-s", str(a.timeout_s)])
    settings = {k: getattr(base, k) for k in SETTINGS}
    d = design(a.seeds, a.workers, a.jobs, a.exclude, a.stress)
    env_path = os.path.join(a.out, "environment.json")
    fresh = environment(a, settings)
    if os.path.exists(env_path):
        with open(env_path) as f:
            env = json.load(f)
        if env.get("settings") != settings:
            raise SystemExit("settings differ from the recorded runs; use another --out")
    else:
        env = fresh
    # Every distinct command that ran against this directory, oldest first.
    revisions = env.get("revisions") or [
        {"at": env["started_at"], "command": env["command"], "git": env["git"]}]
    if revisions[-1]["command"] != fresh["command"]:
        revisions.append({"at": fresh["started_at"], "command": fresh["command"],
                          "git": fresh["git"]})
    env.update(design=d, command=fresh["command"], revisions=revisions)
    for stale_key in ("seeds", "workers", "jobs"):
        env.pop(stale_key, None)
    with open(env_path, "w") as f:
        json.dump(env, f, indent=2)

    todo = pending(plan_design(d), load_rows(raw))
    print(f"{len(todo)} runs to go", flush=True)
    for i, (seed, workers, jobs, mode) in enumerate(todo, 1):
        wait_for_memory()
        args = copy.copy(base)
        args.workers, args.jobs = workers, jobs
        started = time.monotonic()
        try:
            result = bench.run(args, mode, seed)
        except Exception as e:  # recorded and retried on the next resume
            result = {"error": str(e).splitlines()[0][:500]}
        row = {"seed": seed, "workers": workers, "jobs": jobs, **result, "mode": mode,
               "wall_s": round(time.monotonic() - started, 1)}
        with open(raw, "a") as f:
            f.write(json.dumps(row) + "\n")
        brief = row.get("error") or (f"throughput={row['throughput_jobs_per_s']} "
                                     f"duplicates={row['duplicate_ledger_entries']} "
                                     f"violations={row['invariant_violations']}")
        print(f"[{i}/{len(todo)}] seed={seed} workers={workers} jobs={jobs} mode={mode} "
              f"{row['wall_s']}s {brief}", flush=True)
    return 0


def cmd_aggregate(a):
    path = os.path.join(a.out, "summary.md")
    with open(path, "w") as f:
        f.write(render(a.out) + "\n")
    print(f"wrote {path}")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--bin", required=True)
    r.add_argument("--dsn", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--seeds", type=parse_list, default=parse_list("1-20"))
    r.add_argument("--workers", type=parse_list, default=parse_list("1,3,6"))
    r.add_argument("--jobs", type=parse_list, default=parse_list("200,1000"))
    r.add_argument("--timeout-s", type=int, default=900)
    r.add_argument("--exclude", type=parse_cell, action="append", default=[],
                   metavar="W:J", help="leave a cell out of the main grid (repeatable)")
    r.add_argument("--stress", type=parse_stress, action="append", default=[],
                   metavar="W:J:SEEDS",
                   help="separately reported cell with its own seeds (repeatable)")
    ag = sub.add_parser("aggregate")
    ag.add_argument("--out", required=True)
    a = p.parse_args()
    return cmd_run(a) if a.command == "run" else cmd_aggregate(a)


if __name__ == "__main__":
    sys.exit(main())
