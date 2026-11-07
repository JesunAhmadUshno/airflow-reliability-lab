"""Generate RESULTS.md and results/summary.json from results/trials.jsonl.

RESULTS.md is generated, never hand-typed (SPEC.md section 8). Verdicts are
computed from the trial records against the thresholds in HYPOTHESES.md.

Usage: python -m airrel.report [results_dir] [output_path]
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

from .stats import failure_summary

FAULT_LABEL = {
    "kill_mid_write": "F1 kill mid-write",
    "partial_write_error": "F2 partial write then error",
    "duplicate_trigger": "F3 duplicate trigger, same interval",
    "late_data_rerun": "F6 late-arriving data, re-run",
    "scheduler_restart": "F4 scheduler restart mid-run",
    "code_deploy_mid_run": "F5 code deploy mid-run",
}
PIPELINES = ["ingestion", "embedding", "evaluation", "retraining", "monitoring"]


def load(results_dir):
    trials = [
        json.loads(line)
        for line in open(Path(results_dir) / "trials.jsonl", encoding="utf-8")
        if line.strip()
    ]
    env_path = Path(results_dir) / "environment.json"
    env = json.loads(env_path.read_text(encoding="utf-8")) if env_path.exists() else {}
    return trials, env


def _cell(trials, phase=None, fault=None, arm=None, pipeline=None):
    out = []
    for t in trials:
        if phase and t["phase"] != phase:
            continue
        if fault and t.get("fault") != fault:
            continue
        if arm and t["arm"] != arm:
            continue
        if pipeline and t["pipeline"] != pipeline:
            continue
        out.append(t)
    return out


def _failure_stats(records):
    fails = sum(1 for t in records if not t["passed"])
    return failure_summary(fails, len(records))


def _median_recovery(records):
    vals = [t["recovery_seconds"] for t in records if t.get("recovery_seconds")]
    return round(statistics.median(vals), 2) if vals else "n/a"


def compute_verdicts(trials):
    verdicts = {}
    # H1: naive fails at least 3 fault types; F1 and F2 failure rates >= 50%.
    naive_by_fault = {}
    for fault in FAULT_LABEL:
        recs = _cell(trials, fault=fault, arm="naive")
        if recs:
            naive_by_fault[fault] = _failure_stats(recs)
    failed_types = [f for f, s in naive_by_fault.items() if s["failures"] > 0]
    f1_rate = naive_by_fault.get("kill_mid_write", {}).get("rate", 0)
    f2_rate = naive_by_fault.get("partial_write_error", {}).get("rate", 0)
    h1 = len(failed_types) >= 3 and f1_rate >= 0.5 and f2_rate >= 0.5
    verdicts["H1"] = {
        "verdict": "Supported" if h1 else "Refuted",
        "evidence": f"naive arm failed {len(failed_types)} of {len(naive_by_fault)} "
                    f"fault types; F1 failure rate {f1_rate}, F2 failure rate {f2_rate}",
    }
    # H2: hardened zero failures everywhere; partial if only F4 or F5 fails.
    hardened_by_fault = {}
    for fault in FAULT_LABEL:
        recs = _cell(trials, fault=fault, arm="hardened")
        if recs:
            hardened_by_fault[fault] = _failure_stats(recs)
    write_path = ["kill_mid_write", "partial_write_error", "duplicate_trigger",
                  "late_data_rerun"]
    wp_fail = sum(hardened_by_fault.get(f, {}).get("failures", 0) for f in write_path)
    wp_cells_failed = 0
    for fault in write_path:
        for pipeline in PIPELINES:
            recs = _cell(trials, fault=fault, arm="hardened", pipeline=pipeline)
            if recs and any(not t["passed"] for t in recs):
                wp_cells_failed += 1
    any_loss = any(
        t["losses"] > 0 for t in _cell(trials, arm="hardened")
        if t.get("fault") in FAULT_LABEL
    )
    platform_fail = sum(
        hardened_by_fault.get(f, {}).get("failures", 0)
        for f in ("scheduler_restart", "code_deploy_mid_run")
    )
    if wp_fail == 0 and platform_fail == 0:
        h2 = "Supported"
    elif wp_fail == 0 or (wp_cells_failed <= 1 and not any_loss):
        h2 = "Partially supported"
    else:
        h2 = "Refuted"
    # H2 claims every fault cell, so the evidence prints a bound for every
    # fault the hardened arm ran, pooled across that fault's pipelines.
    pooled = {
        f: _failure_stats(_cell(trials, fault=f, arm="hardened"))
        for f in FAULT_LABEL
        if _cell(trials, fault=f, arm="hardened")
    }
    verdicts["H2"] = {
        "verdict": h2,
        "evidence": "hardened bounds per fault, pooled across pipelines: "
                    + "; ".join(
                        f"{FAULT_LABEL[f]}: {s['failures']} failures in {s['n']} "
                        f"trials, 95% upper bound {s['cp95_upper']}"
                        for f, s in pooled.items()
                    ),
    }
    # H3: F5 runs under the Git bundle record exactly one code version.
    f5 = [t for t in trials if t["phase"] == "F5"]
    mixes = sum(1 for t in f5 if t.get("version_mix"))
    verdicts["H3"] = {
        "verdict": "Supported" if f5 and mixes == 0 else "Refuted",
        "evidence": f"{len(f5)} Git-bundle runs, {mixes} with mixed code versions; "
                    f"marker versions seen: "
                    f"{sorted({tuple(t.get('marker_versions', [])) for t in f5})}",
    }
    # H4a: hardened F1/F2 extra executions == 1 in at least 90% of trials.
    hf = [t for t in trials if t["arm"] == "hardened"
          and t.get("fault") in ("kill_mid_write", "partial_write_error")]
    h4a_share = (
        sum(1 for t in hf if t["extra_task_executions"] == 1) / len(hf) if hf else 0
    )
    # H4b: at least 80% of failed naive F1/F2 trials are green-but-wrong.
    nf = [t for t in trials if t["arm"] == "naive"
          and t.get("fault") in ("kill_mid_write", "partial_write_error")
          and not t["passed"]]
    h4b_share = (
        sum(1 for t in nf if t["green_but_wrong"]) / len(nf) if nf else 0
    )
    verdicts["H4"] = {
        "verdict": "Supported" if (h4a_share >= 0.9 and h4b_share >= 0.8)
                   else "Partially supported"
                   if (h4a_share >= 0.9 or h4b_share >= 0.8) else "Refuted",
        "evidence": f"H4a: exactly 1 extra execution in {h4a_share:.0%} of "
                    f"{len(hf)} hardened F1/F2 trials. H4b: green-but-wrong in "
                    f"{h4b_share:.0%} of {len(nf)} failed naive F1/F2 trials",
    }
    return verdicts


def render(trials, env):
    verdicts = compute_verdicts(trials)
    lines = []
    lines.append("# Results (generated)")
    lines.append("")
    lines.append("Generated by src/airrel/report.py from results/trials.jsonl. "
                 "Do not edit by hand.")
    lines.append("")
    lines.append(f"- Airflow version: {env.get('airflow_version', 'unknown')}")
    lines.append(f"- Python: {env.get('python_version', 'unknown')}")
    lines.append(f"- Trials: {env.get('trial_count', len(trials))} "
                 f"(F1/F2 n per cell: {env.get('n_f1f2_per_cell', 'unknown')})")
    lines.append(f"- Study window (UTC): {env.get('started_utc', '?')} to "
                 f"{env.get('finished_utc', '?')}")
    if env.get("phase_errors"):
        lines.append(f"- Phase errors: {env['phase_errors']}")
    lines.append("")
    lines.append("## Verdicts (HYPOTHESES.md)")
    lines.append("")
    for hid in ("H1", "H2", "H3", "H4"):
        v = verdicts[hid]
        lines.append(f"- {hid}: **{v['verdict']}**. {v['evidence']}.")
    lines.append("")
    lines.append("## Controls")
    lines.append("")
    lines.append("| Arm | Trials | Failed | Median recovery (s) |")
    lines.append("|---|---|---|---|")
    for arm in ("naive", "hardened"):
        recs = _cell(trials, phase="control", arm=arm)
        s = _failure_stats(recs)
        lines.append(f"| {arm} | {s['n']} | {s['failures']} | "
                     f"{_median_recovery(recs)} |")
    lines.append("")
    lines.append("## Fault matrix summary (failed trials / trials, 95% upper bound "
                 "on the failure rate)")
    lines.append("")
    lines.append("| Fault | Naive | Hardened |")
    lines.append("|---|---|---|")
    for fault, label in FAULT_LABEL.items():
        cells = []
        for arm in ("naive", "hardened"):
            s = _failure_stats(_cell(trials, fault=fault, arm=arm))
            cells.append(f"{s['failures']}/{s['n']} (bound {s['cp95_upper']})")
        lines.append(f"| {label} | {cells[0]} | {cells[1]} |")
    lines.append("")
    lines.append("## Per-cell detail")
    lines.append("")
    lines.append("| Phase | Pipeline | Arm | Trials | Failed | Duplicates "
                 "| Losses | Mismatches | Median recovery (s) | Median extra "
                 "task executions |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    grouped = defaultdict(list)
    for t in trials:
        if t["phase"] in ("smoke",):
            continue
        grouped[(t["phase"], t["pipeline"], t["arm"])].append(t)
    for (phase, pipeline, arm) in sorted(grouped):
        recs = grouped[(phase, pipeline, arm)]
        s = _failure_stats(recs)
        extras = [t["extra_task_executions"] for t in recs]
        lines.append(
            f"| {phase} | {pipeline} | {arm} | {s['n']} | {s['failures']} | "
            f"{sum(t['duplicates'] for t in recs)} | "
            f"{sum(t['losses'] for t in recs)} | "
            f"{sum(t['content_mismatches'] for t in recs)} | "
            f"{_median_recovery(recs)} | "
            f"{statistics.median(extras) if extras else 0} |"
        )
    lines.append("")
    lines.append("## Exploratory control (not part of any verdict)")
    lines.append("")
    for t in trials:
        if t["phase"] == "F5-control":
            lines.append(f"- Local (unversioned) bundle, mid-run deploy: marker "
                         f"versions {t.get('marker_versions')}, version_mix "
                         f"{t.get('version_mix')}.")
    lines.append("")
    return "\n".join(lines), verdicts


def main():
    results_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("results")
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("RESULTS.md")
    trials, env = load(results_dir)
    text, verdicts = render(trials, env)
    out_path.write_text(text + "\n", encoding="utf-8")
    summary = {
        "environment": env,
        "verdicts": verdicts,
        "trials": len(trials),
    }
    (results_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(f"wrote {out_path} and {results_dir / 'summary.json'}")


if __name__ == "__main__":
    main()
