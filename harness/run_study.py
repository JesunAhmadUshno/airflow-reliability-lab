"""AIR-REL study harness: runs the pre-registered fault matrix (SPEC.md).

Usage (inside the study virtual environment):
    python harness/run_study.py --root workdir/run1 --airflow-bin .venv/bin/airflow

Phases: setup, smoke, controls, F1, F2, F3, F6, F5, F4, local-bundle control,
teardown. Trial records are appended to <root>/trials.jsonl as they complete
and copied to results/ at the end. RESULTS.md is generated separately by
src/airrel/report.py.
"""
import argparse
import json
import shutil
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "harness"))

from airrel import checker, corpus as corpus_mod  # noqa: E402
from airflow_ctl import AirflowDeployment  # noqa: E402

PIPELINES = ["ingestion", "embedding", "evaluation", "retraining", "monitoring"]
DAG_ID = {p: f"airrel_{p}" for p in PIPELINES}
BASE_TASKS = {"ingestion": 3, "embedding": 2, "evaluation": 2,
              "retraining": 2, "monitoring": 2}
SEED_TABLES = {
    "ingestion": None,
    "embedding": ("documents",),
    "evaluation": ("embeddings",),
    "retraining": ("embeddings",),
    "monitoring": ("embeddings",),
}
F4_PIPELINES = ["ingestion", "embedding", "retraining"]
F6_PIPELINES = ["ingestion", "embedding"]
ARMS = ["naive", "hardened"]


def iso_epoch(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


class Study:
    def __init__(self, root, airflow_bin, concurrency=4):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.airflow_bin = airflow_bin
        self.concurrency = concurrency
        self.corpus_dir = self.root / "corpus"
        self.bundle_repo = self.root / "bundle_repo"
        self.trials_path = self.root / "trials.jsonl"
        self.records = []
        self.dep = None
        self.n_f1f2 = 10
        import itertools
        self._logical_counter = itertools.count()

    # -- setup --------------------------------------------------------------
    def setup_bundle(self):
        if self.bundle_repo.exists():
            shutil.rmtree(self.bundle_repo)
        self.bundle_repo.mkdir(parents=True)
        for item in (REPO / "dags_src").iterdir():
            if item.is_file():
                shutil.copy(item, self.bundle_repo / item.name)
        def git(*args):
            subprocess.run(["git", "-C", str(self.bundle_repo), *args],
                           check=True, capture_output=True, text=True)
        git("init", "-b", "main")
        git("config", "user.email", "airrel@example.local")
        git("config", "user.name", "AIR-REL harness")
        git("add", "-A")
        git("commit", "-q", "-m", "DAG bundle v1 (from dags_src)")

    def bundle_head(self):
        out = subprocess.run(
            ["git", "-C", str(self.bundle_repo), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True)
        return out.stdout.strip()

    def set_bundle_marker(self, version):
        path = self.bundle_repo / "airrel_common.py"
        text = path.read_text(encoding="utf-8")
        other = "v2" if version == "v1" else "v1"
        assert f'CODE_VERSION = "{other}"' in text or f'CODE_VERSION = "{version}"' in text
        text = text.replace(f'CODE_VERSION = "{other}"', f'CODE_VERSION = "{version}"')
        path.write_text(text, encoding="utf-8")
        subprocess.run(["git", "-C", str(self.bundle_repo), "add", "-A"],
                       check=True, capture_output=True)
        subprocess.run(
            ["git", "-C", str(self.bundle_repo), "commit", "-q", "-m",
             f"bundle marker {version}"],
            check=True, capture_output=True)
        return self.bundle_head()

    def bundle_config(self):
        return [{
            "name": "airrel-bundle",
            "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
            "kwargs": {
                "repo_url": str(self.bundle_repo),
                "tracking_ref": "main",
                "refresh_interval": 5,
            },
        }]

    def setup_deployment(self):
        corpus_mod.write_corpus(self.corpus_dir)
        self.setup_bundle()
        self.dep = AirflowDeployment(self.root, self.airflow_bin, self.bundle_config())
        self.dep.migrate()
        self.dep.create_git_connection(str(self.bundle_repo))
        self.dep.start_all()
        self.dep.wait_health()
        self.dep.wait_dags([DAG_ID[p] for p in PIPELINES])

    # -- trial plumbing -------------------------------------------------------
    def trial_dirs(self, trial_id):
        base = self.root / "trials" / trial_id
        (base / "markers").mkdir(parents=True, exist_ok=True)
        (base / "artifacts").mkdir(parents=True, exist_ok=True)
        return base

    def base_conf(self, trial_id, arm, partition, sink):
        base = self.root / "trials" / trial_id
        return {
            "arm": arm,
            "partition": partition,
            "sink_path": str(sink),
            "corpus_dir": str(self.corpus_dir),
            "artifacts_dir": str(base / "artifacts"),
            "marker_dir": str(base / "markers"),
        }

    def run_one(self, trial_id, dag_id, conf, timeout=300):
        run_id = f"{trial_id}--{uuid.uuid4().hex[:10]}"
        logical = self.unique_logical(conf["partition"])
        last_err = None
        for attempt in range(3):
            try:
                self.dep.trigger(dag_id, run_id, conf, logical_date=logical)
                break
            except Exception as exc:  # transient API hiccup; logical_date is
                last_err = exc          # required by the 3.4 API, never dropped
                time.sleep(2)
        else:
            raise RuntimeError(f"trigger failed for {run_id}: {last_err}")
        run = self.dep.wait_run(dag_id, run_id, timeout=timeout)
        tis = self.dep.task_instances(dag_id, run_id)
        return {"run_id": run_id, "state": run.get("state"),
                "start": run.get("start_date"), "end": run.get("end_date"),
                "tis": tis}

    def unique_logical(self, partition):
        # The 3.4 create-run API rejects a second run with the same
        # logical_date for a DAG (HTTP 409), so each trigger gets a unique
        # logical timestamp inside the partition day. The data interval
        # under study is the partition, carried in conf (DECISIONS D12).
        n = next(self._logical_counter)
        return f"{partition}T00:{n // 60:02d}:{n % 60:02d}Z"

    def fault_epoch(self, trial_id, run_ids):
        base = self.root / "trials" / trial_id / "markers"
        epochs = []
        for path in base.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if data.get("run_id") in run_ids:
                    epochs.append(data["epoch"])
            except Exception:
                pass
        return min(epochs) if epochs else None

    def finish_record(self, spec, runs, check, recovery=None, extra_notes=(),
                      extra_fields=None):
        # The 3.4 API lists one task-instance entry per try, so the entry
        # count is the execution count (summing try_number would double
        # count: a try-2 entry is one execution, not two).
        total_tries = sum(len(r["tis"]) for r in runs)
        baseline = sum(BASE_TASKS[r["pipeline"]] for r in runs)
        record = {
            "trial_id": spec["trial_id"],
            "phase": spec["phase"],
            "pipeline": spec["pipeline"],
            "arm": spec["arm"],
            "fault": spec.get("fault"),
            "partition": spec["partition"],
            "run_ids": [r["run_id"] for r in runs],
            "run_states": [r["state"] for r in runs],
            "duplicates": check["duplicates"],
            "losses": check["losses"],
            "content_mismatches": check["content_mismatches"],
            "extra_keys": check["extra_keys"],
            "checker_tables": check["tables"],
            "passed": check["passed"],
            "extra_task_executions": total_tries - baseline,
            "recovery_seconds": recovery,
            "green_but_wrong": bool(
                runs and runs[-1]["state"] == "success" and not check["passed"]
            ),
            "notes": list(extra_notes),
        }
        if extra_fields:
            record.update(extra_fields)
        self.append_record(record)
        return record

    def append_record(self, record):
        self.records.append(record)
        with open(self.trials_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")
        status = "PASS" if record["passed"] else "FAIL"
        print(f"[{record['phase']}] {record['trial_id']}: {status} "
              f"dup={record['duplicates']} loss={record['losses']} "
              f"mismatch={record['content_mismatches']}", flush=True)

    def seed(self, spec, sink):
        if spec["phase"] == "F6":
            return  # chain trials build their own state from an empty sink
        tables = SEED_TABLES[spec["pipeline"]]
        if tables:
            checker.seed_sink(
                sink, self.corpus_dir, spec["partition"],
                tables=tables,
                include_reference=(spec["pipeline"] == "monitoring"),
            )

    def standard_trial(self, spec):
        """One pipeline, one or two sequential runs, then the checker."""
        trial_id = spec["trial_id"]
        self.trial_dirs(trial_id)
        sink = self.root / "trials" / trial_id / "sink.db"
        self.seed(spec, sink)
        runs = []
        for step in spec["steps"]:
            conf = self.base_conf(trial_id, spec["arm"], spec["partition"], sink)
            conf.update(step.get("conf", {}))
            run = self.run_one(trial_id, DAG_ID[step["pipeline"]], conf,
                               timeout=step.get("timeout", 300))
            run["pipeline"] = step["pipeline"]
            runs.append(run)
        check = checker.check_trial(
            sink, self.corpus_dir, spec["pipeline"], spec["partition"],
            late=spec.get("late_check", False), tables=spec.get("check_tables"),
        )
        recovery = None
        if spec.get("fault") in ("kill_mid_write", "partial_write_error"):
            epoch = self.fault_epoch(trial_id, [r["run_id"] for r in runs])
            end = iso_epoch(runs[-1]["end"])
            if epoch and end:
                recovery = round(end - epoch, 2)
        elif spec["phase"] in ("F3", "F6") and len(runs) >= 2:
            start, end = iso_epoch(runs[-1]["start"]), iso_epoch(runs[-1]["end"])
            if start and end:
                recovery = round(end - start, 2)
        return self.finish_record(spec, runs, check, recovery=recovery)

    def run_phase(self, specs, serial=False):
        if serial:
            return [self.standard_trial(s) for s in specs]
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            return list(pool.map(self.standard_trial, specs))

    # -- phases ---------------------------------------------------------------
    def phase_smoke(self):
        specs = []
        for arm in ARMS:
            specs.append({
                "trial_id": f"smoke-ingestion-{arm}", "phase": "smoke",
                "pipeline": "ingestion", "arm": arm,
                "partition": corpus_mod.TRIAL_PARTITION,
                "steps": [{"pipeline": "ingestion"}],
            })
        started = time.time()
        records = self.run_phase(specs)
        wall = (time.time() - started) / len(records)
        durations = []
        for rec in records:
            if not rec["passed"]:
                raise RuntimeError(f"smoke trial failed: {rec['trial_id']}")
        print(f"smoke ok, mean trial wall {wall:.1f}s", flush=True)
        self.smoke_wall = wall
        return wall

    def phase_controls(self):
        specs = []
        for pipeline in PIPELINES:
            for arm in ARMS:
                for i in range(3):
                    specs.append({
                        "trial_id": f"control-{pipeline}-{arm}-{i}",
                        "phase": "control", "pipeline": pipeline, "arm": arm,
                        "partition": corpus_mod.TRIAL_PARTITION,
                        "steps": [{"pipeline": pipeline}],
                    })
        for arm in ARMS:
            for i in range(3):
                specs.append({
                    "trial_id": f"control-monitoring-shifted-{arm}-{i}",
                    "phase": "control", "pipeline": "monitoring", "arm": arm,
                    "partition": corpus_mod.SHIFTED_PARTITION,
                    "steps": [{"pipeline": "monitoring"}],
                })
        return self.run_phase(specs)

    def phase_fault(self, phase, fault, n):
        specs = []
        for pipeline in PIPELINES:
            for arm in ARMS:
                for i in range(n):
                    specs.append({
                        "trial_id": f"{phase.lower()}-{pipeline}-{arm}-{i}",
                        "phase": phase, "pipeline": pipeline, "arm": arm,
                        "fault": fault,
                        "partition": corpus_mod.TRIAL_PARTITION,
                        "steps": [{"pipeline": pipeline, "conf": {"fault": fault}}],
                    })
        return self.run_phase(specs)

    def phase_f3(self):
        specs = []
        for pipeline in PIPELINES:
            for arm in ARMS:
                for i in range(3):
                    specs.append({
                        "trial_id": f"f3-{pipeline}-{arm}-{i}",
                        "phase": "F3", "pipeline": pipeline, "arm": arm,
                        "fault": "duplicate_trigger",
                        "partition": corpus_mod.TRIAL_PARTITION,
                        "steps": [{"pipeline": pipeline}, {"pipeline": pipeline}],
                    })
        return self.run_phase(specs)

    def phase_f6(self):
        specs = []
        for arm in ARMS:
            for i in range(3):
                specs.append({
                    "trial_id": f"f6-ingestion-{arm}-{i}",
                    "phase": "F6", "pipeline": "ingestion", "arm": arm,
                    "fault": "late_data_rerun",
                    "partition": corpus_mod.TRIAL_PARTITION,
                    "late_check": True,
                    "steps": [
                        {"pipeline": "ingestion"},
                        {"pipeline": "ingestion", "conf": {"late_applied": True}},
                    ],
                })
                specs.append({
                    "trial_id": f"f6-embedding-{arm}-{i}",
                    "phase": "F6", "pipeline": "embedding", "arm": arm,
                    "fault": "late_data_rerun",
                    "partition": corpus_mod.TRIAL_PARTITION,
                    "late_check": True,
                    "check_tables": ["documents", "embeddings"],
                    "steps": [
                        {"pipeline": "ingestion"},
                        {"pipeline": "ingestion", "conf": {"late_applied": True}},
                        {"pipeline": "embedding"},
                    ],
                })
        return self.run_phase(specs)

    def phase_f5(self):
        records = []
        for arm in ARMS:
            for i in range(3):
                records.append(self.f5_trial(arm, i))
        return records

    def wait_bundle_version(self, sha, timeout=240):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                for bundle in self.dep.bundles():
                    if bundle.get("name") == "airrel-bundle" and \
                            bundle.get("version") == sha:
                        return True
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError(f"bundle version {sha} not seen via API")

    def f5_trial(self, arm, idx):
        trial_id = f"f5-ingestion-{arm}-{idx}"
        spec = {"trial_id": trial_id, "phase": "F5", "pipeline": "ingestion",
                "arm": arm, "fault": "code_deploy_mid_run",
                "partition": corpus_mod.TRIAL_PARTITION}
        self.trial_dirs(trial_id)
        sink = self.root / "trials" / trial_id / "sink.db"
        sha_v1 = self.set_bundle_marker("v1")
        self.wait_bundle_version(sha_v1)
        gate = self.root / "trials" / trial_id / "gate.release"
        conf = self.base_conf(trial_id, arm, spec["partition"], sink)
        conf["gate_file"] = str(gate)
        run_id = f"{trial_id}--{uuid.uuid4().hex[:10]}"
        self.dep.trigger(DAG_ID["ingestion"], run_id, conf,
                         logical_date=self.unique_logical(spec["partition"]))
        deadline = time.time() + 180
        while time.time() < deadline:
            tis = self.dep.task_instances(DAG_ID["ingestion"], run_id)
            states = {ti["task_id"]: ti["state"] for ti in tis}
            if states.get("prepare") == "success":
                break
            time.sleep(1.5)
        else:
            raise RuntimeError(f"F5 prepare never succeeded in {trial_id}")
        sha_v2 = self.set_bundle_marker("v2")
        self.wait_bundle_version(sha_v2)
        gate.touch()
        run = self.dep.wait_run(DAG_ID["ingestion"], run_id, timeout=600)
        tis = self.dep.task_instances(DAG_ID["ingestion"], run_id)
        versions = checker.marker_versions(sink, run_id)
        check = {"duplicates": 0, "losses": 0, "content_mismatches": 0,
                 "extra_keys": 0, "tables": {},
                 "passed": len(versions) == 1}
        record = self.finish_record(
            spec,
            [{"run_id": run_id, "state": run.get("state"),
              "start": run.get("start_date"), "end": run.get("end_date"),
              "tis": tis, "pipeline": "ingestion"}],
            check,
            extra_notes=(f"marker_versions={versions}",),
            extra_fields={"marker_versions": versions,
                          "version_mix": len(versions) > 1},
        )
        return record

    def phase_f4(self):
        sha_v1 = self.set_bundle_marker("v1")
        self.wait_bundle_version(sha_v1)
        records = []
        for pipeline in F4_PIPELINES:
            for arm in ARMS:
                for i in range(3):
                    records.append(self.f4_trial(pipeline, arm, i))
        return records

    def f4_trial(self, pipeline, arm, idx):
        trial_id = f"f4-{pipeline}-{arm}-{idx}"
        spec = {"trial_id": trial_id, "phase": "F4", "pipeline": pipeline,
                "arm": arm, "fault": "scheduler_restart",
                "partition": corpus_mod.TRIAL_PARTITION}
        self.trial_dirs(trial_id)
        sink = self.root / "trials" / trial_id / "sink.db"
        self.seed(spec, sink)
        conf = self.base_conf(trial_id, arm, spec["partition"], sink)
        conf["slow_write"] = True
        run_id = f"{trial_id}--{uuid.uuid4().hex[:10]}"
        self.dep.trigger(DAG_ID[pipeline], run_id, conf,
                         logical_date=self.unique_logical(spec["partition"]))
        deadline = time.time() + 240
        write_running_at = None
        while time.time() < deadline:
            tis = self.dep.task_instances(DAG_ID[pipeline], run_id)
            states = {ti["task_id"]: ti["state"] for ti in tis}
            if states.get("write") == "running":
                write_running_at = time.time()
                break
            if states.get("write") in ("success", "failed"):
                break
            time.sleep(1)
        if write_running_at:
            time.sleep(6)
        kill_epoch = time.time()
        self.dep.stop("scheduler")
        time.sleep(5)
        self.dep.start("scheduler")
        run = self.dep.wait_run(DAG_ID[pipeline], run_id, timeout=900)
        tis = self.dep.task_instances(DAG_ID[pipeline], run_id)
        check = checker.check_trial(sink, self.corpus_dir, pipeline,
                                    spec["partition"])
        end = iso_epoch(run.get("end_date"))
        recovery = round(end - kill_epoch, 2) if end else None
        record = self.finish_record(
            spec,
            [{"run_id": run_id, "state": run.get("state"),
              "start": run.get("start_date"), "end": run.get("end_date"),
              "tis": tis, "pipeline": pipeline}],
            check, recovery=recovery,
            extra_notes=(f"scheduler_killed_at={kill_epoch:.1f}",),
        )
        return record

    def phase_local_control(self):
        """Exploratory F5 control under an unversioned local bundle (D9)."""
        trial_id = "f5-control-local-bundle"
        spec = {"trial_id": trial_id, "phase": "F5-control",
                "pipeline": "ingestion", "arm": "control-local",
                "fault": "code_deploy_mid_run",
                "partition": corpus_mod.TRIAL_PARTITION}
        self.dep.stop_all()
        local_dir = self.root / "local_dags"
        if local_dir.exists():
            shutil.rmtree(local_dir)
        shutil.copytree(self.bundle_repo, local_dir,
                        ignore=shutil.ignore_patterns(".git"))
        local_config = [{
            "name": "airrel-local",
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {"path": str(local_dir)},
        }]
        dep2 = AirflowDeployment(self.root, self.airflow_bin, local_config)
        self.dep = dep2
        dep2.start_all()
        dep2.wait_health()
        dep2.wait_dags([DAG_ID[p] for p in PIPELINES])
        self.trial_dirs(trial_id)
        sink = self.root / "trials" / trial_id / "sink.db"
        gate = self.root / "trials" / trial_id / "gate.release"
        conf = self.base_conf(trial_id, "hardened", spec["partition"], sink)
        conf["gate_file"] = str(gate)
        run_id = f"{trial_id}--{uuid.uuid4().hex[:10]}"
        dep2.trigger(DAG_ID["ingestion"], run_id, conf,
                     logical_date=self.unique_logical(spec["partition"]))
        deadline = time.time() + 180
        while time.time() < deadline:
            tis = dep2.task_instances(DAG_ID["ingestion"], run_id)
            states = {ti["task_id"]: ti["state"] for ti in tis}
            if states.get("prepare") == "success":
                break
            time.sleep(1.5)
        path = local_dir / "airrel_common.py"
        text = path.read_text(encoding="utf-8")
        path.write_text(text.replace('CODE_VERSION = "v1"', 'CODE_VERSION = "v2"'),
                        encoding="utf-8")
        time.sleep(20)
        gate.touch()
        run = dep2.wait_run(DAG_ID["ingestion"], run_id, timeout=600)
        tis = dep2.task_instances(DAG_ID["ingestion"], run_id)
        versions = checker.marker_versions(sink, run_id)
        check = {"duplicates": 0, "losses": 0, "content_mismatches": 0,
                 "extra_keys": 0, "tables": {},
                 "passed": len(versions) == 1}
        record = self.finish_record(
            spec,
            [{"run_id": run_id, "state": run.get("state"),
              "start": run.get("start_date"), "end": run.get("end_date"),
              "tis": tis, "pipeline": "ingestion"}],
            check, extra_notes=(f"marker_versions={versions}",),
            extra_fields={"marker_versions": versions,
                          "version_mix": len(versions) > 1},
        )
        dep2.stop_all()
        return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--airflow-bin", required=True)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--skip-phases", default="")
    args = parser.parse_args()
    skip = {p.strip() for p in args.skip_phases.split(",") if p.strip()}

    study = Study(args.root, args.airflow_bin, concurrency=args.concurrency)
    started = datetime.now(timezone.utc).isoformat()
    study.setup_deployment()
    try:
        wall = study.phase_smoke()
        # Pre-registered adaptation rule (SPEC.md section 6).
        study.n_f1f2 = 6 if wall > 25 else 10
        print(f"F1/F2 n per cell: {study.n_f1f2}", flush=True)
        phases = [
            ("controls", study.phase_controls),
            ("f1", lambda: study.phase_fault("F1", "kill_mid_write", study.n_f1f2)),
            ("f2", lambda: study.phase_fault("F2", "partial_write_error",
                                             study.n_f1f2)),
            ("f3", study.phase_f3),
            ("f6", study.phase_f6),
            ("f5", study.phase_f5),
            ("f4", study.phase_f4),
            ("control-local", study.phase_local_control),
        ]
        study.phase_errors = {}
        for name, fn in phases:
            if name in skip:
                continue
            try:
                fn()
            except Exception as exc:
                study.phase_errors[name] = repr(exc)
                print(f"PHASE {name} ERRORED: {exc!r}", flush=True)
    finally:
        if study.dep is not None:
            study.dep.stop_all()
    env = {
        "airflow_version": subprocess.run(
            [args.airflow_bin, "version"], capture_output=True, text=True
        ).stdout.strip(),
        "python_version": sys.version.split()[0],
        "harness_python": sys.executable,
        "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "n_f1f2_per_cell": study.n_f1f2,
        "smoke_mean_wall_seconds": round(study.smoke_wall, 2),
        "trial_count": len(study.records),
        "phase_errors": getattr(study, "phase_errors", {}),
        "study_repo_head": subprocess.run(
            ["git", "-C", str(REPO), "rev-parse", "HEAD"],
            capture_output=True, text=True).stdout.strip(),
    }
    results_dir = REPO / "results"
    results_dir.mkdir(exist_ok=True)
    shutil.copy(study.trials_path, results_dir / "trials.jsonl")
    (results_dir / "environment.json").write_text(
        json.dumps(env, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(env, indent=2), flush=True)


if __name__ == "__main__":
    main()
