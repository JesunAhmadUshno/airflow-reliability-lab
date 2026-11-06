"""Local Airflow 3.x deployment control for the AIR-REL harness.

Stdlib only. Starts and stops the API server, scheduler and DAG processor
as separate processes (the scheduler must be restartable on its own for
fault F4), triggers runs through the public REST API, and reads task
instance records from the metadata DB.
"""
import base64
import json
import os
import secrets
import signal
import sqlite3
import subprocess
import time
import urllib.request
from pathlib import Path

API_BASE = "http://127.0.0.1:8080/api/v2"


class AirflowDeployment:
    def __init__(self, root, airflow_bin, bundle_config_list, admin_password="airrel-admin"):
        self.root = Path(root)
        self.airflow_bin = str(airflow_bin)
        self.bundle_config_list = bundle_config_list
        self.admin_password = admin_password
        self.home = self.root / "airflow_home"
        self.logs = self.root / "logs"
        self.home.mkdir(parents=True, exist_ok=True)
        self.logs.mkdir(parents=True, exist_ok=True)
        (self.root / "empty_dags").mkdir(parents=True, exist_ok=True)
        self.processes = {}
        fernet = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii")
        self._env = {
            **os.environ,
            "AIRFLOW_HOME": str(self.home),
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__DAGS_FOLDER": str(self.root / "empty_dags"),
            "AIRFLOW__CORE__FERNET_KEY": fernet,
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{self.home}/airflow.db",
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(
                bundle_config_list
            ),
            "AIRFLOW__API__SECRET_KEY": secrets.token_hex(32),
            "AIRFLOW__CORE__PARALLELISM": "16",
        }

    # -- lifecycle ---------------------------------------------------------
    def _run_cli(self, args, timeout=300):
        return subprocess.run(
            [self.airflow_bin, *args],
            env=self._env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def migrate(self):
        result = self._run_cli(["db", "migrate"])
        if result.returncode != 0:
            raise RuntimeError(f"db migrate failed: {result.stderr[-2000:]}")

    def start(self, name, extra_args=()):
        log_path = self.logs / f"{name}.log"
        fh = open(log_path, "ab")
        proc = subprocess.Popen(
            [self.airflow_bin, name, *extra_args],
            env=self._env,
            stdout=fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.processes[name] = proc
        return proc

    def stop(self, name, timeout=30):
        proc = self.processes.pop(name, None)
        if proc is None or proc.poll() is not None:
            return
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)

    def stop_all(self):
        for name in list(self.processes):
            self.stop(name)

    def start_all(self):
        self.start("api-server", ("--port", "8080"))
        self.start("dag-processor")
        self.start("scheduler")

    # -- REST API ----------------------------------------------------------
    def _auth_header(self):
        token = base64.b64encode(
            f"admin:{self.admin_password}".encode("utf-8")
        ).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    def api(self, method, path, body=None, timeout=30):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            API_BASE + path,
            data=data,
            method=method,
            headers={**self._auth_header(), "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def wait_health(self, timeout=240):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                health = self.api("GET", "/monitor/health")
                if health.get("metadatabase", {}).get("status") == "healthy":
                    return health
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("API server did not become healthy")

    def wait_dags(self, expected_ids, timeout=300):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                data = self.api("GET", "/dags?limit=200")
                found = {d["dag_id"] for d in data.get("dags", [])}
                if set(expected_ids) <= found:
                    return found
            except Exception:
                pass
            time.sleep(3)
        raise RuntimeError(f"DAGs did not appear: wanted {expected_ids}")

    def bundles(self):
        data = self.api("GET", "/dagBundles")
        return data.get("dag_bundles", data if isinstance(data, list) else [])

    def trigger(self, dag_id, run_id, conf, logical_date=None):
        body = {"dag_run_id": run_id, "conf": conf}
        if logical_date:
            body["logical_date"] = logical_date
        return self.api("POST", f"/dags/{dag_id}/dagRuns", body)

    def get_run(self, dag_id, run_id):
        return self.api("GET", f"/dags/{dag_id}/dagRuns/{run_id}")

    def wait_run(self, dag_id, run_id, timeout=600):
        deadline = time.time() + timeout
        while time.time() < deadline:
            run = self.get_run(dag_id, run_id)
            if run.get("state") in ("success", "failed"):
                return run
            time.sleep(1.5)
        raise TimeoutError(f"run {dag_id}/{run_id} did not finish in {timeout}s")

    # -- metadata DB -------------------------------------------------------
    def task_instances(self, dag_id, run_id):
        db = self.home / "airflow.db"
        conn = sqlite3.connect(str(db), timeout=60)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT task_id, try_number, state, duration FROM task_instance "
            "WHERE dag_id = ? AND run_id = ? ORDER BY task_id",
            (dag_id, run_id),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
