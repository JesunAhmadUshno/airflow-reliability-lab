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
import urllib.error
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
        (self.root / "tmp").mkdir(parents=True, exist_ok=True)
        self.processes = {}
        fernet = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii")
        self._env = {
            **os.environ,
            # DAG bundle checkouts live under tempfile.gettempdir(), keyed
            # only by bundle name: a machine-global location. Without this,
            # a second deployment silently keeps serving the first
            # deployment's bundle version (DECISIONS D14).
            "TMPDIR": str(self.root / "tmp"),
            "AIRFLOW_HOME": str(self.home),
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "False",
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

    def create_git_connection(self, host):
        result = self._run_cli([
            "connections", "add", "git_default",
            "--conn-type", "git", "--conn-host", str(host),
        ])
        if result.returncode != 0:
            raise RuntimeError(
                f"connections add failed: {result.stderr[-1000:]}")

    # -- REST API ----------------------------------------------------------
    def _read_generated_password(self):
        # SimpleAuthManager (the default in this install) generates the
        # admin password on first API server start and stores it in a
        # JSON file in AIRFLOW_HOME. It is authoritative for this local
        # throwaway deployment.
        password_file = self.home / "simple_auth_manager_passwords.json.generated"
        if password_file.exists():
            try:
                stored = json.loads(password_file.read_text(encoding="utf-8"))
                if stored.get("admin"):
                    self.admin_password = stored["admin"]
            except Exception:
                pass

    def _get_token(self):
        if getattr(self, "_token", None):
            return self._token
        self._read_generated_password()
        data = json.dumps(
            {"username": "admin", "password": self.admin_password}
        ).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:8080/auth/token",
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        self._token = payload["access_token"]
        return self._token

    def api(self, method, path, body=None, timeout=30):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        for attempt in range(2):
            req = urllib.request.Request(
                API_BASE + path,
                data=data,
                method=method,
                headers={
                    "Authorization": f"Bearer {self._get_token()}",
                    "Content-Type": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and attempt == 0:
                    self._token = None
                    continue
                raise

    def wait_health(self, timeout=240):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                health = self.api("GET", "/monitor/health")
                if health.get("metadatabase", {}).get("status") == "healthy":
                    # Health is unauthenticated; prove auth works too.
                    self.api("GET", "/dags?limit=1")
                    return health
            except Exception:
                pass
            time.sleep(2)
        raise RuntimeError("API server did not become healthy (or auth failed)")

    def wait_dags(self, expected_ids, timeout=300):
        deadline = time.time() + timeout
        last_err = None
        while time.time() < deadline:
            try:
                data = self.api("GET", "/dags?limit=200")
                found = {d["dag_id"] for d in data.get("dags", [])}
                if set(expected_ids) <= found:
                    return found
            except Exception as exc:
                last_err = exc
            time.sleep(3)
        raise RuntimeError(
            f"DAGs did not appear: wanted {expected_ids}; last error: {last_err!r}")

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
