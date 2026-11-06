"""Shared task code for the AIR-REL pipelines.

This module is part of the DAG bundle under test. The invariant checker in
src/airrel never imports it; the checker follows SPEC.md only.

Arm difference (SPEC.md section 5): only the read scope and the write
functions differ between the naive and hardened arms. The naive arm
deliberately uses the patterns the Airflow best-practices page warns against
(append INSERT, batch commits, latest-data reads, wall-clock keys). The
hardened arm uses the page's own rules (one transaction per task, UPSERT on
deterministic keys, fixed-partition reads, no wall-clock keys).
"""
import hashlib
import json
import math
import os
import re
import signal
import sqlite3
import time
from pathlib import Path

CODE_VERSION = "v1"
MODEL_VERSION = "hash-embed-v1"
DIM = 32
BATCH = 25
DRIFT_THRESHOLD = 0.25
REFERENCE_PARTITION = "2026-01-01"
_TOKEN_RE = re.compile(r"[a-z0-9]+")

SCHEMA = """
CREATE TABLE IF NOT EXISTS run_markers (
    run_id TEXT, task_id TEXT, code_version TEXT, ts REAL
);
CREATE TABLE IF NOT EXISTS documents (
    partition TEXT, doc_id TEXT, body TEXT, content_hash TEXT
);
CREATE TABLE IF NOT EXISTS embeddings (
    partition TEXT, doc_id TEXT, model_version TEXT, vector_json TEXT, vec_hash TEXT
);
CREATE TABLE IF NOT EXISTS eval_runs (
    eval_key TEXT, partition TEXT, model_version TEXT, n_docs INTEGER, mean_norm REAL
);
CREATE TABLE IF NOT EXISTS model_registry (
    model_key TEXT, partition TEXT, model_name TEXT,
    artifact_path TEXT, artifact_hash TEXT, n_train INTEGER
);
CREATE TABLE IF NOT EXISTS drift_reports (
    report_key TEXT, partition TEXT, reference_partition TEXT,
    drift_stat REAL, drift_flag INTEGER
);
"""


# --------------------------------------------------------------------------
# Spec primitives (SPEC.md section 4). The checker reimplements these from
# the spec text; agreement is by spec, not by shared code.
# --------------------------------------------------------------------------

def embed(text):
    vec = [0.0] * DIM
    for token in _TOKEN_RE.findall(text.lower()):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        bucket = int.from_bytes(digest[0:4], "little") % DIM
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vec[bucket] += sign
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [round(x / norm, 6) for x in vec]


def vec_hash(vector):
    joined = ",".join(f"{x:.6f}" for x in vector)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Plumbing
# --------------------------------------------------------------------------

def conf_of(context):
    dag_run = context.get("dag_run")
    return dict(getattr(dag_run, "conf", None) or {})


def run_id_of(context):
    dag_run = context.get("dag_run")
    return getattr(dag_run, "run_id", "unknown-run")


def connect(sink_path):
    conn = sqlite3.connect(str(sink_path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_sink(sink_path):
    conn = connect(sink_path)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()


def write_marker(conf, context, task_id):
    conn = connect(conf["sink_path"])
    conn.execute(
        "INSERT INTO run_markers (run_id, task_id, code_version, ts) VALUES (?, ?, ?, ?)",
        (run_id_of(context), task_id, CODE_VERSION, time.time()),
    )
    conn.commit()
    conn.close()


def _safe(name):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(name))


def maybe_fault(conf, context):
    """Fire the armed fault once, at the kill point chosen by the caller.

    The fault marker file is the guard: it is written when the fault fires,
    so the Airflow retry of the same task sees the marker and does not fire
    again. The marker also timestamps the fault for recovery measurement.
    """
    fault = conf.get("fault")
    if fault not in ("kill_mid_write", "partial_write_error"):
        return
    marker_dir = Path(conf["marker_dir"])
    marker_dir.mkdir(parents=True, exist_ok=True)
    marker = marker_dir / f"{_safe(run_id_of(context))}.{fault}.json"
    if marker.exists():
        return
    marker.write_text(
        json.dumps({"fault": fault, "epoch": time.time(), "run_id": run_id_of(context)}),
        encoding="utf-8",
    )
    if fault == "kill_mid_write":
        os.kill(os.getpid(), signal.SIGKILL)
    raise RuntimeError("AIR-REL injected fault: partial_write_error")


def slow_sleep(conf, seconds):
    if conf.get("slow_write"):
        time.sleep(seconds)


def read_corpus_docs(conf):
    path = Path(conf["corpus_dir"]) / f"{conf['partition']}.jsonl"
    docs = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
    if conf.get("late_applied"):
        late = Path(conf["corpus_dir"]) / f"{conf['partition']}.late.jsonl"
        docs += [json.loads(line) for line in open(late, encoding="utf-8") if line.strip()]
    return docs


def _batches(rows, size=BATCH):
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


# --------------------------------------------------------------------------
# Row-table writes (documents, embeddings): the two arms.
# rows: list of dicts. key_cols: natural key columns. table: sink table.
# --------------------------------------------------------------------------

def write_rows(conf, context, table, rows, key_cols):
    sink = conf["sink_path"]
    total = len(rows)
    half = max(1, total // 2)
    cols = list(rows[0].keys()) if rows else []
    placeholders = ", ".join("?" for _ in cols)
    col_list = ", ".join(cols)
    if conf["arm"] == "naive":
        conn = connect(sink)
        done = 0
        for batch in _batches(rows):
            for row in batch:
                conn.execute(
                    f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
                    [row[c] for c in cols],
                )
            conn.commit()
            done += len(batch)
            slow_sleep(conf, 1.5)
            if done >= half:
                maybe_fault(conf, context)
        conn.close()
    else:
        conn = connect(sink)
        where = " AND ".join(f"{c} = ?" for c in key_cols)
        for i, row in enumerate(rows):
            conn.execute(
                f"DELETE FROM {table} WHERE {where}", [row[c] for c in key_cols]
            )
            conn.execute(
                f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})",
                [row[c] for c in cols],
            )
            if (i + 1) % 25 == 0:
                # Same pacing as the naive arm (1.5s per 25 rows), inside
                # the transaction: without it the hardened write is a fast
                # burst and the timed F4 kill lands after the run ends
                # (DECISIONS D15).
                slow_sleep(conf, 1.5)
            if i + 1 == half:
                maybe_fault(conf, context)
        conn.commit()
        conn.close()


def write_single_row(conf, context, insert_fn):
    """Single-row tables (eval, registry, drift). insert_fn(conn) does the
    arm-specific insert and returns nothing. Fault point: naive fires after
    its commit (the row is durable, the retry writes a second one); hardened
    fires inside the transaction (the retry starts from a rolled-back sink).
    """
    sink = conf["sink_path"]
    if conf["arm"] == "naive":
        conn = connect(sink)
        insert_fn(conn)
        conn.commit()
        conn.close()
        maybe_fault(conf, context)
    else:
        conn = connect(sink)
        insert_fn(conn)
        maybe_fault(conf, context)
        conn.commit()
        conn.close()


def _timestamp_key(prefix, partition):
    return f"{prefix}-{partition}-{time.time():.6f}"


# --------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------

def ingestion_prepare(**context):
    conf = conf_of(context)
    init_sink(conf["sink_path"])
    write_marker(conf, context, "prepare")
    docs = read_corpus_docs(conf)
    return {"n_source": len(docs)}


def gate_wait(**context):
    """No-op unless conf names a gate file (fault F5). The gate keeps the
    write task from starting until the harness has deployed the new code
    version, so the write task's code version is sampled after the deploy.
    """
    conf = conf_of(context)
    gate = conf.get("gate_file")
    if not gate:
        return {"gate": "none"}
    deadline = time.time() + 900
    while time.time() < deadline:
        if Path(gate).exists():
            return {"gate": "released"}
        time.sleep(1)
    raise RuntimeError("AIR-REL gate file never appeared")


def ingestion_write(**context):
    conf = conf_of(context)
    write_marker(conf, context, "write")
    docs = read_corpus_docs(conf)
    rows = [
        {
            "partition": d["partition"],
            "doc_id": d["doc_id"],
            "body": d["body"],
            "content_hash": d["content_hash"],
        }
        for d in docs
    ]
    slow_sleep(conf, 2)
    write_rows(conf, context, "documents", rows, ["partition", "doc_id"])
    slow_sleep(conf, 2)
    return {"n_written": len(rows)}


# --------------------------------------------------------------------------
# Embedding
# --------------------------------------------------------------------------

def _read_documents(conf):
    conn = connect(conf["sink_path"])
    if conf["arm"] == "naive":
        rows = conn.execute(
            "SELECT partition, doc_id, body FROM documents ORDER BY rowid"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT partition, doc_id, body FROM documents WHERE partition = ? "
            "ORDER BY doc_id",
            (conf["partition"],),
        ).fetchall()
    conn.close()
    return rows


def embedding_prepare(**context):
    conf = conf_of(context)
    init_sink(conf["sink_path"])
    write_marker(conf, context, "prepare")
    return {"n_sink_rows": len(_read_documents(conf))}


def embedding_write(**context):
    conf = conf_of(context)
    write_marker(conf, context, "write")
    rows = []
    for partition, doc_id, body in _read_documents(conf):
        vector = embed(body)
        rows.append(
            {
                "partition": partition,
                "doc_id": doc_id,
                "model_version": MODEL_VERSION,
                "vector_json": json.dumps(vector),
                "vec_hash": vec_hash(vector),
            }
        )
    slow_sleep(conf, 2)
    write_rows(
        conf, context, "embeddings", rows, ["partition", "doc_id", "model_version"]
    )
    slow_sleep(conf, 2)
    return {"n_written": len(rows)}


# --------------------------------------------------------------------------
# Evaluation
# --------------------------------------------------------------------------

def _read_embeddings(conf, partition=None):
    conn = connect(conf["sink_path"])
    rows = conn.execute(
        "SELECT vector_json FROM embeddings WHERE partition = ? AND model_version = ? "
        "ORDER BY rowid",
        (partition or conf["partition"], MODEL_VERSION),
    ).fetchall()
    conn.close()
    return [json.loads(r[0]) for r in rows]


def evaluation_prepare(**context):
    conf = conf_of(context)
    init_sink(conf["sink_path"])
    write_marker(conf, context, "prepare")
    vectors = _read_embeddings(conf)
    norms = [math.sqrt(sum(x * x for x in v)) for v in vectors]
    mean_norm = sum(norms) / len(norms) if norms else 0.0
    return {"n_embeddings": len(vectors), "mean_norm": round(mean_norm, 6)}


def evaluation_write(**context):
    conf = conf_of(context)
    write_marker(conf, context, "write")
    ti = context["ti"]
    prep = ti.xcom_pull(task_ids="prepare")
    partition = conf["partition"]
    slow_sleep(conf, 14)

    def insert(conn):
        if conf["arm"] == "naive":
            key = _timestamp_key("eval", partition)
            conn.execute(
                "INSERT INTO eval_runs (eval_key, partition, model_version, n_docs, "
                "mean_norm) VALUES (?, ?, ?, ?, ?)",
                (key, partition, MODEL_VERSION, prep["n_embeddings"], prep["mean_norm"]),
            )
        else:
            key = f"eval-{partition}-{MODEL_VERSION}"
            conn.execute(
                "DELETE FROM eval_runs WHERE partition = ? AND model_version = ?",
                (partition, MODEL_VERSION),
            )
            conn.execute(
                "INSERT INTO eval_runs (eval_key, partition, model_version, n_docs, "
                "mean_norm) VALUES (?, ?, ?, ?, ?)",
                (key, partition, MODEL_VERSION, prep["n_embeddings"], prep["mean_norm"]),
            )

    write_single_row(conf, context, insert)
    return {"eval_key_rows": 1}


# --------------------------------------------------------------------------
# Retraining
# --------------------------------------------------------------------------

def retraining_prepare(**context):
    conf = conf_of(context)
    init_sink(conf["sink_path"])
    write_marker(conf, context, "prepare")
    vectors = _read_embeddings(conf)
    if vectors:
        centroid = [
            round(sum(v[i] for v in vectors) / len(vectors), 6) for i in range(DIM)
        ]
    else:
        centroid = [0.0] * DIM
    return {"centroid": centroid, "n_train": len(vectors)}


def retraining_write(**context):
    conf = conf_of(context)
    write_marker(conf, context, "write")
    ti = context["ti"]
    prep = ti.xcom_pull(task_ids="prepare")
    partition = conf["partition"]
    artifact_dir = Path(conf["artifacts_dir"]) / partition
    artifact_dir.mkdir(parents=True, exist_ok=True)
    slow_sleep(conf, 14)
    payload = {
        "model_name": "centroid-model",
        "partition": partition,
        "model_version": MODEL_VERSION,
        "n_train": prep["n_train"],
        "centroid": prep["centroid"],
    }
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    artifact_hash = hashlib.sha256(blob).hexdigest()

    if conf["arm"] == "naive":
        key = _timestamp_key("model", partition)
        final_path = artifact_dir / f"model-{key}.json"
        final_path.write_bytes(blob)

        def insert(conn):
            conn.execute(
                "INSERT INTO model_registry (model_key, partition, model_name, "
                "artifact_path, artifact_hash, n_train) VALUES (?, ?, ?, ?, ?, ?)",
                (key, partition, "centroid-model", str(final_path), artifact_hash,
                 prep["n_train"]),
            )

        write_single_row(conf, context, insert)
    else:
        model_key = f"centroid-{partition}-{artifact_hash[:12]}"
        final_path = artifact_dir / f"model-{artifact_hash[:16]}.json"
        tmp_path = artifact_dir / f".tmp-{model_key}.json"
        tmp_path.write_bytes(blob)
        os.replace(tmp_path, final_path)
        # Fault point for the hardened arm: artifact is durable and
        # idempotent, the registry transaction is not yet committed.
        maybe_fault(conf, context)

        def insert(conn):
            conn.execute(
                "DELETE FROM model_registry WHERE partition = ? AND model_name = ?",
                (partition, "centroid-model"),
            )
            conn.execute(
                "INSERT INTO model_registry (model_key, partition, model_name, "
                "artifact_path, artifact_hash, n_train) VALUES (?, ?, ?, ?, ?, ?)",
                (model_key, partition, "centroid-model", str(final_path),
                 artifact_hash, prep["n_train"]),
            )

        conn = connect(conf["sink_path"])
        insert(conn)
        conn.commit()
        conn.close()
    return {"artifact_hash": artifact_hash}


# --------------------------------------------------------------------------
# Monitoring
# --------------------------------------------------------------------------

def monitoring_prepare(**context):
    conf = conf_of(context)
    init_sink(conf["sink_path"])
    write_marker(conf, context, "prepare")
    current = _read_embeddings(conf)
    reference = _read_embeddings(conf, partition=REFERENCE_PARTITION)

    def mean_vec(vectors):
        if not vectors:
            return [0.0] * DIM
        return [sum(v[i] for v in vectors) / len(vectors) for i in range(DIM)]

    mc, mr = mean_vec(current), mean_vec(reference)
    stat = math.sqrt(sum((a - b) ** 2 for a, b in zip(mc, mr)))
    return {
        "drift_stat": round(stat, 6),
        "drift_flag": 1 if stat > DRIFT_THRESHOLD else 0,
        "n_current": len(current),
        "n_reference": len(reference),
    }


def monitoring_write(**context):
    conf = conf_of(context)
    write_marker(conf, context, "write")
    ti = context["ti"]
    prep = ti.xcom_pull(task_ids="prepare")
    partition = conf["partition"]
    slow_sleep(conf, 14)

    def insert(conn):
        if conf["arm"] == "naive":
            key = _timestamp_key("drift", partition)
            conn.execute(
                "INSERT INTO drift_reports (report_key, partition, "
                "reference_partition, drift_stat, drift_flag) VALUES (?, ?, ?, ?, ?)",
                (key, partition, REFERENCE_PARTITION, prep["drift_stat"],
                 prep["drift_flag"]),
            )
        else:
            key = f"drift-{partition}-{REFERENCE_PARTITION}"
            conn.execute(
                "DELETE FROM drift_reports WHERE partition = ? AND "
                "reference_partition = ?",
                (partition, REFERENCE_PARTITION),
            )
            conn.execute(
                "INSERT INTO drift_reports (report_key, partition, "
                "reference_partition, drift_stat, drift_flag) VALUES (?, ?, ?, ?, ?)",
                (key, partition, REFERENCE_PARTITION, prep["drift_stat"],
                 prep["drift_flag"]),
            )

    write_single_row(conf, context, insert)
    return {"drift_flag": prep["drift_flag"]}
