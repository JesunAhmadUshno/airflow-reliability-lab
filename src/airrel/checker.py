"""Independent invariant checker (SPEC.md section 7, METHODS.md).

Reads only the corpus files and the trial sink. Reimplements the embedder
from SPEC.md section 4 (src/airrel/embedder.py) and never imports the DAG
code under test. Also provides seed_sink, which writes the exact expected
fixture state for downstream-pipeline trials; seeding is test setup, not
pipeline code.
"""
import hashlib
import json
import math
import sqlite3
from pathlib import Path

from . import corpus as corpus_mod
from . import embedder

MODEL_VERSION = embedder.MODEL_VERSION
REFERENCE_PARTITION = corpus_mod.REFERENCE_PARTITION
DRIFT_THRESHOLD = 0.25

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


def _connect(path):
    conn = sqlite3.connect(str(path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _expected_docs(corpus_dir, partition, late):
    return {
        (d["partition"], d["doc_id"]): d
        for d in corpus_mod.read_partition(corpus_dir, partition, late=late)
    }


def _expected_embeddings(corpus_dir, partition, late):
    expected = {}
    for (part, doc_id), doc in _expected_docs(corpus_dir, partition, late).items():
        vector = embedder.embed(doc["body"])
        expected[(part, doc_id, MODEL_VERSION)] = embedder.vec_hash(vector)
    return expected


def _group_check(rows, expected_keys):
    """rows: list of key tuples (with repetition). Returns dup/loss counts."""
    counts = {}
    for key in rows:
        counts[key] = counts.get(key, 0) + 1
    duplicates = sum(c - 1 for c in counts.values() if c > 1)
    extra = sum(1 for k in counts if k not in expected_keys)
    losses = sum(1 for k in expected_keys if k not in counts)
    return duplicates, losses, extra


def check_documents(sink_path, corpus_dir, partition, late=False):
    expected = _expected_docs(corpus_dir, partition, late)
    conn = _connect(sink_path)
    rows = conn.execute(
        "SELECT partition, doc_id, content_hash FROM documents"
    ).fetchall()
    conn.close()
    duplicates, losses, extra = _group_check(
        [(r[0], r[1]) for r in rows], set(expected)
    )
    mismatches = 0
    seen = {}
    for part, doc_id, content_hash in rows:
        key = (part, doc_id)
        if key in expected and key not in seen:
            seen[key] = True
            if content_hash != expected[key]["content_hash"]:
                mismatches += 1
    return {
        "duplicates": duplicates,
        "losses": losses,
        "extra_keys": extra,
        "content_mismatches": mismatches,
        "rows": len(rows),
    }


def check_embeddings(sink_path, corpus_dir, partition, late=False):
    expected = _expected_embeddings(corpus_dir, partition, late)
    conn = _connect(sink_path)
    rows = conn.execute(
        "SELECT partition, doc_id, model_version, vec_hash FROM embeddings"
    ).fetchall()
    conn.close()
    duplicates, losses, extra = _group_check(
        [(r[0], r[1], r[2]) for r in rows], set(expected)
    )
    mismatches = 0
    seen = {}
    for part, doc_id, model, vh in rows:
        key = (part, doc_id, model)
        if key in expected and key not in seen:
            seen[key] = True
            if vh != expected[key]:
                mismatches += 1
    return {
        "duplicates": duplicates,
        "losses": losses,
        "extra_keys": extra,
        "content_mismatches": mismatches,
        "rows": len(rows),
    }


def _expected_mean_norm(corpus_dir, partition, late):
    vectors = [
        embedder.embed(d["body"])
        for d in _expected_docs(corpus_dir, partition, late).values()
    ]
    norms = [math.sqrt(sum(x * x for x in v)) for v in vectors]
    return round(sum(norms) / len(norms), 6) if norms else 0.0


def check_eval(sink_path, corpus_dir, partition, late=False):
    expected_n = len(_expected_embeddings(corpus_dir, partition, late))
    expected_norm = _expected_mean_norm(corpus_dir, partition, late)
    conn = _connect(sink_path)
    rows = conn.execute(
        "SELECT n_docs, mean_norm FROM eval_runs WHERE partition = ? "
        "AND model_version = ?",
        (partition, MODEL_VERSION),
    ).fetchall()
    conn.close()
    if not rows:
        return {"duplicates": 0, "losses": 1, "extra_keys": 0,
                "content_mismatches": 0, "rows": 0}
    mismatches = sum(
        1 for n_docs, mean_norm in rows
        if n_docs != expected_n or abs(mean_norm - expected_norm) > 1e-4
    )
    return {
        "duplicates": len(rows) - 1,
        "losses": 0,
        "extra_keys": 0,
        "content_mismatches": mismatches,
        "rows": len(rows),
    }


def _expected_centroid(corpus_dir, partition, late):
    docs = _expected_docs(corpus_dir, partition, late)
    vectors = [embedder.embed(d["body"]) for d in docs.values()]
    if not vectors:
        return [0.0] * embedder.DIM, 0
    centroid = [
        round(sum(v[i] for v in vectors) / len(vectors), 6)
        for i in range(embedder.DIM)
    ]
    return centroid, len(vectors)


def check_registry(sink_path, corpus_dir, partition, late=False):
    centroid, n_train = _expected_centroid(corpus_dir, partition, late)
    payload = {
        "model_name": "centroid-model",
        "partition": partition,
        "model_version": MODEL_VERSION,
        "n_train": n_train,
        "centroid": centroid,
    }
    expected_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    conn = _connect(sink_path)
    rows = conn.execute(
        "SELECT artifact_path, artifact_hash, n_train FROM model_registry "
        "WHERE partition = ? AND model_name = ?",
        (partition, "centroid-model"),
    ).fetchall()
    artifact_files = list(
        (Path(rows[0][0]).parent).glob("model-*.json")
    ) if rows else []
    conn.close()
    if not rows:
        return {"duplicates": 0, "losses": 1, "extra_keys": 0,
                "content_mismatches": 0, "rows": 0, "artifact_files": 0}
    mismatches = 0
    for artifact_path, artifact_hash, n_row in rows:
        ok = artifact_hash == expected_hash and n_row == n_train
        path = Path(artifact_path)
        if not path.exists():
            ok = False
        else:
            file_hash = hashlib.sha256(path.read_bytes()).hexdigest()
            if file_hash != artifact_hash:
                ok = False
        if not ok:
            mismatches += 1
    mismatches += max(0, len(artifact_files) - 1)
    return {
        "duplicates": len(rows) - 1,
        "losses": 0,
        "extra_keys": 0,
        "content_mismatches": mismatches,
        "rows": len(rows),
        "artifact_files": len(artifact_files),
    }


def _mean_vector(vectors):
    if not vectors:
        return [0.0] * embedder.DIM
    return [sum(v[i] for v in vectors) / len(vectors) for i in range(embedder.DIM)]


def _expected_drift(corpus_dir, partition, late=False):
    current = [
        embedder.embed(d["body"])
        for d in _expected_docs(corpus_dir, partition, late).values()
    ]
    reference = [
        embedder.embed(d["body"])
        for d in _expected_docs(corpus_dir, REFERENCE_PARTITION, False).values()
    ]
    mc, mr = _mean_vector(current), _mean_vector(reference)
    stat = math.sqrt(sum((a - b) ** 2 for a, b in zip(mc, mr)))
    return round(stat, 6), 1 if stat > DRIFT_THRESHOLD else 0


def check_drift(sink_path, corpus_dir, partition, late=False):
    expected_stat, expected_flag = _expected_drift(corpus_dir, partition, late)
    conn = _connect(sink_path)
    rows = conn.execute(
        "SELECT drift_stat, drift_flag FROM drift_reports WHERE partition = ? "
        "AND reference_partition = ?",
        (partition, REFERENCE_PARTITION),
    ).fetchall()
    conn.close()
    if not rows:
        return {"duplicates": 0, "losses": 1, "extra_keys": 0,
                "content_mismatches": 0, "rows": 0,
                "expected_stat": expected_stat, "expected_flag": expected_flag}
    mismatches = sum(
        1 for stat, flag in rows
        if abs(stat - expected_stat) > 1e-4 or flag != expected_flag
    )
    return {
        "duplicates": len(rows) - 1,
        "losses": 0,
        "extra_keys": 0,
        "content_mismatches": mismatches,
        "rows": len(rows),
        "expected_stat": expected_stat,
        "expected_flag": expected_flag,
    }


_CHECKS = {
    "documents": check_documents,
    "embeddings": check_embeddings,
    "eval_runs": check_eval,
    "model_registry": check_registry,
    "drift_reports": check_drift,
}

PIPELINE_TABLES = {
    "ingestion": ["documents"],
    "embedding": ["embeddings"],
    "evaluation": ["eval_runs"],
    "retraining": ["model_registry"],
    "monitoring": ["drift_reports"],
}


def check_trial(sink_path, corpus_dir, pipeline, partition, late=False, tables=None):
    tables = tables or PIPELINE_TABLES[pipeline]
    per_table = {}
    total = {"duplicates": 0, "losses": 0, "content_mismatches": 0, "extra_keys": 0}
    for table in tables:
        result = _CHECKS[table](sink_path, corpus_dir, partition, late=late)
        per_table[table] = result
        for key in total:
            total[key] += result.get(key, 0)
    total["passed"] = (
        total["duplicates"] == 0
        and total["losses"] == 0
        and total["content_mismatches"] == 0
    )
    return {"tables": per_table, **total}


def marker_versions(sink_path, run_id):
    conn = _connect(sink_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT code_version FROM run_markers WHERE run_id = ?",
            (run_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    conn.close()
    return sorted(r[0] for r in rows)


def seed_sink(sink_path, corpus_dir, partition, late=False,
              tables=("documents", "embeddings"), include_reference=False):
    """Write the exact expected fixture state (test setup, not a pipeline)."""
    conn = _connect(sink_path)
    conn.executescript(SCHEMA)
    partitions = [partition] + ([REFERENCE_PARTITION] if include_reference else [])
    for part in partitions:
        docs = _expected_docs(corpus_dir, part, late if part == partition else False)
        if "documents" in tables:
            for (p, doc_id), doc in docs.items():
                conn.execute(
                    "INSERT INTO documents (partition, doc_id, body, content_hash) "
                    "VALUES (?, ?, ?, ?)",
                    (p, doc_id, doc["body"], doc["content_hash"]),
                )
        if "embeddings" in tables:
            for (p, doc_id), doc in docs.items():
                vector = embedder.embed(doc["body"])
                conn.execute(
                    "INSERT INTO embeddings (partition, doc_id, model_version, "
                    "vector_json, vec_hash) VALUES (?, ?, ?, ?, ?)",
                    (p, doc_id, MODEL_VERSION, json.dumps(vector),
                     embedder.vec_hash(vector)),
                )
    conn.commit()
    conn.close()
