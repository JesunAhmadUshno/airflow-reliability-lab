"""Deterministic synthetic corpus generator (SPEC.md section 3).

The same seeds always produce the same corpus. Documents are plain text built
from a fixed vocabulary, so content hashes and embeddings are stable across
machines and runs.
"""
import hashlib
import json
import random
from pathlib import Path

BASE_PARTITIONS = ["2026-01-01", "2026-01-02", "2026-01-03"]
SHIFTED_PARTITION = "2026-01-09"
DOCS_PER_PARTITION = 200
LATE_DOCS = 40
TRIAL_PARTITION = "2026-01-02"
REFERENCE_PARTITION = "2026-01-01"

NORMAL_VOCAB = [f"tok{i:03d}" for i in range(400)]
SHIFTED_VOCAB = [f"zzz{i:03d}" for i in range(400)]


def _seed_int(*parts):
    digest = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little")


def make_document(partition, index, stream="base"):
    vocab = SHIFTED_VOCAB if partition == SHIFTED_PARTITION else NORMAL_VOCAB
    rng = random.Random(_seed_int("airrel", partition, stream, index))
    n_tokens = rng.randint(40, 80)
    body = " ".join(rng.choice(vocab) for _ in range(n_tokens))
    return {
        "partition": partition,
        "doc_id": f"p{partition.replace('-', '')}-d{index:04d}",
        "body": body,
        "content_hash": hashlib.sha256(body.encode("utf-8")).hexdigest(),
    }


def partition_documents(partition, late=False):
    docs = [make_document(partition, i) for i in range(DOCS_PER_PARTITION)]
    if late:
        docs += [
            make_document(partition, DOCS_PER_PARTITION + j, stream="late")
            for j in range(LATE_DOCS)
        ]
    return docs


def write_corpus(corpus_dir):
    """Write one JSONL file per partition, plus a .late.jsonl for F6."""
    corpus_dir = Path(corpus_dir)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for partition in BASE_PARTITIONS + [SHIFTED_PARTITION]:
        path = corpus_dir / f"{partition}.jsonl"
        with open(path, "w", encoding="utf-8") as fh:
            for doc in partition_documents(partition):
                fh.write(json.dumps(doc, sort_keys=True) + "\n")
        written.append(path)
        late_path = corpus_dir / f"{partition}.late.jsonl"
        with open(late_path, "w", encoding="utf-8") as fh:
            for doc in partition_documents(partition, late=True)[DOCS_PER_PARTITION:]:
                fh.write(json.dumps(doc, sort_keys=True) + "\n")
        written.append(late_path)
    return written


def read_partition(corpus_dir, partition, late=False):
    path = Path(corpus_dir) / f"{partition}.jsonl"
    docs = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
    if late:
        late_path = Path(corpus_dir) / f"{partition}.late.jsonl"
        docs += [json.loads(line) for line in open(late_path, encoding="utf-8") if line.strip()]
    return docs
