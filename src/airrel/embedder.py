"""Checker-side implementation of the hash embedder (SPEC.md section 4).

This is an independent reimplementation from the written spec. The DAG side
carries its own copy in dags_src/airrel_common.py. The two implementations
must agree because they follow the same spec, not because they share code.
"""
import hashlib
import math
import re

DIM = 32
MODEL_VERSION = "hash-embed-v1"
_TOKEN_RE = re.compile(r"[a-z0-9]+")


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


def embed_hash(text):
    return vec_hash(embed(text))
