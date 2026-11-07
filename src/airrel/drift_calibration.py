"""Drift threshold calibration record (SPEC.md amendment A1).

Computes the monitoring drift statistic (distance between the mean embedding
of a partition and the mean embedding of the reference partition) for every
corpus partition, from the corpus generator and the checker-side embedder.
This is a property of the test fixtures, not of any trial outcome. It moves
the calibration values quoted in SPEC.md into a generated file.

Usage: python -m airrel.drift_calibration [output_path]
"""
import json
import math
import sys
from pathlib import Path

from . import corpus
from .checker import DRIFT_THRESHOLD
from .embedder import DIM, embed


def _mean_vector(partition):
    vectors = [embed(d["body"]) for d in corpus.partition_documents(partition)]
    return [sum(v[i] for v in vectors) / len(vectors) for i in range(DIM)]


def calibrate():
    ref = _mean_vector(corpus.REFERENCE_PARTITION)
    rows = []
    for partition in corpus.BASE_PARTITIONS + [corpus.SHIFTED_PARTITION]:
        if partition == corpus.REFERENCE_PARTITION:
            continue
        cur = _mean_vector(partition)
        stat = math.sqrt(sum((a - b) ** 2 for a, b in zip(cur, ref)))
        rows.append({
            "partition": partition,
            "shifted": partition == corpus.SHIFTED_PARTITION,
            "drift_stat": round(stat, 6),
            "drift_flag": 1 if stat > DRIFT_THRESHOLD else 0,
        })
    return {
        "reference_partition": corpus.REFERENCE_PARTITION,
        "docs_per_partition": corpus.DOCS_PER_PARTITION,
        "threshold": DRIFT_THRESHOLD,
        "partitions": rows,
    }


def main():
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("results/drift_calibration.json")
    out.write_text(json.dumps(calibrate(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
