import math

import airrel_common as dag_side
from airrel import embedder


def test_embedder_deterministic_and_normalized():
    v1 = embedder.embed("tok001 tok002 tok003 tok001")
    v2 = embedder.embed("tok001 tok002 tok003 tok001")
    assert v1 == v2
    assert len(v1) == 32
    assert abs(math.sqrt(sum(x * x for x in v1)) - 1.0) < 1e-5


def test_checker_and_dag_implementations_agree():
    # Two independent implementations of SPEC.md section 4 must agree.
    for text in ("tok000 tok111 zzz999", "a b c", "tok399 " * 50):
        assert embedder.embed(text) == dag_side.embed(text)
        assert embedder.embed_hash(text) == dag_side.vec_hash(dag_side.embed(text))
