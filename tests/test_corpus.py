from airrel import corpus


def test_corpus_is_deterministic():
    a = corpus.make_document("2026-01-02", 7)
    b = corpus.make_document("2026-01-02", 7)
    assert a == b


def test_partition_sizes():
    assert len(corpus.partition_documents("2026-01-02")) == 200
    assert len(corpus.partition_documents("2026-01-02", late=True)) == 240


def test_doc_ids_unique_and_partition_scoped():
    docs = corpus.partition_documents("2026-01-02", late=True)
    ids = [d["doc_id"] for d in docs]
    assert len(set(ids)) == len(ids)
    assert all(d["partition"] == "2026-01-02" for d in docs)


def test_shifted_vocabulary_is_disjoint():
    normal = corpus.make_document("2026-01-02", 0)["body"].split()
    shifted = corpus.make_document(corpus.SHIFTED_PARTITION, 0)["body"].split()
    assert set(normal).isdisjoint(set(shifted))
