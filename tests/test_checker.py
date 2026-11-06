import sqlite3

import pytest

from airrel import checker, corpus


@pytest.fixture()
def corpus_dir(tmp_path):
    corpus.write_corpus(tmp_path)
    return tmp_path


def _seeded(tmp_path, corpus_dir, tables=("documents", "embeddings")):
    sink = tmp_path / "sink.db"
    checker.seed_sink(sink, corpus_dir, "2026-01-02", tables=tables,
                      include_reference=True)
    return sink


def test_clean_sink_passes(tmp_path, corpus_dir):
    sink = _seeded(tmp_path, corpus_dir)
    result = checker.check_trial(sink, corpus_dir, "embedding", "2026-01-02")
    assert result["passed"]
    result = checker.check_documents(sink, corpus_dir, "2026-01-02")
    assert result["duplicates"] == 0 and result["losses"] == 0


def test_checker_flags_planted_duplicate(tmp_path, corpus_dir):
    sink = _seeded(tmp_path, corpus_dir)
    conn = sqlite3.connect(str(sink))
    row = conn.execute(
        "SELECT partition, doc_id, body, content_hash FROM documents LIMIT 1"
    ).fetchone()
    conn.execute("INSERT INTO documents VALUES (?, ?, ?, ?)", row)
    conn.commit()
    conn.close()
    result = checker.check_documents(sink, corpus_dir, "2026-01-02")
    assert result["duplicates"] == 1


def test_checker_flags_planted_loss(tmp_path, corpus_dir):
    sink = _seeded(tmp_path, corpus_dir)
    conn = sqlite3.connect(str(sink))
    conn.execute("DELETE FROM embeddings WHERE doc_id = 'p20260102-d0003'")
    conn.commit()
    conn.close()
    result = checker.check_embeddings(sink, corpus_dir, "2026-01-02")
    assert result["losses"] == 1


def test_checker_flags_content_mismatch(tmp_path, corpus_dir):
    sink = _seeded(tmp_path, corpus_dir)
    conn = sqlite3.connect(str(sink))
    conn.execute(
        "UPDATE embeddings SET vec_hash = 'deadbeef' WHERE doc_id = 'p20260102-d0005'"
    )
    conn.commit()
    conn.close()
    result = checker.check_embeddings(sink, corpus_dir, "2026-01-02")
    assert result["content_mismatches"] == 1


def test_eval_drift_expectations_on_seeded_sink(tmp_path, corpus_dir):
    sink = _seeded(tmp_path, corpus_dir)
    # No eval rows yet: exactly one loss, no duplicates.
    result = checker.check_eval(sink, corpus_dir, "2026-01-02")
    assert result["losses"] == 1
    stat, flag = checker._expected_drift(corpus_dir, "2026-01-02")
    assert flag == 0  # base partition is close to the reference partition
    stat_s, flag_s = checker._expected_drift(corpus_dir, corpus.SHIFTED_PARTITION)
    assert flag_s == 1 and stat_s > stat
