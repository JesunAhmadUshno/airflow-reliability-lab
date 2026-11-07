from pathlib import Path

from airrel.report import FAULT_LABEL, compute_verdicts, load, render

RESULTS = Path(__file__).resolve().parent.parent / "results"


def test_h2_evidence_bounds_every_fault_cell():
    trials, _ = load(RESULTS)
    evidence = compute_verdicts(trials)["H2"]["evidence"]
    for label in FAULT_LABEL.values():
        assert label in evidence, label
    for bound in ("0.0582", "0.181", "0.2831", "0.393", "0.6316"):
        assert bound in evidence, bound


def test_report_prints_na_not_none_for_recovery():
    trials, env = load(RESULTS)
    text, _ = render(trials, env)
    assert "None" not in text
    assert "| n/a |" in text
