from airrel.drift_calibration import calibrate


def test_calibration_reproduces_spec_amendment_a1():
    # SPEC.md amendment A1 quotes these values; the threshold must sit between them.
    cal = calibrate()
    stats = {row["partition"]: row["drift_stat"] for row in cal["partitions"]}
    assert round(stats["2026-01-02"], 4) == 0.1039
    assert round(stats["2026-01-03"], 4) == 0.1028
    assert round(stats["2026-01-09"], 4) == 0.4948
    in_dist = [r["drift_stat"] for r in cal["partitions"] if not r["shifted"]]
    shifted = [r["drift_stat"] for r in cal["partitions"] if r["shifted"]]
    assert max(in_dist) < cal["threshold"] < min(shifted)
