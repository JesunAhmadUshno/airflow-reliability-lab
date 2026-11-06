import pytest

from airrel.stats import clopper_pearson_upper, failure_summary


def test_cp_upper_known_values():
    # For k = 0 the bound is 1 - alpha ** (1 / n).
    assert clopper_pearson_upper(0, 10) == pytest.approx(0.2589, abs=1e-3)
    assert clopper_pearson_upper(0, 50) == pytest.approx(0.0582, abs=1e-3)
    assert clopper_pearson_upper(0, 3) == pytest.approx(0.6316, abs=1e-3)


def test_cp_upper_monotone_and_bounded():
    vals = [clopper_pearson_upper(k, 20) for k in range(0, 21)]
    assert all(0.0 <= v <= 1.0 for v in vals)
    assert all(b >= a for a, b in zip(vals, vals[1:]))
    assert clopper_pearson_upper(20, 20) == 1.0


def test_failure_summary():
    s = failure_summary(0, 50)
    assert s["failures"] == 0 and s["n"] == 50 and s["rate"] == 0.0
    assert s["cp95_upper"] == pytest.approx(0.0582, abs=1e-3)
