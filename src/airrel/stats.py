"""Small statistics helpers (formulas in METHODS.md).

The one-sided Clopper-Pearson upper bound for a failure probability given
k failures in n trials is found by bisection on the binomial CDF:
the bound p solves P(X <= k | p) = alpha. No external statistics package
is needed, and n in this study is at most 60.
"""
import math


def _binom_cdf(k, n, p):
    if p <= 0.0:
        return 1.0
    if p >= 1.0:
        return 1.0 if k >= n else 0.0
    total = 0.0
    for i in range(0, k + 1):
        total += math.comb(n, i) * (p ** i) * ((1.0 - p) ** (n - i))
    return total


def clopper_pearson_upper(k, n, alpha=0.05):
    """One-sided (1 - alpha) upper confidence bound for a binomial rate."""
    if n <= 0:
        return 1.0
    if k >= n:
        return 1.0
    lo, hi = 0.0, 1.0
    for _ in range(200):
        mid = (lo + hi) / 2.0
        if _binom_cdf(k, n, mid) > alpha:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-10:
            break
    return (lo + hi) / 2.0


def failure_summary(failures, n):
    """Return (failures, n, rate, cp_upper) for one cell or pooled group."""
    rate = (failures / n) if n else 0.0
    return {
        "failures": failures,
        "n": n,
        "rate": round(rate, 4),
        "cp95_upper": round(clopper_pearson_upper(failures, n), 4),
    }
