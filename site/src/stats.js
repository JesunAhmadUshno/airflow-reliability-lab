// JavaScript port of src/airrel/stats.py. The one-sided Clopper-Pearson upper
// bound p solves P(X <= k | p) = alpha, found by bisection on the binomial CDF.
// The CDF is summed in log space so large n does not underflow.
export function binomCdf(k, n, p) {
  if (p <= 0) return 1;
  if (p >= 1) return k >= n ? 1 : 0;
  let logPmf = n * Math.log1p(-p);
  let total = Math.exp(logPmf);
  const ratio = Math.log(p) - Math.log1p(-p);
  for (let i = 0; i < k; i++) {
    logPmf += Math.log((n - i) / (i + 1)) + ratio;
    total += Math.exp(logPmf);
  }
  return total;
}

export function clopperPearsonUpper(k, n, alpha = 0.05) {
  if (n <= 0 || k >= n) return 1;
  let lo = 0, hi = 1;
  for (let i = 0; i < 200; i++) {
    const mid = (lo + hi) / 2;
    if (binomCdf(k, n, mid) > alpha) lo = mid; else hi = mid;
    if (hi - lo < 1e-10) break;
  }
  return (lo + hi) / 2;
}

// stats.failure_summary rounds the bound to 4 decimals.
export const round4 = (x) => Number(x.toFixed(4));

// Smallest n whose bound with k failures falls below target (null if beyond maxN).
export function trialsForBound(target, k = 0, alpha = 0.05, maxN = 100000) {
  if (!(target > 0 && target < 1)) return null;
  let lo = k + 1, hi = maxN;
  if (clopperPearsonUpper(k, hi, alpha) >= target) return null;
  while (lo < hi) { // the bound falls as n grows, so bisect on n
    const mid = Math.floor((lo + hi) / 2);
    if (clopperPearsonUpper(k, mid, alpha) < target) hi = mid; else lo = mid + 1;
  }
  return lo;
}
