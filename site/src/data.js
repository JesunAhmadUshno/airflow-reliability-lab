// Pure helpers over results/trials.jsonl, mirroring src/airrel/report.py.
export const parseTrials = (raw) => raw.split('\n').filter((l) => l.trim()).map((l) => JSON.parse(l));

// FAULT_LABEL and PIPELINES are read from report.py so labels and order match RESULTS.md.
export function parseReportPy(src) {
  const block = src.match(/FAULT_LABEL = \{([\s\S]*?)\}/)[1];
  const faults = [...block.matchAll(/"(\w+)": "(F\d) ([^"]+)"/g)].map(([, id, code, label]) => ({ id, code, label }));
  const pipelines = [...src.match(/PIPELINES = \[([^\]]*)\]/)[1].matchAll(/"(\w+)"/g)].map((m) => m[1]);
  const writePath = [...src.match(/write_path = \[([^\]]*)\]/)[1].matchAll(/"(\w+)"/g)].map((m) => m[1]);
  return { faults, pipelines, writePath };
}

export const cell = (trials, f = {}) => trials.filter((t) => Object.entries(f).every(([k, v]) => v == null || t[k] === v));
export const failures = (recs) => recs.filter((t) => !t.passed).length;
export const sum = (recs, key) => recs.reduce((a, t) => a + t[key], 0);
export function median(values) {
  const v = [...values].sort((a, b) => a - b), m = v.length >> 1;
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
}
// toFixed rounds the exact binary value, like Python round(); Math.round would not.
export const round2 = (x) => Number(x.toFixed(2));
export function medianRecovery(recs) {
  const v = recs.map((t) => t.recovery_seconds).filter(Boolean);
  return v.length ? round2(median(v)) : null;
}
export const greenRun = (t) => t.run_states.length > 0 && t.run_states.every((s) => s === 'success');

// Headline counts: naive and hardened trials under the write-path and platform faults.
export function headline(trials, writePath) {
  const naiveWrite = trials.filter((t) => t.arm === 'naive' && writePath.includes(t.fault));
  const hardenedFault = trials.filter((t) => t.arm === 'hardened' && t.fault);
  const f12 = trials.filter((t) => t.arm === 'naive' && ['F1', 'F2'].includes(t.phase));
  return {
    naiveWriteTrials: naiveWrite.length, naiveWriteFailed: failures(naiveWrite),
    hardenedFaultTrials: hardenedFault.length, hardenedFaultFailed: failures(hardenedFault),
    naiveF12Duplicates: sum(f12, 'duplicates'),
    totalLosses: sum(trials, 'losses'),
  };
}
