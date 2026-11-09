// Known-answer check, run by "npm run build": node src/stats.check.mjs
// Every expected value is read from the repo's own result files and sources.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { clopperPearsonUpper, round4, trialsForBound } from './stats.js';
import { parseTrials, parseReportPy, cell, failures, sum, medianRecovery, headline } from './data.js';
import { parseConstants, simulate, SIM_FAULTS, SIM_PIPELINES } from './sim.js';
import { parseReport, parseReadme, parseHypotheses } from './docs.mjs';

const read = (p) => fs.readFileSync(new URL(`../../${p}`, import.meta.url), 'utf8');
const trials = parseTrials(read('results/trials.jsonl'));
const summary = JSON.parse(read('results/summary.json'));
const resultsMd = read('RESULTS.md');
const { faults, pipelines, writePath } = parseReportPy(read('src/airrel/report.py'));

// 1. Clopper-Pearson port reproduces every bound in summary.json and the RESULTS.md fault matrix.
const h2 = [...summary.verdicts.H2.evidence.matchAll(/(\d+) failures in (\d+) trials, 95% upper bound ([\d.]+)/g)];
assert.equal(h2.length, faults.length, 'H2 evidence lists every fault');
for (const [, k, n, want] of h2) assert.equal(round4(clopperPearsonUpper(+k, +n)), +want, `bound k=${k} n=${n}`);
const matrix = [...resultsMd.matchAll(/^\| (F\d) [^|]+\| (\d+)\/(\d+) \(bound ([\d.]+)\) \| (\d+)\/(\d+) \(bound ([\d.]+)\) \|$/gm)];
assert.equal(matrix.length, faults.length, 'RESULTS.md fault matrix rows');
for (const [, code, nf, nn, nb, hf, hn, hb] of matrix) {
  const f = faults.find((x) => x.code === code).id;
  for (const [arm, k, n, b] of [['naive', nf, nn, nb], ['hardened', hf, hn, hb]]) {
    const recs = cell(trials, { fault: f, arm });
    assert.equal(failures(recs), +k, `${code} ${arm} failures`);
    assert.equal(recs.length, +n, `${code} ${arm} n`);
    assert.equal(round4(clopperPearsonUpper(+k, +n)), +b, `${code} ${arm} bound`);
  }
}
// k = 0 closed form, and the inverse search agrees with the forward bound.
for (const n of [3, 6, 9, 15, 50]) assert.ok(Math.abs(clopperPearsonUpper(0, n) - (1 - 0.05 ** (1 / n))) < 1e-8);
const need = trialsForBound(0.01);
assert.ok(clopperPearsonUpper(0, need) < 0.01 && clopperPearsonUpper(0, need - 1) >= 0.01, 'trialsForBound');

// 2. Per-cell duplicates, losses, mismatches and median recovery match RESULTS.md.
const perCell = [...resultsMd.matchAll(/^\| (F\d|F5-control|control) \| (\w+) \| ([\w-]+) \| (\d+) \| (\d+) \| (\d+) \| (\d+) \| (\d+) \| ([\d.]+|n\/a) \|/gm)];
assert.ok(perCell.length > 40, 'per-cell rows parsed');
for (const [, phase, pipeline, arm, n, fails, dup, loss, mis, rec] of perCell) {
  const recs = cell(trials, { phase, pipeline, arm });
  const label = `${phase}/${pipeline}/${arm}`;
  assert.deepEqual([recs.length, failures(recs), sum(recs, 'duplicates'), sum(recs, 'losses'), sum(recs, 'content_mismatches')], [+n, +fails, +dup, +loss, +mis], label);
  assert.equal(medianRecovery(recs), rec === 'n/a' ? null : +rec, `${label} recovery`);
}

// 3. Headline counts agree with the README prose and summary.json.
const head = headline(trials, writePath);
const readme = read('README.md');
assert.equal(trials.length, summary.trials);
assert.ok(readme.includes(`failure rate 1.0 in ${head.naiveWriteFailed} of ${head.naiveWriteTrials}`), 'README 121 of 121');
assert.ok(readme.includes(`failed zero of its ${head.hardenedFaultTrials} fault trials`) && head.hardenedFaultFailed === 0, 'README 133');
assert.ok(readme.includes(`produced ${head.naiveF12Duplicates.toLocaleString('en-US')} duplicate records`), 'README 4,060');
assert.ok(readme.includes(`zero in all ${trials.length} trials`) && head.totalLosses === 0, 'README zero losses');

// 4. The sink model reproduces the recorded per-trial duplicates in every cell it covers.
const C = parseConstants(read('dags_src/airrel_common.py'), read('src/airrel/corpus.py'));
assert.deepEqual(C, { batch: 25, rows: 200, late: 40 }, 'constants parsed from the .py sources');
let covered = 0;
for (const [fault, , faultId] of SIM_FAULTS.filter((f) => f[2])) {
  for (const [pipeline, , recordedPipelines] of SIM_PIPELINES) {
    for (const p of recordedPipelines) {
      for (const arm of ['naive', 'hardened']) {
        const recs = cell(trials, { fault: faultId, pipeline: p, arm });
        if (!recs.length) continue; // not in the study matrix (DECISIONS D6)
        const m = simulate({ arm, fault, pipeline, ...C });
        for (const t of recs) {
          assert.equal(m.duplicates, t.duplicates, `${t.trial_id} duplicates`);
          assert.equal(m.losses, t.losses, `${t.trial_id} losses`);
          assert.equal(m.retries, t.extra_task_executions, `${t.trial_id} extra executions`);
        }
        covered += recs.length;
      }
    }
  }
}
for (const p of SIM_PIPELINES) for (const arm of ['naive', 'hardened']) assert.equal(simulate({ arm, fault: 'none', pipeline: p[0], ...C }).duplicates, 0);

// 5. Build-time document extraction is internally consistent.
const report = parseReport(read('REPORT.md'));
assert.ok(report.threats.length >= 5, 'REPORT.md threats to validity parsed');
assert.equal(report.preregCommit, '15d4e12', 'REPORT.md names the lock commit');
assert.ok(parseReadme(readme).runtimeMinutes > 0);
assert.deepEqual(parseHypotheses(read('HYPOTHESES.md')).map((h) => h.id), Object.keys(summary.verdicts));
assert.ok(pipelines.length === 5);

console.log('checks ok', { bounds: h2.map((m) => +m[3]), perCellRows: perCell.length, modelCoveredTrials: covered, threats: report.threats.length });
