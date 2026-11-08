import { useMemo, useState, useEffect, useId } from 'react';
import trialsRaw from '../../results/trials.jsonl?raw';
import summary from '../../results/summary.json';
import env from '../../results/environment.json';
import drift from '../../results/drift_calibration.json';
import reportPy from '../../src/airrel/report.py?raw';
import commonPy from '../../dags_src/airrel_common.py?raw';
import corpusPy from '../../src/airrel/corpus.py?raw';
import docs from 'virtual:repo-docs';
import { clopperPearsonUpper, round4, trialsForBound } from './stats.js';
import { parseTrials, parseReportPy, cell, failures, sum, median, medianRecovery, greenRun, headline } from './data.js';
import { parseConstants, simulate, SIM_FAULTS, SIM_PIPELINES } from './sim.js';

const REPO = 'https://github.com/JesunAhmadUshno/airflow-reliability-lab';
const blob = (p) => `${REPO}/blob/main/${p}`;
const fmt = (n) => n.toLocaleString('en-US');
const pct = (x, d = 1) => `${(x * 100).toFixed(d)}%`;

const trials = parseTrials(trialsRaw);
const { faults, pipelines, writePath } = parseReportPy(reportPy);
const C = parseConstants(commonPy, corpusPy);
const head = headline(trials, writePath);
const ARMS = ['naive', 'hardened'];
const faultOf = (id) => faults.find((f) => f.id === id);
const pooledHardened = cell(trials, { fault: 'kill_mid_write', arm: 'hardened' });
const pooledBound = round4(clopperPearsonUpper(failures(pooledHardened), pooledHardened.length));
// The study's bounds as summary.json prints them, for the calculator's seeds.
const summaryBounds = [...summary.verdicts.H2.evidence.matchAll(/(F\d) [^:]+: (\d+) failures in (\d+) trials, 95% upper bound ([\d.]+)/g)]
  .map(([, code, k, n, b]) => ({ code, k: +k, n: +n, bound: +b }));

const SECTIONS = [['top', 'Overview', '◎'], ['matrix', 'Fault matrix', '▦'], ['sim', 'Sink simulator', '▶'], ['green', 'Green but wrong', '✓'], ['bounds', 'Bounds', '≤'],
  ['recovery', 'Recovery', '◷'], ['pinning', 'Version pinning', '⎇'], ['hypotheses', 'Hypotheses', 'H'], ['limits', 'Limitations', '!'], ['reproduce', 'Reproduce', '❯_']];

function useReveal() {
  useEffect(() => {
    const els = document.querySelectorAll('.reveal');
    if (!('IntersectionObserver' in window)) { els.forEach((e) => e.classList.add('in')); return; }
    const io = new IntersectionObserver((es) => es.forEach((e) => { if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); } }), { threshold: 0.08 });
    els.forEach((e) => io.observe(e));
    return () => io.disconnect();
  }, []);
}

function MenuBar() {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => { const t = setInterval(() => setNow(new Date()), 30000); return () => clearInterval(t); }, []);
  return (
    <div className="menubar" role="banner">
      <span className="apple" aria-hidden="true">◆</span>
      <a className="app" href="#top">Airflow Reliability Lab</a>
      <a className="m" href={blob('SPEC.md')}>Spec</a>
      <a className="m" href={blob('HYPOTHESES.md')}>Hypotheses</a>
      <a className="m" href={blob('RESULTS.md')}>Results</a>
      <span className="spacer" />
      <span className="status"><i /> Pre-registered</span>
      <a href={REPO}>GitHub ↗</a>
      <span className="clock">{now.toLocaleString('en-US', { weekday: 'short', hour: 'numeric', minute: '2-digit' })}</span>
    </div>
  );
}

function Dock() {
  const [active, setActive] = useState('top');
  useEffect(() => {
    const io = new IntersectionObserver((es) => es.forEach((e) => e.isIntersecting && setActive(e.target.id)), { rootMargin: '-45% 0px -50% 0px' });
    SECTIONS.forEach(([id]) => { const el = document.getElementById(id); if (el) io.observe(el); });
    return () => io.disconnect();
  }, []);
  return (
    <nav className="dock" aria-label="Sections">
      {SECTIONS.map(([id, label, icon]) => (
        <a key={id} href={`#${id}`} className={active === id ? 'on' : ''} aria-current={active === id ? 'true' : undefined} aria-label={label}>
          <span className="ico" aria-hidden="true">{icon}</span><span className="lbl">{label}</span>
        </a>
      ))}
    </nav>
  );
}

// Charts switch to a compact layout on phones so their text stays readable.
function useNarrow() {
  const q = '(max-width: 760px)';
  const [narrow, setNarrow] = useState(() => typeof window !== 'undefined' && window.matchMedia(q).matches);
  useEffect(() => {
    const m = window.matchMedia(q), on = () => setNarrow(m.matches);
    m.addEventListener('change', on);
    return () => m.removeEventListener('change', on);
  }, []);
  return narrow;
}

function Range({ label, value, min, max, step, onChange, show }) {
  const id = useId();
  return (
    <div className="field">
      <label htmlFor={id}>{label} <span>{show}</span></label>
      <input id={id} type="range" min={min} max={max} step={step} value={value} onChange={(e) => onChange(+e.target.value)} />
    </div>
  );
}

function Select({ label, value, onChange, options }) {
  const id = useId();
  return (
    <div className="field">
      <label htmlFor={id}>{label}</label>
      <select id={id} value={value} onChange={(e) => onChange(e.target.value)}>
        {options.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
      </select>
    </div>
  );
}

export default function App() {
  useReveal();
  return (
    <div className="wrap">
      <MenuBar />
      <div className="glow" aria-hidden="true" />
      <Hero />
      <FaultMatrix />
      <SinkSimulator />
      <GreenButWrong />
      <Bounds />
      <Recovery />
      <Pinning />
      <Hypotheses />
      <Limitations />
      <Reproduce />
      <Dock />
      <footer>
        Code: Apache-2.0. Every number on this page is read at build time from <code>results/</code>, the repo&apos;s Markdown and its Python sources.
        Charts marked recorded replay the study&apos;s trials; the sink simulator and the bounds calculator compute live in your browser.
      </footer>
    </div>
  );
}

function Hero() {
  const supported = Object.values(summary.verdicts).filter((v) => v.verdict === 'Supported').length;
  const nHyp = Object.keys(summary.verdicts).length;
  return (
    <header className="card hero window" id="top" data-title="airflow-reliability-lab — README">
      <div className="head">
        <p className="eyebrow">Pre-registered fault-injection study · Airflow {env.airflow_version}</p>
        <span className="pill good">● {supported} of {nHyp} hypotheses supported</span>
      </div>
      <h1>A <em>green</em> Airflow run is not a correct one.</h1>
      <p className="sub">
        Airflow runs every task at least once. A task can be killed halfway through a write, retried, triggered twice or re-run after late data, and the DAG still reports success.
        Naive pipelines duplicated data in all {head.naiveWriteFailed} of {head.naiveWriteTrials} write-fault trials while reporting success. Hardened pipelines failed {head.hardenedFaultFailed} of {head.hardenedFaultTrials} fault trials.
        The sink is the thing to measure, not the DAG status.
      </p>
      <div className="actions">
        <a className="btn primary" href="#sim">Run the sink simulator</a>
        <a className="btn ghost" href={blob('HYPOTHESES.md')}>Read the pre-registration</a>
        <span className="hint">Every trial record is in <code>results/trials.jsonl</code></span>
      </div>
      <div className="tiles">
        <div className="tile"><div className="num">{fmt(trials.length)}</div><div className="cap">Recorded trials on a real Airflow {env.airflow_version} deployment</div></div>
        <div className="tile"><div className="num">{fmt(head.naiveF12Duplicates)}</div><div className="cap">Duplicate records from naive pipelines in the F1 and F2 trials alone</div></div>
        <div className="tile"><div className="num">{head.totalLosses}</div><div className="cap">Lost records across all trials: the failure mode is duplication</div></div>
        <div className="tile"><div className="num">{pooledBound}</div><div className="cap">95% upper bound on the hardened failure rate, pooled F1 (n={pooledHardened.length})</div></div>
      </div>
    </header>
  );
}

function FaultMatrix() {
  const [sel, setSel] = useState({ fault: faults[0].id, arm: 'naive' });
  const narrow = useNarrow();
  const W = narrow ? 380 : 640, rowH = narrow ? 76 : 52, top = 30, lab = narrow ? 0 : 220, colW = (W - lab) / 2, H = top + faults.length * rowH + 4;
  const cellY = narrow ? 24 : 4, cellH = narrow ? 48 : rowH - 8, midY = cellY + cellH / 2;
  const choose = (fault, arm) => setSel({ fault, arm });
  const recs = cell(trials, sel);
  const rows = pipelines.map((p) => [p, cell(recs, { pipeline: p })]).filter(([, r]) => r.length);
  return (
    <section id="matrix" className="card window reveal" data-title={`fault_matrix — ${faults.length} faults × ${ARMS.length} arms`}>
      <div>
        <p className="eyebrow">Recorded results</p>
        <h2>Every write fault broke the naive arm. None broke the hardened arm.</h2>
        <p className="sub">
          Failed trials over trials, per fault and arm, pooled across pipelines. A trial fails when the independent checker finds a duplicate, a lost record or a content mismatch in the sink.
          Every zero carries its one-sided Clopper-Pearson 95% upper bound: zero failures in a few trials never proves a rate of zero. Select a cell for its per-pipeline breakdown.
        </p>
      </div>
      <svg className="chart" viewBox={`0 0 ${W} ${H}`} role="group" aria-label="Fault matrix: failed trials over trials per fault and arm, with 95% upper bounds" style={{ marginTop: 16 }}>
        {ARMS.map((arm, j) => <text key={arm} x={lab + j * colW + colW / 2} y={18} textAnchor="middle" style={{ fontWeight: 700, fill: 'var(--text)' }}>{arm}</text>)}
        {faults.map((f, i) => (
          <g key={f.id} transform={`translate(0 ${top + i * rowH})`}>
            <text x={0} y={narrow ? 14 : rowH / 2 + 4} style={{ fill: 'var(--text)', fontSize: 13 }}><tspan style={{ fontWeight: 700 }}>{f.code}</tspan> {f.label}</text>
            {ARMS.map((arm, j) => {
              const r = cell(trials, { fault: f.id, arm }), k = failures(r), n = r.length;
              const on = sel.fault === f.id && sel.arm === arm;
              return (
                <g key={arm} role="button" tabIndex={0} aria-pressed={on} aria-label={`${f.code} ${arm}: ${k} of ${n} failed`} style={{ cursor: 'pointer' }}
                  onClick={() => choose(f.id, arm)} onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); choose(f.id, arm); } }}>
                  <rect x={lab + j * colW + 4} y={cellY} width={colW - 8} height={cellH} rx={8} fill={k ? 'var(--bad-bg)' : 'var(--good-bg)'} stroke={on ? 'var(--text)' : 'var(--border)'} strokeWidth={on ? 2 : 1} />
                  <text x={lab + j * colW + (narrow ? 14 : 18)} y={midY + 5} style={{ fill: k ? 'var(--bad)' : 'var(--good)', fontSize: 15, fontWeight: 700 }}>{k}/{n}</text>
                  <text x={lab + (j + 1) * colW - (narrow ? 12 : 16)} y={midY + 4} textAnchor="end">{k ? 'failed' : `${narrow ? '≤' : 'bound ≤'} ${pct(clopperPearsonUpper(k, n))}`}</text>
                </g>
              );
            })}
          </g>
        ))}
      </svg>
      <h3 style={{ marginTop: 16 }}>{faultOf(sel.fault).code} {faultOf(sel.fault).label}, {sel.arm} arm, by pipeline <span className="pill info" style={{ marginLeft: 6 }}>recorded</span></h3>
      <div style={{ overflowX: 'auto', marginTop: 8 }}>
        <table>
          <thead><tr><th>Pipeline</th><th>Failed / trials</th><th>Duplicates</th><th>Losses</th><th>Mismatches</th><th>Median extra executions</th><th>Median recovery (s)</th></tr></thead>
          <tbody>
            {rows.map(([p, r]) => {
              const rec = medianRecovery(r);
              return (
                <tr key={p}><td className="mono">{p}</td><td className={failures(r) ? 'no' : 'ok'}>{failures(r)}/{r.length}</td><td>{fmt(sum(r, 'duplicates'))}</td><td>{sum(r, 'losses')}</td><td>{sum(r, 'content_mismatches')}</td>
                  <td>{median(r.map((t) => t.extra_task_executions))}</td><td>{rec ?? 'n/a'}</td></tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function SinkBar({ name, state, cap, batch }) {
  const runs = [], seen = new Set();
  const push = (s) => { const last = runs[runs.length - 1]; if (last && last.s === s) last.n += 1; else runs.push({ s, n: 1 }); };
  for (const k of state.committed) { push(seen.has(k) ? 'dup' : 'ok'); seen.add(k); }
  for (let i = 0; i < state.pending.length; i++) push('pending');
  const W = 600, H = 30, x = (v) => (v / cap) * W;
  const dups = runs.filter((r) => r.s === 'dup').reduce((a, r) => a + r.n, 0);
  let at = 0;
  return (
    <div style={{ marginTop: 10 }}>
      <div className="hint" style={{ display: 'flex', justifyContent: 'space-between', gap: 8 }}>
        <span className="mono">{name}</span>
        <span>{state.committed.length} committed{dups ? `, ${dups} duplicate` : ''}{state.pending.length ? `, ${state.pending.length} uncommitted` : ''}</span>
      </div>
      <svg className="chart" viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`${name}: ${state.committed.length} committed rows, ${dups} duplicates, ${state.pending.length} uncommitted`}>
        <rect x={0} y={0} width={W} height={H} rx={6} fill="var(--primary-pale)" />
        {runs.map((r, i) => {
          const x0 = x(at); at += r.n;
          return <rect key={i} x={x0} y={r.s === 'pending' ? 3 : 0} width={Math.max(1, x(r.n))} height={r.s === 'pending' ? H - 6 : H} rx={4}
            fill={r.s === 'ok' ? 'var(--good)' : r.s === 'dup' ? 'var(--bad)' : 'var(--primary)'} opacity={r.s === 'pending' ? 0.35 : 0.85}
            stroke={r.s === 'pending' ? 'var(--primary)' : 'none'} strokeDasharray="4 3" />;
        })}
        {batch > 1 && Array.from({ length: Math.floor(at / batch) }, (_, i) => <line key={i} x1={x((i + 1) * batch)} x2={x((i + 1) * batch)} y1={0} y2={H} stroke="var(--surface-strong)" strokeWidth="1" />)}
      </svg>
    </div>
  );
}

function SinkSimulator() {
  const [arm, setArm] = useState('naive');
  const [fault, setFault] = useState('kill');
  const [pipeline, setPipeline] = useState('ingestion');
  const [rows, setRows] = useState(C.rows);
  const [batch, setBatch] = useState(C.batch);
  const [late, setLate] = useState(C.late);
  const [step, setStep] = useState(Infinity);
  const [playing, setPlaying] = useState(false);
  const model = useMemo(() => simulate({ arm, fault, pipeline, rows, batch, late }), [arm, fault, pipeline, rows, batch, late]);
  const last = model.steps.length - 1;
  const cur = Math.min(step, last);
  useEffect(() => { setStep(Infinity); setPlaying(false); }, [model]);
  useEffect(() => {
    if (!playing) return undefined;
    if (cur >= last) { setPlaying(false); return undefined; }
    const t = setTimeout(() => setStep(cur + 1), 420);
    return () => clearTimeout(t);
  }, [playing, cur, last]);
  const snap = model.steps[cur];
  const cap = Math.max(1, ...model.steps.flatMap((s) => Object.values(s.tables).map((t) => t.committed.length + t.pending.length)));
  const atStudy = rows === C.rows && batch === C.batch && late === C.late;
  const faultId = SIM_FAULTS.find((f) => f[0] === fault)[2];
  const recorded = SIM_PIPELINES.find((p) => p[0] === pipeline)[2].map((p) => {
    const r = cell(trials, faultId ? { fault: faultId, pipeline: p, arm } : { phase: 'control', pipeline: p, arm });
    return { p, n: r.length, dup: sum(r, 'duplicates'), loss: sum(r, 'losses'), mis: sum(r, 'content_mismatches') };
  });
  const play = () => { setStep(0); setPlaying(true); };
  return (
    <section id="sim" className="card window reveal" data-title="write_rows.py — live">
      <div className="head">
        <div>
          <p className="eyebrow">Live model, computed in your browser</p>
          <h2>Watch a retry turn a half-finished write into duplicates.</h2>
          <p className="sub">
            A model of <a href={blob('dags_src/airrel_common.py')}>write_rows and write_single_row</a>. The naive arm commits every {C.batch} rows and appends; the hardened arm does DELETE + INSERT on the natural key inside one transaction.
            The fault fires once, then Airflow retries the task. Batch size, partition size and late documents default to the constants in the Python sources.
          </p>
        </div>
        <span className="pill info">model</span>
      </div>
      <div className="controls" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))' }}>
        <div className="field"><span className="hint" style={{ display: 'block', fontWeight: 600, marginBottom: 6, color: 'var(--text)' }}>Arm</span>
          <div className="seg" role="group" aria-label="Arm">
            {ARMS.map((a) => <button key={a} aria-pressed={arm === a} onClick={() => setArm(a)}>{a}</button>)}
          </div>
        </div>
        <Select label="Fault" value={fault} onChange={setFault} options={SIM_FAULTS} />
        <Select label="Pipeline" value={pipeline} onChange={setPipeline} options={SIM_PIPELINES} />
        <Range label="Rows per partition" value={rows} min={10} max={400} step={10} onChange={setRows} show={rows} />
        <Range label="BATCH (rows per commit)" value={batch} min={1} max={100} step={1} onChange={setBatch} show={batch} />
        <Range label="Late documents" value={late} min={0} max={100} step={5} onChange={setLate} show={late} />
      </div>
      <div className="grid2">
        <div>
          <div className="actions" style={{ marginTop: 0, marginBottom: 14 }}>
            <button className="btn primary" onClick={play} disabled={playing}>{playing ? 'Playing…' : 'Play the run'}</button>
            <span className="hint">Step {cur + 1} of {model.steps.length}</span>
          </div>
          <Range label="Step through the run" value={cur} min={0} max={last} step={1} onChange={(v) => { setPlaying(false); setStep(v); }} show={`${cur + 1}/${model.steps.length}`} />
          <p className={`pill ${snap.kind === 'fault' ? 'bad' : snap.kind === 'retry' ? 'warn' : 'info'}`} style={{ whiteSpace: 'normal', marginTop: 4 }} aria-live="polite">{snap.note}</p>
          {Object.entries(snap.tables).map(([name, st]) => <SinkBar key={name} name={name === 'run_table' ? 'eval / registry / drift row' : name} state={st} cap={cap} batch={name === 'run_table' ? 1 : batch} />)}
          <div className="legend"><span><i style={{ background: 'var(--good)' }} />first copy of a key</span><span><i style={{ background: 'var(--bad)' }} />duplicate</span><span><i style={{ background: 'var(--primary)', opacity: 0.4 }} />uncommitted</span></div>
        </div>
        <div>
          <h3>Model result for the whole run</h3>
          <div className="tiles" style={{ gridTemplateColumns: '1fr 1fr', marginTop: 10 }}>
            <div className="tile"><div className="num" style={{ color: model.duplicates ? 'var(--bad)' : 'var(--good)' }}>{model.duplicates}</div><div className="cap">duplicate rows</div></div>
            <div className="tile"><div className="num">{model.losses}</div><div className="cap">lost rows</div></div>
            <div className="tile"><div className="num">{model.tables.reduce((a, t) => a + t.rows, 0)}</div><div className="cap">rows in the sink</div></div>
            <div className="tile"><div className="num">{model.retries}</div><div className="cap">extra task executions</div></div>
          </div>
          <h3 style={{ marginTop: 18 }}>Same cell in the study <span className="pill info" style={{ marginLeft: 6 }}>recorded</span></h3>
          {!atStudy ? <p className="hint">Recorded values appear at the study settings ({C.rows} rows, BATCH {C.batch}, {C.late} late documents).</p>
            : recorded.every((r) => !r.n) ? <p className="hint">No recorded cell: this fault did not run on this pipeline (DECISIONS.md D6). The model result above is unverified.</p>
              : (
                <table style={{ marginTop: 6 }}>
                  <thead><tr><th>Pipeline</th><th>Recorded duplicates</th><th>Per trial</th><th>Model</th></tr></thead>
                  <tbody>
                    {recorded.filter((r) => r.n).map((r) => (
                      <tr key={r.p}><td className="mono">{r.p}</td><td>{fmt(r.dup)} over {r.n} trials</td><td>{r.dup / r.n}</td>
                        <td className={r.dup / r.n === model.duplicates ? 'ok' : 'no'}>{r.dup / r.n === model.duplicates ? 'matches' : 'differs'}</td></tr>
                    ))}
                  </tbody>
                </table>
              )}
          <p className="hint" style={{ marginTop: 10 }}>The build fails unless the model reproduces the recorded duplicates, losses and extra executions of every trial it covers (<code>src/stats.check.mjs</code>). Platform faults F4 and F5 are not modelled.</p>
        </div>
      </div>
    </section>
  );
}

function GreenButWrong() {
  const failed = trials.filter((t) => !t.passed);
  const gbw = failed.filter(greenRun);
  const phases = [...new Set(trials.map((t) => t.phase))];
  const [phase, setPhase] = useState('all');
  const [pipeline, setPipeline] = useState('all');
  const [arm, setArm] = useState('all');
  const [onlyFailed, setOnlyFailed] = useState(true);
  const [hover, setHover] = useState(null);
  const arms = [...new Set(trials.map((t) => t.arm))];
  const shown = trials.filter((t) => (phase === 'all' || t.phase === phase) && (pipeline === 'all' || t.pipeline === pipeline) && (arm === 'all' || t.arm === arm) && (!onlyFailed || !t.passed));
  const narrow = useNarrow();
  const W = narrow ? 380 : 960, lab = narrow ? 0 : 96, pitch = narrow ? 9 : 8.4, line = 16;
  const perRow = Math.floor((W - lab) / pitch);
  let y = 0;
  const layout = phases.map((ph) => {
    const rs = cell(trials, { phase: ph }), y0 = y + (narrow ? 16 : 0);
    y = y0 + Math.ceil(rs.length / perRow) * line + 6;
    return { ph, rs, y0 };
  });
  const H = y;
  const color = (t) => (t.passed ? 'var(--good)' : greenRun(t) ? 'var(--bad)' : 'var(--warn)');
  const all = (o) => [['all', 'All'], ...o.map((v) => [v, v])];
  return (
    <section id="green" className="card window reveal" data-title="dag_runs vs sink.db">
      <p className="eyebrow">Recorded replay · every trial</p>
      <h2>{gbw.length} of {failed.length} failed trials ended in DAG state <em>success</em>.</h2>
      <p className="sub">
        Each square is one trial: the DAG run state Airflow recorded, set against the sink check. Green squares passed the checker. Red squares failed it while every DAG run in the trial ended in success: green-but-wrong.
        No failed trial ever showed a failed DAG run, so a dashboard of DAG states would have shown nothing wrong.
      </p>
      <svg className="chart" viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`Every trial by phase, coloured by sink check: ${gbw.length} green-but-wrong of ${failed.length} failed`} style={{ marginTop: 16 }} onMouseLeave={() => setHover(null)}>
        {layout.map(({ ph, rs, y0 }) => (
          <g key={ph} transform={`translate(0 ${y0})`}>
            <text x={0} y={narrow ? -4 : 13} style={{ fill: 'var(--text)' }}>{ph} <tspan style={{ fill: 'var(--muted)' }}>({rs.length})</tspan></text>
            {rs.map((t, j) => <rect key={t.trial_id} x={lab + (j % perRow) * pitch} y={3 + Math.floor(j / perRow) * line} width={pitch - 2} height={12} rx={2} fill={color(t)} opacity={hover && hover !== t ? 0.45 : 0.9} onMouseEnter={() => setHover(t)} />)}
          </g>
        ))}
      </svg>
      <div className="tip" aria-live="polite">{hover ? <><span className="mono">{hover.trial_id}</span>: run states {hover.run_states.join(', ')}; sink {hover.passed ? 'passed' : `failed (${hover.duplicates} duplicates, ${hover.losses} losses, ${hover.content_mismatches} mismatches)`}</> : 'Hover a square to inspect one trial.'}</div>
      <div className="legend"><span><i style={{ background: 'var(--good)' }} />sink passed</span><span><i style={{ background: 'var(--bad)' }} />sink failed, DAG success</span><span><i style={{ background: 'var(--warn)' }} />sink failed, DAG not success</span></div>
      <h3 style={{ marginTop: 22 }}>Trial explorer <span className="pill info" style={{ marginLeft: 6 }}>recorded</span></h3>
      <div className="controls" style={{ marginTop: 10 }}>
        <Select label="Phase" value={phase} onChange={setPhase} options={all(phases)} />
        <Select label="Pipeline" value={pipeline} onChange={setPipeline} options={all(pipelines)} />
        <Select label="Arm" value={arm} onChange={setArm} options={all(arms)} />
        <div className="field"><label htmlFor="only-failed">Failed trials only</label>
          <input id="only-failed" type="checkbox" checked={onlyFailed} onChange={(e) => setOnlyFailed(e.target.checked)} style={{ width: 18, height: 18, accentColor: 'var(--primary)' }} /></div>
      </div>
      <p className="hint">{shown.length} trials, {shown.filter((t) => t.green_but_wrong).length} flagged green-but-wrong in the record.</p>
      <div style={{ overflow: 'auto', maxHeight: 340 }}>
        <table>
          <thead><tr><th>Trial</th><th>Run states</th><th>Sink check</th><th>Duplicates</th><th>Losses</th><th>Mismatches</th><th>Green but wrong</th></tr></thead>
          <tbody>
            {shown.map((t) => (
              <tr key={t.trial_id}><td className="mono">{t.trial_id}</td><td>{t.run_states.join(', ')}</td><td className={t.passed ? 'ok' : 'no'}>{t.passed ? 'passed' : 'failed'}</td>
                <td>{t.duplicates}</td><td>{t.losses}</td><td>{t.content_mismatches}</td><td>{t.green_but_wrong ? 'yes' : 'no'}</td></tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Bounds() {
  const [k, setK] = useState(0);
  const [n, setN] = useState(summaryBounds[0].n);
  const [target, setTarget] = useState(0.01);
  const kk = Math.max(0, Math.min(k, n));
  const bound = clopperPearsonUpper(kk, n);
  const need = trialsForBound(target, kk);
  const seeds = [...new Map(summaryBounds.map((s) => [s.n, s])).values()];
  const match = summaryBounds.find((s) => s.k === kk && s.n === n);
  const W = 460, H = 220, pad = { l: 40, r: 10, t: 10, b: 24 }, nMax = 120;
  const sx = (v) => pad.l + ((v - 1) / (nMax - 1)) * (W - pad.l - pad.r);
  const sy = (p) => pad.t + (1 - p) * (H - pad.t - pad.b);
  const curve = useMemo(() => Array.from({ length: nMax - kk }, (_, i) => [kk + 1 + i, clopperPearsonUpper(kk, kk + 1 + i)]).filter(([v]) => v <= nMax), [kk]);
  return (
    <section id="bounds" className="card window reveal" data-title="stats.py — clopper_pearson_upper">
      <p className="eyebrow">Live calculator · port of src/airrel/stats.py</p>
      <h2>How sure is <em>zero failures</em>? It depends on n.</h2>
      <p className="sub">
        The same bisection on the binomial CDF that <a href={blob('src/airrel/stats.py')}>stats.py</a> uses, running in your browser. Enter failures and trials for the one-sided 95% upper bound,
        or pick a target to see how many clean trials it would take. The seeds are the study&apos;s hardened cells; the build checks the port against every bound in <a href={blob('results/summary.json')}>summary.json</a>.
      </p>
      <div className="actions" role="group" aria-label="Study cells">
        {seeds.map((s) => <button key={s.n} className="btn ghost" aria-pressed={s.n === n && kk === s.k} onClick={() => { setK(s.k); setN(s.n); }}>{summaryBounds.filter((x) => x.n === s.n).map((x) => x.code).join(' + ')}: {s.k}/{s.n}</button>)}
      </div>
      <div className="grid2" style={{ marginTop: 16 }}>
        <div>
          <div className="controls" style={{ gridTemplateColumns: '1fr 1fr' }}>
            <div className="field"><label htmlFor="cp-k">Failures k</label><input id="cp-k" type="number" min={0} max={n} value={k} onChange={(e) => setK(Math.max(0, Math.floor(+e.target.value || 0)))} /></div>
            <div className="field"><label htmlFor="cp-n">Trials n</label><input id="cp-n" type="number" min={1} max={100000} value={n} onChange={(e) => setN(Math.max(1, Math.min(100000, Math.floor(+e.target.value || 1))))} /></div>
          </div>
          <Range label="Target upper bound" value={target} min={0.005} max={0.3} step={0.005} onChange={setTarget} show={pct(target, 1)} />
          <div className="tiles" style={{ gridTemplateColumns: '1fr 1fr', marginTop: 8 }}>
            <div className="tile"><div className="num">{round4(bound)}</div><div className="cap">95% upper bound for {kk} of {n}{match ? `; summary.json: ${match.bound}` : ''}</div></div>
            <div className="tile"><div className="num">{need ? fmt(need) : 'n/a'}</div><div className="cap">trials with {kk} failures to push the bound under {pct(target, 1)}</div></div>
          </div>
        </div>
        <div>
          <h3>Upper bound by number of trials, k = {kk}</h3>
          <svg className="chart" viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`Clopper-Pearson 95% upper bound against trials for ${kk} failures`}>
            {[0, 0.25, 0.5, 0.75, 1].map((t) => <g key={t}><line className="axis" x1={pad.l} x2={W - pad.r} y1={sy(t)} y2={sy(t)} /><text x={pad.l - 6} y={sy(t) + 4} textAnchor="end">{t}</text></g>)}
            <line x1={pad.l} x2={W - pad.r} y1={sy(target)} y2={sy(target)} stroke="var(--warn)" strokeDasharray="5 4" />
            <polyline fill="none" stroke="var(--primary)" strokeWidth="2.5" points={curve.map(([v, p]) => `${sx(v)},${sy(p)}`).join(' ')} />
            {kk === 0 && seeds.map((s) => <g key={s.n}><circle cx={sx(s.n)} cy={sy(s.bound)} r="5" fill="var(--bad)" stroke="var(--surface-strong)" strokeWidth="2" /><text x={sx(s.n) + 8} y={sy(s.bound) - 6}>n={s.n}</text></g>)}
            {n <= nMax && <circle cx={sx(n)} cy={sy(bound)} r="5" fill="var(--primary)" stroke="var(--surface-strong)" strokeWidth="2" />}
            <text x={pad.l} y={H - 4}>n = 1</text><text x={W - pad.r} y={H - 4} textAnchor="end">n = {nMax}</text>
          </svg>
          <p className="hint">Red dots: the study&apos;s zero-failure cells as summary.json reports them. Dashed line: your target.</p>
        </div>
      </div>
    </section>
  );
}

function Recovery() {
  const groups = [];
  for (const f of faults) for (const arm of ARMS) {
    const r = cell(trials, { fault: f.id, arm }).filter((t) => t.recovery_seconds);
    if (r.length) groups.push({ key: `${f.code} ${arm}`, f, arm, r });
  }
  const [hover, setHover] = useState(null);
  const xMax = Math.ceil(Math.max(...groups.flatMap((g) => g.r.map((t) => t.recovery_seconds))) / 5) * 5;
  const narrow = useNarrow();
  const W = narrow ? 380 : 960, lab = narrow ? 92 : 120, rowH = 26, H = groups.length * rowH + 30, sx = (v) => lab + (v / xMax) * (W - lab - 12);
  const hf = trials.filter((t) => t.arm === 'hardened' && ['kill_mid_write', 'partial_write_error'].includes(t.fault));
  const oneExtra = hf.filter((t) => t.extra_task_executions === 1).length;
  const f12 = groups.filter((g) => ['F1', 'F2'].includes(g.f.code) && g.arm === 'hardened').flatMap((g) => g.r);
  const f4 = groups.filter((g) => g.f.code === 'F4').flatMap((g) => g.r);
  return (
    <section id="recovery" className="card window reveal" data-title="recovery_seconds — recorded">
      <p className="eyebrow">Recorded replay</p>
      <h2>Recovery costs one retry and a few seconds.</h2>
      <p className="sub">
        Hardened F1 and F2 trials recovered with exactly one extra task execution in {oneExtra} of {hf.length} trials, median {medianRecovery(f12)} s. Scheduler restarts (F4) took a median {medianRecovery(f4)} s.
        Writes were paced artificially (DECISIONS.md D15), so these times describe the harness, not production latency.
      </p>
      <svg className="chart" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Strip plot of recovery seconds per fault and arm" style={{ marginTop: 16 }} onMouseLeave={() => setHover(null)}>
        {Array.from({ length: xMax / 5 + 1 }, (_, i) => i * 5).map((t) => <g key={t}><line className="axis" x1={sx(t)} x2={sx(t)} y1={0} y2={H - 22} /><text x={sx(t)} y={H - 6} textAnchor="middle">{t}s</text></g>)}
        {groups.map((g, i) => (
          <g key={g.key} transform={`translate(0 ${i * rowH + 14})`}>
            <text x={0} y={4} style={{ fill: 'var(--text)' }}>{g.key}</text>
            {g.r.map((t, j) => <circle key={t.trial_id} cx={sx(t.recovery_seconds)} cy={((j % 5) - 2) * 2.4} r={hover === t ? 6 : 3.6} fill={g.arm === 'naive' ? 'var(--bad)' : 'var(--good)'} opacity={0.75} onMouseEnter={() => setHover(t)} />)}
          </g>
        ))}
      </svg>
      <div className="tip" aria-live="polite">{hover ? <><span className="mono">{hover.trial_id}</span>: {hover.recovery_seconds} s, {hover.extra_task_executions} extra task execution{hover.extra_task_executions === 1 ? '' : 's'}</> : 'Hover a dot for its trial id.'}</div>
      <div className="legend"><span><i style={{ background: 'var(--bad)' }} />naive</span><span><i style={{ background: 'var(--good)' }} />hardened</span></div>
    </section>
  );
}

function Pinning() {
  const runs = trials.filter((t) => t.fault === 'code_deploy_mid_run');
  const versions = [...new Set(runs.flatMap((t) => t.marker_versions))].sort();
  const git = runs.filter((t) => t.phase === 'F5');
  const by = (v) => git.filter((t) => t.marker_versions.length === 1 && t.marker_versions[0] === v).length;
  const narrow = useNarrow();
  const W = narrow ? 380 : 960, lab = narrow ? 0 : 210, rowH = narrow ? 46 : 30, H = runs.length * rowH + 8, by0 = narrow ? 18 : 2;
  const vColor = (v) => (v === versions[0] ? 'var(--primary)' : 'var(--warn)');
  return (
    <section id="pinning" className="card window reveal" data-title="dag_bundle — git vs local">
      <p className="eyebrow">Recorded replay · F5 code deploy mid-run</p>
      <h2>A Git bundle pins each run to <em>one</em> code version.</h2>
      <p className="sub">
        Each bar is one ingestion run during a mid-run deploy; its segments are the distinct CODE_VERSION values the run&apos;s task markers recorded.
        Under the Git bundle, {by(versions[0])} runs held {versions[0]} across the deploy and {by(versions[1])} were serialized after it and held {versions[1]}; none mixed.
        The local-bundle control mixed both inside one run, but it is a single trial: it shows the mechanism, not a rate.
      </p>
      <svg className="chart" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Code versions recorded per F5 run, Git bundle versus local bundle" style={{ marginTop: 16 }}>
        {runs.map((t, i) => {
          const w = (W - lab - (narrow ? 60 : 90)) / t.marker_versions.length;
          return (
            <g key={t.trial_id} transform={`translate(0 ${i * rowH + 4})`}>
              <text x={0} y={narrow ? 12 : 16} style={{ fill: 'var(--text)' }} className="mono">{t.trial_id}</text>
              {t.marker_versions.map((v, j) => <g key={v}><rect x={lab + j * w} y={by0} width={w - 2} height={20} rx={5} fill={vColor(v)} opacity={0.8} /><text x={lab + j * w + 10} y={by0 + 14} style={{ fill: '#fff', fontWeight: 700 }}>{v}</text></g>)}
              <text x={W - 4} y={by0 + 14} textAnchor="end" style={{ fill: t.version_mix ? 'var(--bad)' : 'var(--good)', fontWeight: 700 }}>{t.version_mix ? 'mixed' : 'pinned'}</text>
            </g>
          );
        })}
      </svg>
      <p className="hint" style={{ marginTop: 8 }}><span className="pill warn">n={runs.length - git.length}, exploratory</span> The control is excluded from the H3 verdict by pre-registration. Only {by(versions[0])} of {git.length} Git-bundle runs exercised the strict mid-run timing.</p>
    </section>
  );
}

function Hypotheses() {
  return (
    <section id="hypotheses" className="card window reveal" data-title="HYPOTHESES.md — verdicts">
      <p className="eyebrow">Pre-registered, then tested</p>
      <h2>{docs.hypotheses.length} hypotheses, written down before any experiment code.</h2>
      <p className="sub">
        SPEC.md and HYPOTHESES.md were committed in <a href={`${REPO}/commit/${docs.report.preregCommit}`} className="mono">{docs.report.preregCommit}</a> (tag proposal-v1); research, methods and the first experiment code came later in{' '}
        {docs.report.nextCommits.map((c, i) => <span key={c}>{i ? ' and ' : ''}<a href={`${REPO}/commit/${c}`} className="mono">{c}</a></span>)}. Thresholds are quoted from HYPOTHESES.md; verdicts and evidence are computed by report.py into summary.json.
      </p>
      <div className="grid2" style={{ marginTop: 16 }}>
        {docs.hypotheses.map((h) => {
          const v = summary.verdicts[h.id];
          return (
            <div className="tile" key={h.id}>
              <div className="head" style={{ marginBottom: 6 }}><h3>{h.id} ({h.rq}): {h.title}</h3><span className={`pill ${v.verdict === 'Supported' ? 'good' : 'warn'}`}>{v.verdict}</span></div>
              <p className="hint" style={{ margin: '0 0 8px', fontSize: 13, whiteSpace: 'pre-line' }}>{h.text}</p>
              <p style={{ margin: 0, fontSize: 13 }}><b>Evidence:</b> {v.evidence}.</p>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function Limitations() {
  const r = docs.report;
  const inDist = drift.partitions.filter((p) => !p.shifted), shifted = drift.partitions.filter((p) => p.shifted);
  return (
    <section id="limits" className="card window reveal" data-title="REPORT.md — threats to validity">
      <p className="eyebrow">Limitations and threats to validity</p>
      <h2>What this study cannot tell you.</h2>
      <p className="sub">
        Rendered here from REPORT.md at build time. The main gaps a reviewer would raise first:
        the hardened arm bundles its patterns (one transaction per task, upsert on deterministic keys, fixed-partition reads) with no ablation, so the study cannot say which one is necessary; everything ran on SQLite, one machine, one executor and one Airflow version;
        the fault space is shaped around duplication, so no fault tested a loss path. The full review and the open gaps are in <a href={blob('REPORT.md')}>REPORT.md</a>.
      </p>
      <ul style={{ marginTop: 16 }}>
        {r.threats.map((t) => <li key={t} style={{ marginBottom: 6 }}>{t}</li>)}
      </ul>
      <p className="hint" style={{ marginTop: 12 }}>
        Drift threshold (SPEC.md amendment A1): {drift.threshold}, between the in-distribution statistics ({inDist.map((p) => p.drift_stat.toFixed(4)).join(', ')}) and the shifted partition ({shifted.map((p) => p.drift_stat.toFixed(4)).join(', ')}),
        regenerated by <code>make calibrate</code> into <a href={blob('results/drift_calibration.json')}>drift_calibration.json</a>. It was chosen after looking at these fixture values.
      </p>
    </section>
  );
}

function Reproduce() {
  const list = [
    ['SPEC.md', 'Spec', 'Design, fault matrix, metrics'],
    ['HYPOTHESES.md', 'Hypotheses', 'Thresholds fixed in advance'],
    ['METHODS.md', 'Methods', 'Formulas, controls, threats'],
    ['RESULTS.md', 'Results', 'Generated from trials.jsonl'],
    ['DECISIONS.md', 'Decisions', 'Every deviation, dated'],
    ['REPORT.md', 'Report', 'Verdict, threats, review, open gaps'],
  ];
  return (
    <section id="reproduce" className="card window reveal" data-title="Terminal — make reproduce">
      <p className="eyebrow">Reproduce it</p>
      <h2>One command reruns the whole study locally.</h2>
      <ol className="steps" style={{ marginTop: 16 }}>
        <li><span className="n">1</span><div><b>Unit tests and the report check</b><div className="hint"><code>make setup && make test</code></div></div></li>
        <li><span className="n">2</span><div><b>Full study</b><div className="hint"><code>make reproduce</code> stands up a local Airflow {env.airflow_version} deployment, runs all {fmt(env.trial_count)} trials and regenerates RESULTS.md: about {docs.readme.runtimeMinutes} minutes on {docs.readme.cores} cores, no cost.</div></div></li>
        <li><span className="n">3</span><div><b>Recorded environment</b><div className="hint">Python {env.python_version}, study window {env.started_utc.slice(0, 16).replace('T', ' ')} to {env.finished_utc.slice(11, 16)} UTC, repo head <a className="mono" href={`${REPO}/commit/${env.study_repo_head}`}>{env.study_repo_head.slice(0, 7)}</a>.</div></div></li>
      </ol>
      <div className="docs" style={{ marginTop: 20 }}>
        {list.map(([p, t, d]) => <a key={p} href={blob(p)}><b>{t}</b><small>{d}</small></a>)}
      </div>
    </section>
  );
}
