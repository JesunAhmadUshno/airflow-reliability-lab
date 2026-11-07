// A model of the two write paths in dags_src/airrel_common.py (write_rows and
// write_single_row) plus the Airflow retry. It is a model, not a replay: the
// recorded trial results are shown beside it on the page.

// BATCH comes from airrel_common.py, partition and late-data sizes from corpus.py.
export function parseConstants(commonPy, corpusPy) {
  const num = (src, name) => Number(src.match(new RegExp(`^${name} = (\\d+)`, 'm'))[1]);
  return { batch: num(commonPy, 'BATCH'), rows: num(corpusPy, 'DOCS_PER_PARTITION'), late: num(corpusPy, 'LATE_DOCS') };
}

export const SIM_FAULTS = [
  ['none', 'No fault', null],
  ['kill', 'F1: kill mid-write, halfway', 'kill_mid_write'],
  ['partial', 'F2: partial write, then error', 'partial_write_error'],
  ['duplicate', 'F3: duplicate trigger', 'duplicate_trigger'],
  ['late', 'F6: late data, then re-run', 'late_data_rerun'],
];
export const SIM_PIPELINES = [
  ['ingestion', 'Ingestion: documents', ['ingestion']],
  ['embedding', 'Embedding: documents to embeddings', ['embedding']],
  ['single', 'Single-row: eval, registry, drift', ['evaluation', 'retraining', 'monitoring']],
];

export function simulate({ arm, fault, pipeline, rows, batch, late }) {
  const tables = {};
  const steps = [];
  let retries = 0;
  const table = (name) => (tables[name] ??= { committed: [], pending: [] });
  const snap = (note, kind = 'write') => steps.push({
    note, kind,
    tables: Object.fromEntries(Object.entries(tables).map(([k, t]) => [k, { committed: t.committed.slice(), pending: t.pending.slice() }])),
  });
  const keys = (n) => Array.from({ length: n }, (_, i) => i);

  function fire(name) {
    const t = table(name), lost = t.pending.length;
    t.pending = [];
    const what = fault === 'kill' ? 'Worker killed (SIGKILL)' : 'Task raises an error';
    snap(lost ? `${what}: the open transaction rolls back ${lost} uncommitted row${lost === 1 ? '' : 's'}` : `${what}: committed rows stay in ${name}`, 'fault');
    return true;
  }

  function commitPending(t) {
    const incoming = new Set(t.pending); // DELETE then INSERT on the natural key, row by row
    t.committed = t.committed.filter((k) => !incoming.has(k)).concat([...incoming]);
    t.pending = [];
  }

  // write_rows: naive commits every BATCH rows; hardened holds one transaction.
  function writeRows(name, rowKeys, armed) {
    const t = table(name), half = Math.max(1, Math.floor(rowKeys.length / 2));
    if (arm === 'naive') {
      let done = 0;
      for (let i = 0; i < rowKeys.length; i += batch) {
        const b = rowKeys.slice(i, i + batch);
        t.committed.push(...b);
        done += b.length;
        snap(`INSERT ${b.length} rows into ${name}, COMMIT (${done}/${rowKeys.length})`);
        if (armed && done >= half) return fire(name);
      }
      return false;
    }
    for (let i = 0; i < rowKeys.length; i++) {
      t.pending.push(rowKeys[i]);
      const atFault = armed && i + 1 === half;
      if ((i + 1) % batch === 0 || i + 1 === rowKeys.length || atFault) snap(`DELETE + INSERT ${i + 1}/${rowKeys.length} rows into ${name}, transaction open`);
      if (atFault) return fire(name);
    }
    commitPending(t);
    snap(`COMMIT: ${rowKeys.length} rows land in ${name} at once`);
    return false;
  }

  // write_single_row: naive commits then faults; hardened faults before commit.
  function writeSingle(armed) {
    const name = 'run_table', t = table(name);
    if (arm === 'naive') {
      t.committed.push(0);
      snap('INSERT 1 row keyed by wall-clock time, COMMIT');
      return armed ? fire(name) : false;
    }
    t.pending.push(0);
    snap('DELETE + INSERT 1 row on its deterministic key, transaction open');
    if (armed) return fire(name);
    commitPending(t);
    snap('COMMIT');
    return false;
  }

  function task(label, write, armed) {
    snap(`${label} starts`, 'run');
    if (write(armed)) {
      retries += 1;
      snap(`Airflow retries ${label} (the fault fires once)`, 'retry');
      write(false);
    }
  }

  const armed = fault === 'kill' || fault === 'partial';
  const ingest = (n, a) => task('ingestion write', (x) => writeRows('documents', keys(n), x), a);
  const embedRun = (a) => task('embedding write', (x) => writeRows('embeddings', table('documents').committed.slice(), x), a);
  const twoRuns = fault === 'duplicate' || fault === 'late';

  if (pipeline === 'ingestion') {
    if (fault === 'late') { ingest(rows, false); ingest(rows + late, false); } else { ingest(rows, armed); if (twoRuns) ingest(rows, false); }
  } else if (pipeline === 'embedding') {
    if (fault === 'late') { ingest(rows, false); ingest(rows + late, false); embedRun(false); } else {
      table('documents').committed = keys(rows);
      snap(`Harness seeds documents with ${rows} clean rows`, 'run');
      embedRun(armed);
      if (twoRuns) embedRun(false);
    }
  } else {
    task('single-row write', writeSingle, armed);
    if (twoRuns) task('single-row write', writeSingle, false);
  }

  const expected = pipeline === 'single' ? 1 : rows + (fault === 'late' ? late : 0);
  const result = Object.entries(tables).map(([name, t]) => {
    const present = new Set(t.committed);
    const missing = keys(expected).filter((k) => !present.has(k)).length;
    return { name, rows: t.committed.length, distinct: present.size, duplicates: t.committed.length - present.size, losses: missing };
  });
  return {
    steps, retries, tables: result,
    duplicates: result.reduce((a, r) => a + r.duplicates, 0),
    losses: result.reduce((a, r) => a + r.losses, 0),
  };
}
