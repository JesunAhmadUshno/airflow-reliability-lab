# Methods

Study: AIR-REL (SPEC.md). Hypotheses: HYPOTHESES.md. This file records how the study is
run and analysed, so a reviewer can repeat it and can see the threats to validity.

## Design

A two-arm controlled comparison. Both arms run the same five pipeline DAGs, on the same
corpus, on the same Airflow deployment, with the same retry settings. The only difference
is the write and read pattern (SPEC.md section 5): naive versus hardened. Faults are
injected per pre-registered matrix (SPEC.md section 6). Every trial ends with an
independent invariant check on the trial sink.

## Data

Synthetic deterministic corpus (SPEC.md section 3): 3 base partitions of 200 documents,
one shifted partition of 200 documents, 40 late documents per F6 trial. The generator is
seeded; the same seed always produces the same corpus. No external data is downloaded.

## Controls

- Clean controls: every pipeline and arm runs with no fault (n = 3) and must pass all
  invariants before its fault results count. A pipeline whose clean control fails is a
  build defect, not a result.
- Monitoring controls: monitoring also runs on the shifted partition (n = 3 per arm);
  the pre-registered expectation is drift flag true there and false on the base partition.
- Exploratory control for RQ3: one ingestion run under a local unversioned bundle during
  a mid-run deploy. Reported separately; excluded from the H3 verdict.

## Formulas

- duplicates = total rows in sink table minus distinct natural keys.
- losses = expected natural keys absent from the sink.
- duplicate rate = duplicates / distinct expected keys. Loss rate = losses / expected keys.
- version-mix rate = runs with more than one distinct CODE_VERSION / F5 runs.
- recovery_seconds and extra_task_executions as defined in SPEC.md section 7.
- Uncertainty: one-sided Clopper-Pearson 95 percent upper bound for a failure probability
  given k failures in n trials, computed with the beta distribution (scipy-free
  implementation in src/airrel/stats.py, checked against known values in tests).

## Repetitions and analysis

- Trial counts per cell are fixed in SPEC.md section 6, with one pre-registered adaptation
  rule for F1 and F2 counts based on measured smoke-trial time.
- Verdicts are count-based against the thresholds in HYPOTHESES.md. There is no
  significance-test fishing: each hypothesis names its cells and its threshold in advance.
- Pooling: for RQ2, hardened-arm trials are also pooled per fault across pipelines
  (n = 50 for F1 and F2), because the write pattern is a single shared implementation.
  Per-cell results are always printed beside pooled results.
- Multiple comparisons: hypotheses H1 to H4 concern different mechanisms and are judged
  separately; no family-wise correction is claimed, and no pooled p-value is reported.

## Independence of the checker

The invariant checker (src/airrel/checker.py) reads only the corpus files and the trial
sink. It reimplements the embedder from SPEC.md section 4 and never imports DAG code or
the write functions under test. A hand-planted duplicate and a hand-planted missing row
are used in unit tests to prove the checker flags both (research pack upgrade 2).

## Threats to validity

- Single machine, LocalExecutor, SQLite for both the metadata DB and the sinks. SQLite
  locking and rollback behaviour is part of what is measured; a Postgres sink could
  behave differently under F1 and F2. Stated as a scope limit, not hidden.
- Trial counts bound, but do not eliminate, uncertainty about rare failures. Every zero
  cell is printed with its 95 percent upper bound.
- The corpus is synthetic and small (200 documents per partition). Duplicate and loss
  mechanisms do not depend on corpus realism, but recovery times do depend on size.
- The embedder is deterministic by design. Non-deterministic ML steps (GPU training,
  unfixed seeds) would need the tolerance treatment named in the research pack; that is
  out of scope here.
- F4 (scheduler restart) trials run with no other runs active, because the fault is
  global to the deployment. This matches how the fault would be handled in a test
  environment, not a busy production scheduler.
- The same author wrote both arms and the checker. The checker's independence is
  structural (separate implementation, spec-only sharing) and is tested with planted
  faults, but it is not an external audit.
