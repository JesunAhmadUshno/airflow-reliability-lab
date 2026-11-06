# Decisions

Dated design decisions for the AIR-REL study. New entries are appended; entries are
never rewritten. Pre-registration: SPEC.md and HYPOTHESES.md (commit "Pre-register
AIR-REL study", 2026-11-06).

## 2026-11-06, before any experiment code

- D1. All DAGs load through one Git DAG bundle. Reason: the official Dag Bundles
  documentation states that the local bundle does not support versioning (tasks always
  run the latest code), while the Git bundle records the commit per run. RQ3 cannot be
  tested without a versioned bundle (pack row E6, verified in RESEARCH.md). The harness
  builds the bundle repository at run time from the committed templates in dags_src/,
  so the study repo stays a single normal repository.
- D2. A deterministic hash embedder stands in for a learned embedding model. Reason:
  the research object is external-effect correctness. A deterministic embedder makes
  outcome equality exact (tolerance zero) and removes model download, GPU and seed
  variance from a study that is not about embedding quality.
- D3. SQLite is both the Airflow metadata DB and the sink store. Reason: local only,
  zero spend, and SQLite gives real transactions and rollback for the hardened arm.
  Scope cost: results are SQLite-specific until repeated on another sink (ROADMAP).
- D4. One DAG per pipeline type, with the arm passed as dag_run.conf. Reason: both arms
  then share the same task code; only the read and write functions differ, which keeps
  the comparison matched. The alternative (ten separate DAG files) doubles the code
  that can drift apart.
- D5. Trial counts: F1 and F2 at n = 10 per cell, pooled per fault for the primary RQ2
  analysis (pooled n = 50). Reason: the pack's rule-of-three analysis shows small n
  gives weak bounds; pooling is legitimate because the write pattern is one shared
  implementation per arm. Every zero cell still prints its own bound.
- D6. F4 is scoped to 3 of 5 pipelines (ingestion, embedding, retraining) and F6 to 2
  (ingestion, embedding). Reasons: a scheduler restart is a global event and each trial
  is slow and serial; late data enters the system at ingestion and propagates to
  embedding, while same-interval re-runs of downstream pipelines are already covered
  by F3. These reductions are recorded here, before the matrix runs, and are priced
  into the grade in AUDIT.md.
- D7. The monitoring pipeline uses one drift statistic (mean-vector distance) with the
  threshold fixed in SPEC.md, plus planted-shift controls. A comparison of drift
  detection methods (pack row E16) is deferred to the Deep pass.
- D8. The retraining artifact is a JSON file. The hardened arm writes it to a temporary
  path and renames it atomically; the checker hashes the artifact and compares it with
  the registry row. This keeps one real file side effect inside the measured scope.
- D9. The local control for RQ3 (one run under an unversioned local bundle) is
  exploratory, n = 1, and excluded from the H3 verdict, as pre-registered.
- D10. Amendments A1 and A2 (SPEC.md section 10) were recorded on 2026-11-06 before
  any trial ran: the drift threshold was recalibrated to 0.25 from measured fixture
  values (in-distribution about 0.10, shifted 0.4948), and the F1/F2 kill point for
  single-row tables was clarified. A unit test caught the threshold problem before
  the matrix did, which is the outcome the failure gate exists for.
- D11. The ingestion DAG has a third task (gate) between prepare and write. It is a
  no-op unless a gate file is configured (fault F5 only). Reason: the mid-run deploy
  must land before the write task process starts, so the write task's code version
  is sampled after the deploy. All ingestion trials use the same three-task shape,
  and per-pipeline clean-run task counts in the harness account for it.
- D12. Run logical dates are unique per trigger (measured platform behaviour):
  the Airflow 3.4 create-run API requires a logical_date (422 without one) and
  rejects a second run for the same DAG with the same logical_date (409). The
  harness therefore stamps each run with a unique logical time inside the
  partition day, and the data interval under study is the partition carried in
  dag_run.conf. F3's "same data interval" means the same partition and the same
  conf, which is the level at which duplicate effects occur.
