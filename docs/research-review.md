# Research review: AIR-REL

Reviewed 2026-11-07 against a doctoral-level empirical research rubric. The
study's own self-audit (AUDIT.md) grades it 6,900 of 10,000; this review lands
at about 7,000 after the fixes below. Every number quoted here can be traced
to results/trials.jsonl, results/summary.json or RESULTS.md.

## Grade by criterion

| Criterion | Score | Assessment |
|---|---|---|
| Research question | Strong | Four precise RQs (README, SPEC.md section 2): does a green DAG mean a correct sink, under named faults, for named pipeline types. The question is practical and measurable. |
| Hypotheses | Strong | H1 to H4 have numeric thresholds and falsification rules, committed in 15d4e12 before the first code commits (c99d9ae, 7e31706). Verdicts are computed by report.py, not judged by hand. |
| Method | Good | Two-arm controlled comparison on a real Airflow 3.4.0 deployment, an independent invariant checker tested with planted faults, clean controls per pipeline and arm. Weakness: the hardened arm bundles three patterns, so there is no ablation (see gaps). |
| Baselines | Good | The naive arm is the baseline, built from the anti-patterns the Airflow best-practices page warns against. Clean controls (36 trials, 0 failures) rule out harness breakage. There is no external baseline (for example a managed Airflow or another orchestrator). |
| Statistics and uncertainty | Good | Every zero carries a one-sided Clopper-Pearson 95% upper bound; per-cell results are printed beside pooled ones; no p-value fishing. Small cells give wide bounds: F3 0.181, F4 0.2831, F6 0.393, F5 0.6316. |
| Reproducibility | Strong | `make reproduce` reruns all 305 trials locally (about 25 minutes, no cost); RESULTS.md and summary.json regenerate byte-for-byte from trials.jsonl; CI runs the tests and the report on Python 3.11 and 3.12; environment.json records versions and the study window. |
| Threats to validity | Good | METHODS.md and AUDIT.md list the scope limits (SQLite, one machine, LocalExecutor, synthetic corpus, same author for both arms and the checker). |
| Related work | Adequate | RESEARCH.md checks each cited source (Airflow docs, Sculley et al. 2015, Breck et al. 2017, Rabanser et al. 2019, Carbone et al. 2015). There is no survey of exactly-once or idempotent-sink literature in stream processing beyond Carbone et al. |
| Limitations | Strong | Stated in README, METHODS.md and AUDIT.md with itemized deductions, and repeated on the dashboard. |

## Fixed in this change

1. H2 evidence now bounds every fault cell. H2 claims zero failures in every
   cell, but summary.json cited only the pooled F1 and F2 bounds. report.py now
   prints the hardened bound for F1 to F6; RESULTS.md and summary.json were
   regenerated from the unchanged trials.jsonl (tests/test_report.py).
2. Recovery cells that do not apply print `n/a`, not `None`.
3. Drift calibration is a generated record. The values behind SPEC amendment
   A1 (0.1039 and 0.1028 in-distribution, 0.4948 shifted, threshold 0.25) were
   only in SPEC.md prose. `make calibrate` (src/airrel/drift_calibration.py)
   now writes results/drift_calibration.json, and a test pins the values.
4. METHODS.md said the bound is computed with the beta distribution; it now
   describes the binomial-CDF bisection that stats.py actually runs.
5. The local test run is green: `make setup && make test` installs
   apache-airflow 3.4.0 from requirements-lock.txt and passes all tests,
   including the DAG parse test that fails with a system Python without
   Airflow. No test was skipped.
6. The live dashboard (site/) has its own known-answer check: the JavaScript
   Clopper-Pearson port must match every bound in summary.json and RESULTS.md,
   and the write-path model must reproduce the recorded duplicates, losses and
   extra executions of every trial it covers (242 trials).

## Remaining gaps and what each one needs

| Gap | Why it matters | What closing it needs |
|---|---|---|
| No ablation of the hardened patterns | The study cannot say whether one transaction per task, DELETE+INSERT on deterministic keys, or fixed-partition reads is necessary or sufficient for each fault. | A 2x2x2 factorial over F1, F2, F3, F6: six new arms at the current cell sizes (121 write-fault trials per arm) is about 730 more trials. The study ran 305 trials in about 25 minutes, so this is roughly an hour of local runtime under the existing harness. No GPU or paid service. |
| SQLite only, LocalExecutor, one machine, one Airflow version | Postgres isolation levels and a distributed executor could change F1 to F3 behaviour. | A Postgres 16 container for the sinks and the metadata DB, a CeleryExecutor or KubernetesExecutor deployment (Redis broker plus two or more worker hosts), and a second Airflow minor version. Several hours of runs. |
| Wide bounds on small cells | F5 rests on 6 Git-bundle runs, only 4 of which hit the strict mid-run timing; the local-bundle contrast is n=1. | To push a zero-failure bound under 5% needs at least 59 clean trials per pooled fault (computed with stats.py; the dashboard calculator gives the same). Raising the hardened F3, F4, F5 and F6 cells to 59 each is about 200 more hardened trials (twice that with the naive arm); F5 trials take longer because each waits for a bundle deploy. |
| Unbalanced matrix | F4 covers 3 of 5 pipelines, F6 2 of 5, F5 ingestion only (pre-registered in DECISIONS D6). | Run the missing pipeline cells under the existing harness: about 54 more trials at 3 per cell and arm (F4 on 2 pipelines, F6 on 3, F5 on 4). |
| Fault space shaped around duplication | Lost records were zero by construction (a retry always follows). Nothing tests a loss path. | New fault types: a poison record that fails permanently, a metadata DB outage, a worker kill under a distributed executor, clock skew. Each needs harness code and a pre-registration amendment before data. |
| Recovery times reflect artificial pacing | The 1.5 s per 25 rows pacing (D15) sets the ~7 s recovery figure, so it says little about production latency. | A run without pacing on a larger corpus, with the F4 kill timed from the task start rather than wall clock. |
| Exact outcome equality | The embedder is a deterministic hash stand-in; a learned model would need tolerance-based checking, which is untested. | A small learned embedder (CPU is enough for a 200-document partition) and a tolerance-based checker with its own planted-fault tests. |
| Monitoring rests on one drift statistic | The threshold was recalibrated after looking at fixture values (A1); no drift-method comparison (D7 deferred). | A comparison of mean-vector distance against at least one classifier-based or MMD test (Rabanser et al. 2019) on several shifted partitions. CPU only. |
| Same author wrote both arms and the checker | Checker independence is structural and tested with planted faults, but not externally audited. | An external reviewer re-deriving the checker from SPEC.md alone, or a second independent checker implementation. |
| Foreign absolute path in environment.json | `harness_python` records the interpreter path of the machine that ran the study. | Change run_study.py to record only the interpreter version, then rerun the study; a hand edit of the generated record was not made. |

## Security and supply chain (site/)

Dependencies are exact pins (react 18.3.1, react-dom 18.3.1, vite 5.4.11,
@vitejs/plugin-react 4.3.4). `npm audit --omit=dev` reports 0
vulnerabilities: nothing shipped to the browser is affected. The full audit
reports advisories in the pinned vite 5.4.11 dev server (one rated high, in the
bundled launch-editor on Windows); they affect `vite dev`, not the static
build. Clearing them needs vite 6.4.4 or later, which is outside the pinned
toolchain and is left to the owner.
