# Report: AIR-REL

Effectively-once ML pipelines on an at-least-once orchestrator
(Apache Airflow 3.4.0), a pre-registered fault-injection study.

This file is the report in the portfolio standard's sense. It brings
together what used to be AUDIT.md (the build self-audit, 2026-11-06),
docs/research-review.md (the review of 2026-11-07) and ROADMAP.md (the
next cycle). Grades from those files are kept outside the public repo;
everything else they said is below. The locked plan is SPEC.md plus
HYPOTHESES.md (tag proposal-v1); every number in RESULTS.md is
generated from results/trials.jsonl by src/airrel/report.py.

## Background

Airflow executes tasks at least once. A task can be killed after it
wrote half its rows, retried, triggered twice for the same data
interval, or re-run after late data arrives. In each case the DAG can
report success while the sink holds duplicate or stale records. The
study measures the sink, not the DAG status. Prior work and the source
checks are in RESEARCH.md (Apache Airflow 3.4.0 documentation, Best
Practices and DAG Bundles pages; Sculley et al. 2015; Breck et al. 2017;
Rabanser, Janzing and Schölkopf 2019; Carbone et al. 2015).

## Method

SPEC.md (design, fault matrix, metrics, amendments A1 and A2 recorded
before any trial) and METHODS.md (formulas, controls, analysis). Two
arms on one real Airflow 3.4.0 deployment (LocalExecutor, SQLite):
naive (append inserts, commit per batch, latest-data reads, wall-clock
keys) and hardened (one transaction per write task, DELETE plus INSERT
upsert on deterministic keys, fixed-partition reads and writes, atomic
artifact rename). Five pipelines, six fault types, clean controls, and
an independent invariant checker that re-derives the expected sink from
the corpus spec. Every zero carries a one-sided Clopper-Pearson 95
percent upper bound. Deviations, each dated, are in DECISIONS.md
(D1 to D15).

## Results against the locked hypotheses

Scored by code (src/airrel/report.py) into RESULTS.md and
results/summary.json from the 305 records in results/trials.jsonl.

| Hypothesis | Pass line (HYPOTHESES.md) | Measured | Verdict |
| --- | --- | --- | --- |
| H1 naive breaks | fails at least 3 of 6 fault types, F1 and F2 rates at least 0.5 | fails 4 of 6 (F1, F2, F3, F6); F1 rate 1.0, F2 rate 1.0 | Supported |
| H2 hardened holds | zero failures in every fault cell | 0 of 133 fault trials; pooled bound 0.0582 for F1 and F2 (n=50 each), 0.181 F3, 0.393 F6, 0.2831 F4, 0.6316 F5 | Supported |
| H3 bundle pins version | 0 of the F5 Git-bundle runs mix versions | 0 of 6 mixed | Supported |
| H4a cheap recovery | exactly 1 extra execution in at least 90 percent of hardened F1/F2 trials | 100 of 100 | Supported |
| H4b green-but-wrong | at least 80 percent of failed naive F1/F2 trials end in DAG success | 100 of 100 | Supported |

Other measured facts: lost records were zero in all 305 trials (the
failure mode is duplication); the naive arm produced 4,060 duplicate
records across the F1 and F2 trials alone; median hardened recovery from
a write fault is about 7 seconds (RESULTS.md per-cell table); the
exploratory local-bundle control (n=1, outside any verdict) mixed
versions v1 and v2 in one run.

## Misses and what did not happen as planned

No hypothesis missed its pass line. What fell short of the plan or of
a strong claim:

- F4 (scheduler restart) and F5 (mid-run deploy) corrupted neither arm,
  so they do not discriminate between the designs; they are platform
  facts, not evidence for the hardened patterns.
- In two of six Git-bundle F5 trials the deploy landed before
  serialization (the run held version 2 throughout), so the strict
  mid-run timing is exercised in four of six trials.
- Two harness defects were found during the first full run and fixed
  (D13 clean-tree commit, D15 F4 pacing), plus the bundle storage
  isolation fix (D14). Reported results come from a clean re-run under
  the fixed code; the first run is not reported as a result.
- The drift threshold was recalibrated after looking at fixture values
  (SPEC.md amendment A1, before any trial; values in
  results/drift_calibration.json).

## Field profile (systems, reliability) and the verdict

The standard's reliability profile asks for stated availability, RTO
and RPO targets, timed drills, monitoring that would catch each drill's
fault, a runbook and postmortems. This study has timed fault drills
(305 trials, recovery seconds per trial) but no stated RTO or RPO
targets, no alerting that would have caught each fault, and no runbook.
The profile is therefore not met, and the verdict is Functional: tests
pass in CI, `make reproduce` is one command, and the system under study
(a real Airflow 3.4.0 deployment) ran end to end for all 305 trials,
with every record in results/trials.jsonl.

## What was verified, and how

- Pre-registration order in git history: SPEC.md and HYPOTHESES.md are
  commit 15d4e12 (tag proposal-v1); the next commits are c99d9ae
  (research, methods, decisions) and 7e31706 (the first experiment code).
  SPEC.md section 10 (amendments A1, A2) was appended in 7e31706, before
  any trial; tests/test_layout.py pins HYPOTHESES.md and the locked
  bytes of SPEC.md by SHA-256.
- All 305 trial records sit in results/trials.jsonl. Every number in
  RESULTS.md is generated from them by src/airrel/report.py, and CI
  checks that RESULTS.md and results/summary.json regenerate unchanged.
  An independent recount of the F1 and F2 records (100 naive, 100
  hardened) matched the generated report exactly.
- The DAG code that run 1 executed was verified byte-identical to
  dags_src at the study commit after the bundle storage defect (D14)
  was found. Run 2, the reported run, used per-deployment bundle storage.
- The dashboard build (site/src/stats.check.mjs) re-derives every bound,
  every per-cell count and the README headline counts from
  results/trials.jsonl, and fails the build on any mismatch.
- Jev claim_check on the headline claim (results/jev-claim-check.json):
  noul 0.21, route REVIEW. Per the standing rule the claim was then
  verified by hand against results/trials.jsonl; the recount matches and
  the claim stands on that verification.
- Zero spend, local only, nothing published outside this GitHub repo,
  no new credentials.

## Threats to validity

- Sinks and the metadata DB are SQLite only. No Postgres sink, no production-grade metadata store; locking and isolation behaviour there is unmeasured.
- The embedder is a deterministic stand-in for a learned model, and outcome equality is exact match. A learned model needs tolerance-based equality, which changes the checker.
- Pre-registered scoping reductions (DECISIONS D6): F4 runs on three of five pipelines, F6 on two of five, F5 on ingestion only. The matrix is not uniform across pipelines.
- Statistical reach. The pooled write-fault bound is 5.8 percent at n=50; F3 18.1 percent, F6 39.3 percent, F4 28.3 percent, F5 63.2 percent. The small-n faults support direction, not tight rates.
- One machine, LocalExecutor, one Airflow configuration. No distributed executor, no managed Airflow, no second version of Airflow.
- Scheduler restart is a process kill and restart. No host reboot, no metadata DB corruption or failover.
- The drift-method comparison is deferred (DECISIONS D7). The monitoring result rests on one drift statistic with a calibrated threshold (SPEC amendment A1).
- The local-bundle contrast for F5 is a single exploratory trial (n=1). It shows the mechanism, not a rate.
- The hardened arm bundles its patterns with no ablation, so the study cannot say which one is necessary for each fault.
- The fault space is shaped around duplication: lost records were zero by construction (a retry always follows), and nothing tests a loss path.
- Recovery times reflect artificial pacing (1.5 s per 25 rows, D15), so the roughly 7 s figure says little about production latency.
- The same author wrote both arms and the checker. Independence is structural and tested with planted faults, not externally audited.

## Review of 2026-11-07

A review against an empirical research checklist assessed each area
as follows (its scores are kept internal):

| Criterion | Assessment |
| --- | --- |
| Research question | Four precise RQs (SPEC.md section 1): does a green DAG mean a correct sink, under named faults, for named pipeline types. Practical and measurable. |
| Hypotheses | H1 to H4 have numeric thresholds and falsification rules, committed in 15d4e12 before the first code commits (c99d9ae, 7e31706). Verdicts are computed by report.py, not judged by hand. |
| Method | Two-arm controlled comparison on a real Airflow 3.4.0 deployment, an independent invariant checker tested with planted faults, clean controls per pipeline and arm. Weakness: the hardened arm bundles three patterns, so there is no ablation. |
| Baselines | The naive arm is the baseline, built from the anti-patterns the Airflow best-practices page warns against. Clean controls (36 trials, 0 failures) rule out harness breakage. No external baseline (a managed Airflow or another orchestrator). |
| Statistics and uncertainty | Every zero carries a one-sided Clopper-Pearson 95% upper bound; per-cell results beside pooled ones; no p-value fishing. Small cells give wide bounds: F3 0.181, F4 0.2831, F6 0.393, F5 0.6316. |
| Reproducibility | `make reproduce` reruns all 305 trials locally (about 25 minutes, no cost); RESULTS.md and summary.json regenerate byte-for-byte from trials.jsonl; CI runs the tests and the report on Python 3.11 and 3.12; environment.json records versions and the study window. |
| Threats to validity | METHODS.md and this report list the scope limits (SQLite, one machine, LocalExecutor, synthetic corpus, same author for both arms and the checker). |
| Related work | RESEARCH.md checks each cited source. No survey of exactly-once or idempotent-sink literature in stream processing beyond Carbone et al. |
| Limitations | Stated in README, METHODS.md and this report, and repeated on the dashboard. |

Fixed in that review:

1. H2 evidence now bounds every fault cell. summary.json had cited only
   the pooled F1 and F2 bounds; report.py now prints the hardened bound
   for F1 to F6; RESULTS.md and summary.json were regenerated from the
   unchanged trials.jsonl (tests/test_report.py).
2. Recovery cells that do not apply print `n/a`, not `None`.
3. Drift calibration is a generated record: `make calibrate`
   (src/airrel/drift_calibration.py) writes results/drift_calibration.json
   (0.1039 and 0.1028 in-distribution, 0.4948 shifted, threshold 0.25),
   and a test pins the values.
4. METHODS.md now describes the binomial-CDF bisection that stats.py
   actually runs, not a beta-distribution call.
5. `make setup && make test` installs apache-airflow 3.4.0 from
   requirements-lock.txt and passes all tests, including the DAG parse
   test. No test was skipped.
6. The dashboard has its own known-answer check: the JavaScript
   Clopper-Pearson port must match every bound in summary.json and
   RESULTS.md, and the write-path model must reproduce the recorded
   duplicates, losses and extra executions of every trial it covers
   (242 trials).

Remaining gaps and what each one needs:

| Gap | Why it matters | What closing it needs |
| --- | --- | --- |
| No ablation of the hardened patterns | The study cannot say whether one transaction per task, DELETE+INSERT on deterministic keys, or fixed-partition reads is necessary or sufficient for each fault. | A 2x2x2 factorial over F1, F2, F3, F6: six new arms at the current cell sizes (121 write-fault trials per arm) is about 730 more trials, roughly an hour of local runtime under the existing harness. No GPU or paid service. |
| SQLite only, LocalExecutor, one machine, one Airflow version | Postgres isolation levels and a distributed executor could change F1 to F3 behaviour. | A Postgres 16 container for the sinks and the metadata DB, a CeleryExecutor or KubernetesExecutor deployment (Redis broker plus two or more worker hosts), and a second Airflow minor version. Several hours of runs. |
| Wide bounds on small cells | F5 rests on 6 Git-bundle runs, only 4 at the strict mid-run timing; the local-bundle contrast is n=1. | A zero-failure bound under 5% needs at least 59 clean trials per pooled fault (computed with stats.py; the dashboard calculator gives the same). Raising hardened F3 to F6 to 59 each is about 200 more hardened trials (twice that with the naive arm); F5 trials take longer because each waits for a bundle deploy. |
| Unbalanced matrix | F4 covers 3 of 5 pipelines, F6 2 of 5, F5 ingestion only (DECISIONS D6). | About 54 more trials at 3 per cell and arm under the existing harness (F4 on 2 pipelines, F6 on 3, F5 on 4). |
| Fault space shaped around duplication | Lost records were zero by construction (a retry always follows). Nothing tests a loss path. | New faults (poison record that fails permanently, metadata DB outage, worker kill under a distributed executor, clock skew), each with harness code and a pre-registration amendment before data. |
| Recovery times reflect artificial pacing | The 1.5 s per 25 rows pacing (D15) sets the ~7 s recovery figure, so it says little about production latency. | A run without pacing on a larger corpus, with the F4 kill timed from the task start rather than wall clock. |
| Exact outcome equality | The embedder is a deterministic hash stand-in; a learned model would need tolerance-based checking, which is untested. | A small learned embedder (CPU is enough for a 200-document partition) and a tolerance-based checker with its own planted-fault tests. |
| Monitoring rests on one drift statistic | The threshold was recalibrated after looking at fixture values (A1); no drift-method comparison (D7 deferred). | Mean-vector distance against at least one classifier-based or MMD test (Rabanser et al. 2019) on several shifted partitions. CPU only. |
| Same author wrote both arms and the checker | Checker independence is structural and tested with planted faults, but not externally audited. | An external reviewer re-deriving the checker from SPEC.md alone, or a second independent checker implementation. |
| Foreign absolute path in environment.json | `harness_python` records the interpreter path of the machine that ran the study. | Change run_study.py to record only the interpreter version, then rerun the study; a hand edit of the generated record was not made. |

Security and supply chain (site/), as reviewed: dependencies are exact
pins; `npm audit --omit=dev` reported 0 vulnerabilities, so nothing
shipped to the browser was affected. The full audit reported advisories
in the then-pinned vite 5.4.11 dev server (one rated high, in the
bundled launch-editor on Windows), affecting `vite dev`, not the static
build. The site was then pinned to vite 6.4.5 (commit d1e9cbc).

## Next cycle (from ROADMAP.md)

The Build pass measured effectively-once outcomes on one local Airflow
3.4.0 deployment with SQLite sinks. The next cycle, which needs its own
locked proposal before data, widens the claim:

- Sinks and stores: repeat the full matrix with Postgres as the sink
  and as the metadata DB, measuring whether transaction isolation
  changes any cell (in particular the F3 duplicate-trigger cells); add
  one object-store style sink (local MinIO or a file protocol with the
  same semantics) to test the artifact path under the same faults.
- Model realism: replace the hash embedder with a small learned
  embedding model served locally; outcome equality becomes
  tolerance-based, and the checker compares vectors within a stated
  epsilon and reports the mismatch distribution, not only a count. Run
  the drift-method comparison deferred in D7: mean-vector distance
  against the pairwise-distance and classifier-based statistics
  discussed by Rabanser et al. 2019, on the same partitions.
- Scale of evidence: raise F3, F4, F5 and F6 to n=10 per cell or more,
  so their 95 percent bounds fall below 0.30, and extend F4 and F6 to
  all five pipelines; add a second Airflow minor version (3.3.x) to
  check that the version pinning result and the create-run API
  behaviour (D12) are not single-version facts; add a distributed
  executor (Celery or Kubernetes executor on the local fleet) and a
  worker-kill fault, which LocalExecutor cannot express.
- New faults: metadata DB unavailability during a run; clock skew
  between scheduler and worker; a poison record (one malformed document
  that fails the write task on every retry) to measure dead-letter
  behaviour in both arms.
- Reliability profile: stated RTO and RPO targets per pipeline,
  alerting that would catch each fault, and a runbook a second person
  can follow, which together would let a locked cycle reach Evaluated.
- Presentation: publish the next results generated the same way, with
  the Build numbers kept beside them for comparison. No hand-typed
  numbers.

## Conclusion

On this deployment, the naive write pattern corrupted the sink in every
write-path fault trial (121 of 121) while the DAG reported success, and
the hardened pattern held in every fault trial (0 of 133), with the
bounds above limiting how strongly a zero can be read. The failure mode
is duplication, not loss. The claim stops at one machine, SQLite and
LocalExecutor.
