# Audit: AIR-REL build

Date: 2026-11-06. Auditor: the build worker, self-audit before delivery
(house rule). Grade on the 10,000 scale, floored, with itemized
deductions. The grade covers the build and the strength of its claims,
not effort.

## Grade: 6,900 of 10,000

Deductions:

| Points | Item |
|---|---|
| -450 | Sinks and the metadata DB are SQLite only. No Postgres sink, no production-grade metadata store; locking and isolation behaviour there is unmeasured. |
| -350 | The embedder is a deterministic stand-in for a learned model, and outcome equality is exact match. A learned model needs tolerance-based equality, which changes the checker. |
| -400 | Pre-registered scoping reductions (DECISIONS D6): F4 runs on three of five pipelines, F6 on two of five. The matrix is not uniform across pipelines. |
| -300 | F5 runs on ingestion only, three trials per arm, and in two of six Git-bundle trials the deploy landed before serialization (the run held version 2 throughout), so the strict mid-run timing is exercised in four of six trials. |
| -500 | Statistical reach. Pooled write-fault bound is 5.8 percent at n=50; F3 bound 18.1 percent, F6 39.3 percent, F4 28.3 percent, F5 63.2 percent. The small-n faults support direction, not tight rates. |
| -350 | One machine, LocalExecutor, one Airflow configuration. No distributed executor, no managed Airflow, no second version of Airflow. |
| -200 | Scheduler restart is a process kill and restart. No host reboot, no metadata DB corruption or failover. |
| -250 | The drift-method comparison is deferred (DECISIONS D7). The monitoring result rests on one drift statistic with a calibrated threshold (SPEC amendment A1). |
| -150 | Two harness defects were found during the first full run and fixed (D13 clean-tree commit, D15 F4 pacing), plus the bundle storage isolation fix (D14). Final results come from a clean re-run under the fixed code, but the defects cost a full cycle. |
| -150 | The local-bundle contrast for F5 is a single exploratory trial (n=1). It shows the mechanism, not a rate. |
| Total -3,100 | |

## What was verified, and how

- Pre-registration order in git history: SPEC.md and HYPOTHESES.md are
  commit 15d4e12; the first experiment code is c99d9ae and 7e31706.
- All 305 trial records sit in results/trials.jsonl. Every number in
  RESULTS.md is generated from them by src/airrel/report.py. An
  independent recount of the F1 and F2 records (100 naive, 100 hardened)
  matches the generated report exactly.
- The DAG code that run 1 executed was verified byte-identical to
  dags_src at the study commit after the bundle storage defect (D14) was
  found. Run 2, the reported run, used per-deployment bundle storage.
- Unit tests: 15 passed on Python 3.12 locally; CI covers 3.11 and 3.12.
- Jev claim_check on the headline claim (results/jev-claim-check.json):
  noul 0.21, route REVIEW. Per the standing rule the claim was then
  verified by hand against results/trials.jsonl; the recount matches and
  the claim stands on that verification.
- Zero spend, local only, nothing published outside this GitHub repo, no
  new credentials.

## Open items handed to the Deep pass

See ROADMAP.md. In short: Postgres sinks, a learned embedder with
tolerance-based equality, a distributed executor, larger n on F3 to F6,
the drift-method comparison, and a second Airflow minor version.
