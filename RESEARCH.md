# Research Review: Effectively-once ML Pipelines on Apache Airflow 3.x

Date: 2026-11-06. This file is the literature record for the AIR-REL study (SPEC.md).
It starts from Claude Verdict Pack 03 (Apache Airflow for ML Workflows, 2026-11-05) and
re-checks every row the pack marked Unverified against primary sources. Rows that could
not be verified are dropped from all claims.

## Search record

Searches run 2026-11-06 (this build) plus the pack's own pass on 2026-11-05:

- Official documentation: "Apache Airflow DAG bundles documentation GitDagBundle versioning"
  (airflow.apache.org), then the full Dag Bundles page and the full Best Practices page
  read directly.
- Papers: title searches for Sculley et al. 2015 and Breck et al. 2017; Rabanser et al. 2019;
  Carbone et al. 2015 (arXiv 1506.08603); Alvaro et al. 2015 (lineage-driven fault injection).
- Inclusion rule: official Airflow documentation and release notes for platform claims;
  primary papers for method claims. Aggregator and vendor pages are leads only, never evidence.

## Evidence table (pack rows, re-checked)

| Row | Claim | Source | Status after re-check |
|---|---|---|---|
| E1 | Airflow can retry a task, so tasks should produce the same outcome on every re-run | Official Best Practices page, Airflow 3.4.0 docs | Verified in this pass (page text read directly) |
| E2 | Treat tasks like database transactions; never produce incomplete results | Official Best Practices page | Verified in this pass |
| E3 | Replace INSERT with UPSERT on re-runs; read and write a fixed partition, never the latest data; do not use now() for critical computation | Official Best Practices page | Verified in this pass (the hardened arm is built from exactly these three rules) |
| E4 | Airflow 3.0.0 went GA on 2025-04-22 | Airflow 3.0.0 release notes | Verified in the pack; not load-bearing here |
| E5 | Airflow 3 runs a DAG to completion on the version it started with, even if code is uploaded mid-run | Airflow 3 GA blog (pack); mechanism documented on the official Dag Bundles page | Verified as a documented mechanism for versioned bundles; this study tests it empirically (RQ3) |
| E6 | Local, S3 and GCS bundles do not version; tasks run the latest code; a Git bundle is needed for versioning | Orchestra guide (pack, unverified); official Dag Bundles page, Airflow 3.4.0 docs | VERIFIED in this pass against official docs: "LocalDagBundle does not support versioning. Tasks always run against the latest code on disk." "GitDagBundle supports versioning. Each Dag run records the Git commit it was created with." S3 and GCS bundles: same no-versioning note |
| E7 | In Airflow 3 the scheduler runs backfills, startable from UI or API | Airflow 3 GA blog | Verified in the pack; not used in this study (re-runs are fresh triggers) |
| E8 | Assets replace datasets; @asset decorator for event-driven scheduling (AIP-74, AIP-75) | Airflow 3.0.0 release notes | Verified in the pack; not used in this study's design |
| E9 | AIP-83 removed the execution-date unique constraint | Airflow 3 GA blog | Verified in the pack; context only |
| E10, E11 | Airflow 3.3.x release line (3.3.0 on 2026-07-06; 3.3.2 exists) | Official announcements and docs | Partly verified in the pack. Superseded for this study: the stable docs at build time are the 3.4.0 documentation, and the exact installed version is recorded in results/environment.json |
| E12 | A proposal would let users pin which DAG version the scheduler uses | Apache Confluence AIP drafts | Verified as a proposal in the pack; context only. Note: 3.3 added rerun_with_latest_version controls for clear, rerun and backfill on versioned bundles (release notes), which is a different mechanism from pinning new runs |
| E13 | Astronomer "durable execution" saves state such as a submitted job ID across retries | Astronomer docs | UNVERIFIED for open-source core. Dropped: not used anywhere in this study |
| E14 | ML systems carry hidden technical debt in data dependencies and pipeline glue | Sculley et al., "Hidden Technical Debt in Machine Learning Systems", NeurIPS 2015 (Advances in Neural Information Processing Systems 28, pp. 2503-2511) | VERIFIED in this pass (bibliographic record confirmed in two independent bibliographies) |
| E15 | A rubric of ML production tests, including data and monitoring tests | Breck et al., "The ML Test Score: A Rubric for ML Production Readiness and Technical Debt Reduction", IEEE BigData 2017, pp. 1123-1132 | VERIFIED in this pass (bibliographic record confirmed) |
| E16 | Two-sample tests on reduced representations detect dataset shift well | Rabanser, Gunnemann, Lipton, "Failing Loudly: An Empirical Study of Methods for Detecting Dataset Shift", NeurIPS 2019; arXiv:1810.11953 | VERIFIED in this pass (paper text and arXiv record read). Context for the monitoring pipeline; this study does not compare drift methods |
| E17 | Exactly-once state in stream processing via asynchronous snapshots | Carbone, Fóra, Ewen, Haridi, Tzoumas, "Lightweight Asynchronous Snapshots for Distributed Dataflows", arXiv:1506.08603, 2015 | VERIFIED in this pass (arXiv record read). Contrast case: the engine provides the guarantee there; Airflow does not, which is why the guarantee must come from pipeline design here |
| E18 | Fault injection guided by data lineage finds bugs in fault-tolerance protocols | Alvaro, Rosen, Hellerstein, "Lineage-driven Fault Injection", SIGMOD 2015 | Partly verified: the paper and venue are confirmed by secondary references; the primary PDF was not re-fetched in this pass. Cited as method background only, never as evidence for a claim |

## Horizons

- Past: Airflow 2.x practice already knew the idempotency rules (E3 was in the 2.7.0 docs).
  What was missing was version pinning: in 2.x, a re-run always used the latest code.
- Present: Airflow 3.x adds DAG bundles with versioning (Git bundle), per-run bundle version
  records, and rerun version controls. The platform now documents the mechanisms; measured
  evidence that the mechanisms deliver effectively-once outcomes is what this study adds.
- Future: scheduler-side DAG version pinning for new runs is still a proposal (E12).
  Asset-event-driven retraining on drift (E8, pack upgrade 6) is a Deep-pass direction,
  not part of this build.

## What this study does not take from the literature

- No vendor feature (E13) is assumed to exist in open-source Airflow.
- No exactly-once claim is made for Airflow itself. The literature and the docs agree:
  the orchestrator is at-least-once, and effectively-once is a property of the pipeline
  design, proven by fault injection and invariant checks (the pack's central reframing).
