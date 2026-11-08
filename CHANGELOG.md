# Changelog

All notable changes to this project. Format: Keep a Changelog 1.1.
Versions follow Semantic Versioning; no release is tagged yet.

## [Unreleased]

### Changed
- 2026-11-08: Adopted the portfolio standard v2. README opens with the
  question, headline, field, job family and verdict (Functional,
  pre-registered). REPORT.md is the report: it takes in AUDIT.md,
  docs/research-review.md and ROADMAP.md, which are removed; their
  grades moved out of the public repo (grades are internal). The
  dashboard's limitations panel reads REPORT.md instead of the audit
  grade. The lock is tagged proposal-v1 on 15d4e12, and a test pins
  HYPOTHESES.md and the locked bytes of SPEC.md by SHA-256. CI checks
  that RESULTS.md and results/summary.json regenerate unchanged, with
  read-only permissions. CodeQL and Dependabot added; main protected.
  Files named by the lock keep their names.

### Fixed
- 2026-11-07: Site pinned to vite 6.4.5 (d1e9cbc), clearing the vite
  5.4.11 dev-server advisories.

### Added
- 2026-11-07: Live dashboard in site/, published to GitHub Pages, with
  a known-answer check of every browser computation.
- 2026-11-07: Research review; H2 bounds for every fault cell; `n/a`
  for recovery cells that do not apply; drift calibration recorded in
  results/drift_calibration.json; METHODS.md bound description
  corrected.
- 2026-11-06: The full study: 305 trials on Airflow 3.4.0, RESULTS.md
  and results/summary.json generated from results/trials.jsonl; harness
  defects D13 to D15 found and fixed before the reported run.
- 2026-11-06: Harness: corpus, two arms, five DAGs, fault injector,
  invariant checker, tests, CI on Python 3.11 and 3.12.
- 2026-11-06: Research record (RESEARCH.md), METHODS.md, DECISIONS.md.
- 2026-11-06: Pre-registration, SPEC.md and HYPOTHESES.md (15d4e12),
  before any code or result.
