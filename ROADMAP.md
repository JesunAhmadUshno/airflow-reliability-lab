# Roadmap: the Deep pass

The Build pass (this repo, 2026-11-06) measured effectively-once outcomes
on one local Airflow 3.4.0 deployment with SQLite sinks. The Deep pass
widens the claim along the axes AUDIT.md deducts for.

## Sinks and stores

- Repeat the full matrix with Postgres as the sink and as the metadata
  DB. Measure whether transaction isolation changes any cell, in
  particular the F3 duplicate-trigger cells.
- Add one object-store style sink (local MinIO or a file protocol with
  the same semantics) to test the artifact path under the same faults.

## Model realism

- Replace the hash embedder with a small learned embedding model served
  locally. Outcome equality becomes tolerance-based: the checker compares
  vectors within a stated epsilon and reports the mismatch distribution,
  not only a count.
- Run the drift-method comparison deferred in DECISIONS D7: mean-vector
  distance against the pairwise-distance and classifier-based statistics
  discussed by Rabanser et al. 2019, on the same partitions.

## Scale of evidence

- Raise F3, F4, F5, and F6 to n=10 per cell or more, so their 95 percent
  bounds fall below 0.30, and extend F4 and F6 to all five pipelines.
- Add a second Airflow minor version (3.3.x) to check that the version
  pinning result and the create-run API behaviour (DECISIONS D12) are not
  single-version facts.
- Add a distributed executor (Celery or Kubernetes executor on the local
  fleet) and a worker-kill fault, which LocalExecutor cannot express.

## New faults

- Metadata DB unavailability during a run.
- Clock skew between scheduler and worker.
- Poison record: one malformed document that fails the write task on
  every retry, to measure dead-letter behaviour in both arms.

## Presentation

- Publish the Deep results as RESULTS-DEEP.md generated the same way,
  with the Build numbers kept beside them for comparison. No hand-typed
  numbers.
