# Pre-registered Hypotheses

Pre-registered 2026-11-06, before any experiment code was written. The full study design,
fault matrix, trial counts, and metric definitions are in SPEC.md. Verdicts use the
programme protocol: Supported, Partially supported, Refuted, Inconclusive, Not yet measured.
A failed hypothesis is a valid result and is reported, not hidden.

## H1 (RQ1): the naive arm breaks under write-path faults

Claim: the naive arm (append writes, batch commits, latest-data reads, timestamp keys)
fails its sink invariants (duplicates > 0 or losses > 0) in at least 3 of the 6 fault
types, and in at least 50 percent of trials for each of F1 and F2.

Falsified if: the naive arm passes all 6 fault types, or fails fewer than 3, or its F1
or F2 failure rate is below 50 percent.

## H2 (RQ2): the hardened arm holds effectively-once outcomes

Claim: the hardened arm (single transaction per write task, upsert on deterministic keys,
fixed-partition reads and writes, deterministic run keys, atomic artifact rename) shows
zero duplicates, zero losses, and zero content mismatches in every fault cell.

Verdict mapping:
- Supported: zero failures in every cell, reported with Clopper-Pearson 95 percent upper
  bounds per cell and pooled per fault (pooled n = 50 for F1 and for F2).
- Partially supported: the hardened arm passes all write-path faults (F1, F2, F3, F6) and
  fails only platform-level faults (F4 or F5), or fails a single cell with a classified cause.
- Refuted: the hardened arm fails any of F1, F2, F3, or F6 in more than one cell, or loses
  records in any cell.

## H3 (RQ3): a versioned bundle pins a run to one code version

Claim: under F5 with the Git DAG bundle, every ingestion run records exactly one
CODE_VERSION across its tasks (version-mix rate 0 of 3 per arm).

Falsified if: any F5 run records two or more distinct CODE_VERSION values.
The exploratory local-bundle control (n = 1) is reported beside the claim and is not part
of this verdict.

## H4 (RQ4): recovery is cheap, and the dangerous failure is green-but-wrong

Claim, two parts:
- H4a: in the hardened arm, each F1 and F2 trial recovers with exactly 1 extra task
  execution (the single retry of the faulted write task), in at least 90 percent of trials.
- H4b: in the naive arm, at least 80 percent of failed F1 and F2 trials are green-but-wrong:
  the dag run ends in state success while the sink invariants fail.

Falsified if: H4a holds in fewer than 90 percent of hardened trials, or H4b in fewer than
80 percent of failed naive trials. Recovery times are reported per fault as medians with
ranges, whatever the verdict.

## Notes on strength of evidence

- Zero failures in n trials never proves a failure rate of zero. Every zero cell carries
  its exact 95 percent upper bound (for n = 10 the bound is about 26 percent; pooled
  n = 50 it is about 6 percent). The README and RESULTS.md must print the bounds next to
  any zero.
- Trial counts and the one allowed adaptation (F1 and F2 n drops to 6 if the median smoke
  trial exceeds 25 seconds) are fixed in SPEC.md section 6.
