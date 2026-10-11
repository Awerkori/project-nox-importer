# Lite validation gates and capacity budget

No Lite process has been deployed or benchmarked by this branch. The values
below are a **historical infrastructure budget**, not a Lite performance
claim: `benchmark_homologation_30m_final.json` is from 2026-09-21 and the
later 30-minute legacy validation still reported 1,295 staged rows, so it is
not accepted as proof of the Lite antiblock invariant.

Historical representative sample:

| Measure | Observed |
|---|---:|
| chapters / minute | 10.16 over 30 min (305 unique publications) |
| pages / chapter, mean / p95 | 24.2 / 52 |
| MB / chapter, mean / p95 | 8.26 / 49.26 |
| pages / minute | 273.5 |
| MB / minute | 94.9 |
| Telegram 429 | 0 |
| direct pool wait p95 | 1 ms |

Derived mean demand:

| Target | pages/min | MB/min |
|---|---:|---:|
| 8 chapters/min | 193.6 | 66.1 |
| 9 chapters/min | 217.8 | 74.3 |
| 10 chapters/min | 242.0 | 82.6 |

The final Lite configuration must be selected by the slowest measured stage
(source download, Telegram upload, YSQL publication, or Reader/site health),
with headroom. The historical run's p95 chapter was 49.26 MB, so means alone
are insufficient for a memory or bandwidth budget.

## Required pre-cutover procedure

1. Apply the additive migration to a disposable/staging database and exercise
   concurrent claims, expired leases, staff priority changes and page receipt
   recovery.
2. Run source and Telegram ramps with small authorised files, recording bot,
   channel and global limits; never interpret an HTTP 200 as an import-capable
   source health check.
3. Establish a fresh site baseline and enforce the documented health gate.
4. With a sufficiently supplied Lite queue, run two consecutive 10-minute
   windows and then a longer soak. Count only chapters with `published_at` and
   Reader-accessible pages.
5. Compare ready age, stored-but-unpublished age, YSQL wait, Telegram 429,
   RSS/heap/external memory, source errors and site p95s. Any unexplained
   decline while `READY` work exists fails the gate.

The branch intentionally has no deployment or migration runner. Cutover and
rollback require owner approval and a producer fence so the legacy importer
and Lite cannot publish the same canonical chapter concurrently.
