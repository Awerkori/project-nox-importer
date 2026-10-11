# Lite validation gates and capacity budget

No Lite process has been deployed or benchmarked by this branch. The values
below are a **historical infrastructure budget**, not a Lite performance
claim: `benchmark_homologation_30m_final.json` is from 2026-09-21 and the
later 30-minute legacy validation still reported 1,295 staged rows, so it is
not accepted as proof of the Lite antiblock invariant.

## Isolated validation completed

The additive Lite migration was applied in an ephemeral PGlite database (not
to production) and the direct repository/executor path was exercised with
actual SQL functions and the existing atomic Reader page function. The current
integration suite verifies:

- panel-triggered STAFF promotion followed by strict `STAFF > P0 > P1 > P2`;
- STAFF is an admission overlay rather than a finalization lock: P0/P1/P2
  tasks already running complete safely while duplicated panel requests are
  collapsed logically, and cancellation restores the durable base priority;
- concurrent claim fencing: exactly one worker can lease a durable task;
- a ready work remains claimable behind 1,300 blocked task rows and peers of
  the same priority rotate fairly by work, including a 1,300-chapter ready
  work that cannot monopolize other P1 works; a 1,300-chapter catalog batch is
  also materialised through the bounded batch enqueue path;
- expired leases, due retries, publish-task materialisation and bounded,
  per-work blocked-task re-evaluation, with the origin import settled after
  publication so recovery does not recreate completed work;
- atomic Reader-visible publication only after cover and page media validation;
- healthy source fallback before storage, and safe source/manifest pinning
  after partial storage, including adapter ownership proof before an
  alternative source route is used;
- explicit Telegram 429 retry versus ambiguous upload/receipt failure, which
  becomes durable `UNKNOWN` rather than a blind duplicate upload, including a
  reconciler pass that surfaces a crash-surviving unknown upload as one
  observable blocked task while healthy neighbouring work proceeds;
- source-health persistence across discovery/catalog/import: failures create a
  bounded cooldown and successful operations clear it, allowing a confirmed
  alternative source route without changing task priority;
- source administration uses the actual stable `importer_sources` contract:
  paused/disabled sources are not seeded or contacted, cooldown is respected,
  and a disabled preferred chapter route yields immediately to a confirmed
  healthy alternative; optional panel flags are read safely when they exist,
  including metadata-only cataloguing with chapter ingestion disabled;
- a recurring P0 observation remains exactly one durable, recoverable row
  after a prolonged upstream outage, with a six-hour capped retry cadence;
  it does not block ready P1 work even after its ordinary attempt threshold;
- a repeatedly expired lease reaches `max_attempts` and becomes an explicit
  blocked task, rather than cycling forever through recovery;
- P0 monitoring materialises newly observed chapters as P0 without rebucketing
  initial P1 fill work, while a source manifest entry at or before the durable
  frontier is materialised as a P1 repair. 1,300 monitor rows are invisible to
  normal P1/P2 claims; monitor probes have their own one-slot empty backoff,
  bounded intervals, interrupted-lease recovery, and panel wake-up after the
  catalog task attaches its canonical work ID;
- a fully isolated path from `DISCOVER` through `CATALOG`, source route,
  Telegram receipt, `PUBLISH`, `published_at` and the Reader page manifest.
- the Telegram receipt journal persists provider file ID, bot, shard, channel
  and message ID before a page can enter the publish transaction;
- a Telegram 429 leaves the same durable page retryable without bot hopping
  and applies the server wait to both the selected bot and channel/shard;
- a compact Lite progress probe counts publication events rather than download
  completion, including ready/stored age so an online process cannot be
  mistaken for real progress;
- the Lite catalog boundary forwards full work metadata (including aliases,
  adult classification and specific source tags) to the existing conservative
  deduplication/tag writer after synopsis sanitization;
- slow uploads with concurrent page discovery, proving that the global retained
  page-buffer limit is applied rather than multiplied by chapter workers.
- the additive migration is re-applied in the same ephemeral database,
  confirming that it preserves pre-existing Reader rows and Lite tables rather
  than requiring a destructive reset.

This is an integration and regression gate, not an external capacity result:
PGlite does not model Yugabyte latency, real source limits, Telegram capacity,
DIScloud RSS, or public-site contention.

## Full-suite result in this workspace

The complete repository suite was run after the Lite tests. It reported 102
passing files / 606 passing tests / 15 skipped tests and 7 failing files / 13
failures. None is in `src/lite/` or `src/storage/__tests__/direct-telegram`:
the failures are legacy tests that either require an absent direct-YSQL
password in this isolated workspace, wait on the legacy memory autotuner until
their 30-second timeout, or assert pre-existing legacy scheduler snapshots
(including the known `claim-filters` 4-vs-5 assertion). They are recorded as
baseline debt and are not presented as Lite validation.

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
