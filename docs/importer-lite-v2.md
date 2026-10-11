# Importer Lite 2.0

Lite is opt-in with `IMPORTER_RUNTIME_MODE=lite` and requires
`IMPORTER_DB_MODE=direct`. It is not a production cutover switch by default.

```
DISCOVER -> CATALOG -> IMPORT_CHAPTER -> PUBLISH
                 |          |              |
              durable      media          Reader-visible
              task/route   receipt        transaction
```

Invariants:

- Only a durable `READY` task is claimable. The database projection chooses a
  work fairly inside the highest priority that actually has eligible work.
- Priority is `STAFF > P0 > P1 > P2`; the panel’s existing staff request table
  promotes or restores Lite task rows through a database trigger.
- Priority governs **admission only**. A task that was already claimed keeps
  its lease and can finish its download, Telegram receipt, media persistence or
  atomic publication after a STAFF request arrives. There is no global
  priority lock in any finalization transaction. On each finish the durable
  panel overlay is read again, so cancelling the final STAFF request restores
  the base priority instead of leaving an invisible STAFF flag behind.
- A duplicate panel click is harmless: all active request rows for a work form
  one STAFF overlay over idempotent task rows. Releasing one request does not
  demote the work while another active request remains.
- A catalog task discovered before identity resolution attaches its canonical
  `work_id` when it completes that catalog pass. This lets a later panel STAFF
  request wake the durable monitor immediately; source discovery identity is
  retained as the task key and is never replaced by a guessed title match.
- A recurring P0 catalog pass compares the durable source-manifest order
  frontier: chapters beyond it are P0 releases, while a newly revealed older
  manifest entry becomes a P1 repair. This is evidence from the source
  manifest, never a guessed numerical gap; existing P1 tasks keep their
  priority through their idempotency key.
- A recurring P0 **check is not P0 execution**. Ongoing and hiatus works keep
  one durable `monitoring=true` catalog row that is excluded from the
  executable-work projection. A single monitor lane claims at most one due
  check, shares an empty-probe backoff, and never consumes a chapter worker.
  It rechecks a new release after five minutes, exponentially backs off quiet
  works from fifteen minutes to six hours, uses six hours for hiatus, and
  exponentially backs off source failures. Only a newly inserted chapter
  beyond the verified frontier becomes a normal P0 task. Monitoring rows have
  a partial index and are reported separately from executable READY work.
- A catalog manifest is persisted in bounded batches (500 rows per SQL write),
  with one short queue transaction and one ready-work refresh per work. A
  large work therefore does not turn into a per-chapter YSQL transaction loop.
- Source rank is independent of task priority. MangaFlix is rank 10 and
  Manhastro (the configured technical source corresponding to the requested
  Manhua source) rank 20; either loses to a confirmed healthy alternative only
  when it is unavailable, unhealthy, cooling down, or not the same edition.
- Media is not publication. An intent precedes upload, a receipt precedes page
  persistence, and the atomic publish transaction validates every page before
  setting `published_at`. The durable receipt records Telegram `file_id`, bot,
  shard, channel and `message_id` when supplied by the provider.
- An upload with an explicit Telegram rejection remains retryable using the
  same reserved media ID. A timeout, lost response, or a Telegram success
  followed by failed receipt persistence becomes `UNKNOWN` and blocks that
  page for recovery instead of blindly uploading it again. The reconciler
  also surfaces an `UNKNOWN` (or a stale intent) left by a restart as an
  explicit task block; it never silently replays it.
- Fallback is allowed before media persists. Once a page receipt exists, the
  task is pinned to its selected source and page-manifest hash; an alternative
  cannot silently mix another edition into a partial chapter.
- When an adapter exposes an ownership verifier, the durable source-work and
  source-chapter IDs must pass it before the route is downloaded. A failed
  proof is an integrity block for that route, not a reason to publish from a
  merely similar work.
- Download request slots and retained page buffers have separate global
  semaphores. Neither is multiplied by the number of chapter workers.
- Direct image and cover downloads also acquire the existing per-host rate
  limiter; an executor-local per-source chapter semaphore stops one slow
  provider from claiming the entire worker pool.
- One bounded reconciler handles expired leases, due retries, and completed
  imports without a publish task. It also isolates durable upload outcomes
  that need operator reconciliation. Lease and retry recovery stop at each
  task's durable `max_attempts` and become a reasoned block rather than an
  infinite crash/retry loop. It has no global work sweep.
- Discovery, cataloguing and chapter import record source health in the same
  durable table. A successful operation clears a temporary cooldown; a failed
  preferred source therefore makes a confirmed alternative selectable without
  changing the task's `STAFF > P0 > P1 > P2` priority.
- Idle workers share one short claim backoff, so an empty Lite queue performs
  one bounded probe per interval rather than multiplying empty claims by the
  configured worker count.
- Every 60 seconds, when the direct pool has no waiter, Lite emits one compact
  operational snapshot: real publications in the last minute, ready and
  stored-but-unpublished age, queue counts, top blocked reasons, source health,
  productive/idle slots, RSS/heap/external memory, pool state, aggregate
  Telegram activity/429s, and last site-probe result. It does not write a new
  telemetry table or scan catalog rows.

## Chapter sequence policy

The source manifest, not a numerical gap, is the authority on what chapters
exist. Lite persists a textual canonical identity with display number, order
key, edition, language and special-kind information; `12.1`, `12.10`, an
extra numbered `12`, and another edition are never conflated by the importer.
The Reader contract currently permits independent publication of a complete,
valid canonical chapter, so Lite does **not** manufacture predecessor tasks or
hold a valid chapter behind a missing number. If the Reader later requires an
editorial sequence barrier, the dependency must be persisted explicitly for
that verified manifest relation and reawakened by the predecessor's publish
event—not inferred from a float or a gap.

## Catalog metadata boundary

Lite passes the adapter's title aliases, sanitized synopsis, author, artist,
kind, status, year, age rating, adult rating and source genres to the existing
conservative deduplication engine. That engine remains the only writer of
canonical work metadata/tags and preserves manual provenance, healthy covers
and specific adult genres; selecting a chapter source never deletes metadata.

The migration is additive. It creates no worker, removes no source, modifies
no legacy task, and does not activate Lite. A cutover needs an approved backup,
legacy producer fence, migration application, site baseline, controlled
benchmark, and rollback rehearsal.
