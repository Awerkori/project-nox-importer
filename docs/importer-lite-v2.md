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
- Source rank is independent of task priority. MangaFlix is rank 10 and
  Manhastro (the configured technical source corresponding to the requested
  Manhua source) rank 20; either loses to a confirmed healthy alternative only
  when it is unavailable, unhealthy, cooling down, or not the same edition.
- Media is not publication. An intent precedes upload, a receipt precedes page
  persistence, and the atomic publish transaction validates every page before
  setting `published_at`.
- An upload with an explicit Telegram rejection remains retryable using the
  same reserved media ID. A timeout, lost response, or a Telegram success
  followed by failed receipt persistence becomes `UNKNOWN` and blocks that
  page for recovery instead of blindly uploading it again.
- Fallback is allowed before media persists. Once a page receipt exists, the
  task is pinned to its selected source and page-manifest hash; an alternative
  cannot silently mix another edition into a partial chapter.
- Download request slots and retained page buffers have separate global
  semaphores. Neither is multiplied by the number of chapter workers.
- One bounded reconciler handles expired leases, due retries, and completed
  imports without a publish task. It has no global work sweep.

The migration is additive. It creates no worker, removes no source, modifies
no legacy task, and does not activate Lite. A cutover needs an approved backup,
legacy producer fence, migration application, site baseline, controlled
benchmark, and rollback rehearsal.
