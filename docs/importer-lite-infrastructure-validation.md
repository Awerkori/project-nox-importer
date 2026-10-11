# Importer Lite final infrastructure validation

This document separates safe preparation from the authorised staging and
production gates. Nothing in this branch starts Lite, applies a migration,
uploads to Telegram or calls a public benchmark endpoint.

## Read-only production compatibility check — completed

On 2026-10-11, a single direct YSQL/TLS connection used
`default_transaction_read_only=on`, one connection, a ten-second statement
timeout and only catalog queries. It confirmed:

- YugabyteDB reports PostgreSQL 15.12-YB-2026.1.1.2-b0; the direct TLS path
  works and the observed database had two active connections.
- `works`, `chapters`, `pages`, `media`, `importer_sources`,
  `importer_staff_requests`, `importer_chapter_mappings` and `settings` have
  every Lite migration prerequisite. Existing source admin flags and STAFF
  status are compatible.
- `importer_replace_pages(uuid,jsonb)` validates every referenced media row as
  ready, image-shaped and non-empty before replacing Reader pages. The
  `published_at` trigger updates the work feed timestamp.
- Lite tables/functions are absent, as expected before migration; the shared
  `importer_runtime_leader` setting exists and is JSON-shaped.

A single non-benchmark public probe also returned HTTP 200 for health, home
and one already-published Reader chapter (235–309 ms). It establishes only
that the routes were reachable at that instant; collect the baseline samples
and p50/p95 values in the staged load gate below.

The inspected local runtime profile is **not** a cutover configuration: it has
direct DB mode but no explicit Lite mode, uses the legacy worker storage
provider, has no local storage checkpoint, and retains a legacy chapter-worker
value of 32. No secret was inspected or changed. Lite now uses its separate
`LITE_CHAPTER_WORKERS` cap and therefore will not inherit that value.

### Cutover blocker: Reader chapter identity

The real Reader has `chapters_work_id_number_key` on `(work_id, number)`. A
numeric Reader row cannot represent two distinct canonical identities which
parse to one number, such as `12.1` and `12.10`. The isolated Reader branch
`feat/reader-canonical-chapter-identity` now contains the compatible two-phase
migration, UUID-route/API changes and its own guarded staging verifier.

Lite refuses to start until the Reader phase-two contract is present:
`canonical_chapter_key`, `display_number`, `chapter_order_key`, kind/edition/
language fields, a canonical unique index, nullable legacy `number`, and no
`chapters_work_id_number_key`. It fails closed with
`READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED`; this prevents downloads or
uploads from beginning against a schema that could collapse identities. Neither
Reader migration has been applied outside local disposable tests.

## Disposable/staging YSQL migration gate

`npm run validate:lite:staging` is intentionally fail-closed:

- it accepts only `LITE_VALIDATION_*` credentials and has no production-env
  fallback;
- it requires `LITE_VALIDATION_TARGET=staging` or `disposable`, rejects a
  production-looking database name, verifies the three application roles, and
  uses a one-connection TLS pool;
- without `LITE_VALIDATION_APPLY=1` it is a read-only schema preflight;
- applying requires the exact explicit confirmation
  `LITE_VALIDATION_CONFIRM=APPLY_LITE_TO_NON_PRODUCTION`;
- it applies only the additive Lite migration, verifies all Lite tables,
  indexes, functions and the STAFF trigger, and fails if any legacy table row
  count changes.

Use a restored/sanitised clone with the actual Reader schema and roles. First
run preflight, then run the guarded apply. Do not point this runner at the
production database.

## Telegram validation — requires isolated credentials and approval

Use a dedicated non-production bot plus private validation channel/shard that
the Reader does not serve. First verify bot identity and posting permission
without upload. With approval, run a gradual synthetic-image ladder: one small
valid image, then controlled per-bot/per-channel concurrency. Persist and
verify `file_id`, bot, shard, channel, message ID, MIME, byte count and hash;
then test one 429/cooldown and one post-upload database interruption. Never
move the same request to another bot to evade a Telegram retry delay.

## Reader/site and sustained benchmark — requires approved staging run

1. Record a quiet baseline for home, catalog, work page, Reader and API:
   p50/p95, 5xx, direct-pool wait and database connections.
2. Apply Reader phase 1, backfill/verify it, then Reader phase 2 and the Lite
   migration to the disposable/staging clone. Run the integration fixture
   through discovery, catalog, media persistence, publication and a real
   Reader fetch. Verify `published_at`, textual chapter ordering, old UUID
   URLs, Telegram references and no duplicate event.
3. Supply a known eligible queue. Ramp chapter, source, Telegram and DB limits
   one dimension at a time. Record pages/chapter, bytes/chapter, source latency,
   upload latency, pool wait, RSS/heap/external memory and 429s.
4. Run two consecutive ten-minute windows at 8, then 9, then 10 valid
   Reader-visible chapters/minute, followed by a soak. Fail the gate for an
   unexplained throughput decline, rising ready/stored age, site regression,
   task leak or resource budget breach.

`LITE_CHAPTER_WORKERS` is intentionally independent of the legacy
`MAX_CONCURRENT_CHAPTERS` and defaults to one. Set it explicitly only to the
highest level whose staging window remains healthy; the historical local value
of `MAX_CONCURRENT_CHAPTERS=32` is not a Lite capacity approval.

Only `published_at` chapters whose Reader pages resolve count. A queue, upload
or heartbeat does not count as throughput.

## Future DIScloud cutover exclusivity check

After owner approval, stop/fence the legacy producer, deploy exactly one
artifact with `IMPORTER_RUNTIME_MODE=lite` and `IMPORTER_DB_MODE=direct`, then
verify its SHA in startup logs. The shared `settings.importer_runtime_leader`
lease must name one Lite boot identity only; logs must show no legacy engine
start and one active Lite leader. Check the process list/health endpoint and
the lease before admitting benchmark work. Roll back by stopping Lite and
restoring the legacy producer; do not restore the database or delete Lite rows.

The ordered operational gates, evidence to retain and non-destructive rollback
conditions are in [the dedicated cutover runbook](importer-lite-cutover-runbook.md).
