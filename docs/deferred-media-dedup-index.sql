-- DEFERRED: production attempt on 2026-09-28 timed out after 180 seconds.
-- Incomplete index was removed; no media data were changed.
-- Do not auto-apply: schedule only with provider CPU/work visibility and headroom.
-- Run outside a transaction: Yugabyte online index backfill keeps DML available.
-- Query: media WHERE sha256 = ? AND storage_ready = true LIMIT 1.
-- Before: parallel sequential scan of ~991k rows, one observed miss took 7366ms.
-- Narrow nonunique key: duplicates remain legal; no included payload columns.
-- Cost: one index entry per ready media, maintained only on readiness/hash changes
-- and insertion/deletion. No pages or media data are rewritten by the application.
SET lock_timeout = '3s';
SET statement_timeout = '180s';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_media_ready_sha256
  ON public.media (sha256) WHERE storage_ready = true;
