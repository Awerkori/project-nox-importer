-- Cover ingestion performs an exact SHA-256 lookup before uploading a new asset.
-- Without this index Yugabyte planned a parallel sequential scan across `media`.
-- The partial predicate matches the only reusable media state, keeping the index
-- smaller than a full-table index and avoiding an INCLUDE backfill hazard.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_media_sha256_ready
  ON public.media (sha256)
  WHERE storage_ready = true;
