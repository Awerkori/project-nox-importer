-- Additive: previous importers keep operating during cutover. No historical backfill.
-- The chapter PK makes ambiguous retries idempotent; minute index serves reconciliation.
CREATE TABLE IF NOT EXISTS importer_publication_events (
  chapter_id uuid PRIMARY KEY,
  transition_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL,
  bucket_minute timestamptz NOT NULL,
  is_fresh_release boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS importer_publication_events_minute_idx
  ON importer_publication_events (bucket_minute);
COMMENT ON COLUMN importer_publication_events.recorded_at IS
  'DB clock immediately before transaction commit, NOT an exact commit timestamp. Event visibility is atomic with chapter and bucket.';
