-- Durable lifecycle state for the O(M) P0 catalog discovery pass.
-- Catalog discovery is intentionally not an admission path: mappings still
-- reference an existing row in public.works and P2's canonical set remains
-- frozen by the runtime coordinator.
CREATE TABLE IF NOT EXISTS public.importer_p0_cycles (
  id uuid PRIMARY KEY,
  status text NOT NULL CHECK (status IN ('IDLE', 'RUNNING', 'COMPLETED')),
  started_at timestamptz NOT NULL,
  completed_at timestamptz,
  metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
  source_progress jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS importer_p0_cycles_status_updated_idx
  ON public.importer_p0_cycles (status, updated_at DESC);
