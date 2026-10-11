-- Project Nox Importer Lite 2.0
-- Additive only.  Nothing here changes or deletes a legacy importer row.
-- Lite is activated only by IMPORTER_RUNTIME_MODE=lite after an approved cutover.
BEGIN;

CREATE TABLE IF NOT EXISTS public.importer_lite_tasks (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_type text NOT NULL CHECK (task_type IN ('DISCOVER', 'CATALOG', 'IMPORT_CHAPTER', 'PUBLISH')),
  work_id uuid NULL REFERENCES public.works(id) ON DELETE RESTRICT,
  -- Non-null even before a canonical work exists: source:<id>:<external id>.
  work_key text NOT NULL,
  chapter_key text NULL,
  priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 3), -- STAFF=0,P0=1,P1=2,P2=3
  base_priority smallint NOT NULL CHECK (base_priority BETWEEN 1 AND 3),
  state text NOT NULL DEFAULT 'READY' CHECK (state IN ('READY', 'RUNNING', 'RETRY_WAIT', 'BLOCKED', 'DONE')),
  idempotency_key text NOT NULL UNIQUE,
  next_run_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  max_attempts integer NOT NULL DEFAULT 8 CHECK (max_attempts BETWEEN 1 AND 32),
  lease_owner text NULL,
  lease_until timestamptz NULL,
  reason text NULL,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((state = 'RUNNING') = (lease_owner IS NOT NULL AND lease_until IS NOT NULL))
);

-- This is the explicit executable-work projection. Claims read this tiny table
-- first, then one task for the chosen work; blocked/retry rows never hide it.
CREATE TABLE IF NOT EXISTS public.importer_lite_ready_works (
  work_key text PRIMARY KEY,
  priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 3),
  next_run_at timestamptz NOT NULL,
  last_claimed_at timestamptz NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS importer_lite_tasks_ready_work_idx
  ON public.importer_lite_tasks (work_key, priority, next_run_at, created_at)
  WHERE state = 'READY';
CREATE INDEX IF NOT EXISTS importer_lite_tasks_recovery_idx
  ON public.importer_lite_tasks (state, lease_until, next_run_at)
  WHERE state IN ('RUNNING', 'RETRY_WAIT');
CREATE INDEX IF NOT EXISTS importer_lite_tasks_work_chapter_idx
  ON public.importer_lite_tasks (work_id, chapter_key) WHERE chapter_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS importer_lite_ready_works_claim_idx
  ON public.importer_lite_ready_works (priority, next_run_at, last_claimed_at);

-- Confirmed chapter alternatives; it separates source health from task priority.
CREATE TABLE IF NOT EXISTS public.importer_lite_chapter_sources (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  work_id uuid NOT NULL REFERENCES public.works(id) ON DELETE RESTRICT,
  canonical_chapter_key text NOT NULL,
  source text NOT NULL REFERENCES public.importer_sources(id) ON DELETE RESTRICT,
  source_chapter_id text NOT NULL,
  language text NOT NULL DEFAULT 'und',
  edition text NOT NULL DEFAULT 'default',
  source_rank integer NOT NULL DEFAULT 100,
  confirmed boolean NOT NULL DEFAULT false,
  has_chapter boolean NOT NULL DEFAULT true,
  last_verified_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (source, source_chapter_id),
  UNIQUE (work_id, canonical_chapter_key, source, source_chapter_id)
);
CREATE INDEX IF NOT EXISTS importer_lite_chapter_sources_select_idx
  ON public.importer_lite_chapter_sources (work_id, canonical_chapter_key, confirmed, source_rank);

CREATE TABLE IF NOT EXISTS public.importer_lite_source_health (
  source text PRIMARY KEY REFERENCES public.importer_sources(id) ON DELETE RESTRICT,
  health text NOT NULL DEFAULT 'HEALTHY' CHECK (health IN ('HEALTHY', 'DEGRADED', 'RATE_LIMITED', 'BROKEN', 'DISABLED')),
  cooldown_until timestamptz NULL,
  consecutive_failures integer NOT NULL DEFAULT 0,
  last_error text NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- A canonical identity remains textual even though the current Reader uses a
-- numeric display column. The mapping prevents 12.1 and 12.10 collapsing.
CREATE TABLE IF NOT EXISTS public.importer_lite_chapters (
  work_id uuid NOT NULL REFERENCES public.works(id) ON DELETE RESTRICT,
  canonical_chapter_key text NOT NULL,
  chapter_id uuid NOT NULL REFERENCES public.chapters(id) ON DELETE RESTRICT,
  display_number text NOT NULL,
  order_key text NOT NULL,
  edition text NOT NULL DEFAULT 'default',
  language text NOT NULL DEFAULT 'und',
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (work_id, canonical_chapter_key),
  UNIQUE (chapter_id)
);
CREATE INDEX IF NOT EXISTS importer_lite_chapters_order_idx
  ON public.importer_lite_chapters (work_id, order_key);

-- Upload intent is written before networking.  A completed receipt is
-- durable and reused after restart; an unknown outcome is blocked rather than
-- silently uploading a duplicate file.
CREATE TABLE IF NOT EXISTS public.importer_lite_uploads (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id uuid NOT NULL REFERENCES public.importer_lite_tasks(id) ON DELETE RESTRICT,
  page_position integer NOT NULL CHECK (page_position > 0),
  media_id uuid NOT NULL,
  sha256 text NOT NULL,
  bytes integer NULL,
  mime text NULL,
  bot_reference text NULL,
  storage_shard_id text NULL,
  channel_id text NULL,
  provider_key text NULL,
  -- RETRYABLE means Telegram explicitly rejected the request (for example
  -- 429), while UNKNOWN means an outcome may have reached Telegram and must
  -- never be blindly sent again.
  state text NOT NULL DEFAULT 'INTENT' CHECK (state IN ('INTENT', 'RETRYABLE', 'UPLOADED', 'PERSISTED', 'UNKNOWN')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (task_id, page_position)
);
CREATE INDEX IF NOT EXISTS importer_lite_uploads_recovery_idx
  ON public.importer_lite_uploads (state, updated_at) WHERE state IN ('INTENT', 'RETRYABLE', 'UPLOADED', 'UNKNOWN');

-- The panel remains the authority for STAFF.  Its existing writes immediately
-- promote/demote Lite tasks; no restart or periodic global scan is required.
CREATE OR REPLACE FUNCTION public.importer_lite_refresh_ready_work(p_work_key text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_priority smallint; v_next timestamptz;
BEGIN
  SELECT priority, next_run_at INTO v_priority, v_next
  FROM public.importer_lite_tasks
  WHERE work_key = p_work_key AND state='READY'
  ORDER BY priority, next_run_at, created_at
  LIMIT 1;
  IF NOT FOUND THEN
    DELETE FROM public.importer_lite_ready_works WHERE work_key=p_work_key;
  ELSE
    INSERT INTO public.importer_lite_ready_works(work_key,priority,next_run_at,updated_at)
    VALUES (p_work_key,v_priority,v_next,now())
    ON CONFLICT(work_key) DO UPDATE SET priority=excluded.priority,next_run_at=excluded.next_run_at,updated_at=now();
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.importer_lite_staff_priority_sync()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_work_id uuid; v_active boolean; v_key text;
BEGIN
  v_work_id := COALESCE(NEW.work_id, OLD.work_id);
  IF v_work_id IS NULL THEN RETURN COALESCE(NEW,OLD); END IF;
  v_active := TG_OP <> 'DELETE' AND NEW.status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE');
  UPDATE public.importer_lite_tasks
  SET priority=CASE WHEN v_active THEN 0 ELSE base_priority END, updated_at=now()
  WHERE work_id=v_work_id AND state IN ('READY','RETRY_WAIT','BLOCKED');
  FOR v_key IN SELECT DISTINCT work_key FROM public.importer_lite_tasks WHERE work_id=v_work_id LOOP
    PERFORM public.importer_lite_refresh_ready_work(v_key);
  END LOOP;
  RETURN COALESCE(NEW,OLD);
END;
$$;
DROP TRIGGER IF EXISTS importer_lite_staff_priority_sync_trigger ON public.importer_staff_requests;
CREATE TRIGGER importer_lite_staff_priority_sync_trigger
AFTER INSERT OR UPDATE OR DELETE ON public.importer_staff_requests
FOR EACH ROW EXECUTE FUNCTION public.importer_lite_staff_priority_sync();

-- Atomic, bounded claim. No source joins, metadata reads, predecessor scans or
-- publication checks occur here.  A work-level projection makes fair selection
-- possible without scanning a large task table.
CREATE OR REPLACE FUNCTION public.importer_lite_claim_task(p_worker_id text, p_lease interval DEFAULT interval '10 minutes')
RETURNS SETOF public.importer_lite_tasks LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE v_work_key text; v_task_id uuid;
BEGIN
  SELECT work_key INTO v_work_key
  FROM public.importer_lite_ready_works
  WHERE next_run_at <= now()
  ORDER BY priority, last_claimed_at NULLS FIRST, next_run_at, work_key
  LIMIT 1 FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN RETURN; END IF;
  SELECT id INTO v_task_id FROM public.importer_lite_tasks
  WHERE work_key=v_work_key AND state='READY' AND next_run_at<=now()
  ORDER BY priority,next_run_at,created_at LIMIT 1 FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN
    PERFORM public.importer_lite_refresh_ready_work(v_work_key);
    RETURN;
  END IF;
  RETURN QUERY UPDATE public.importer_lite_tasks
  SET state='RUNNING', lease_owner=p_worker_id, lease_until=now()+p_lease,
      attempts=attempts+1, updated_at=now()
  WHERE id=v_task_id RETURNING *;
  UPDATE public.importer_lite_ready_works SET last_claimed_at=now(),updated_at=now() WHERE work_key=v_work_key;
  PERFORM public.importer_lite_refresh_ready_work(v_work_key);
END;
$$;

REVOKE ALL ON public.importer_lite_tasks, public.importer_lite_ready_works,
  public.importer_lite_chapter_sources, public.importer_lite_source_health,
  public.importer_lite_chapters, public.importer_lite_uploads FROM PUBLIC, anon, authenticated;
GRANT ALL ON public.importer_lite_tasks, public.importer_lite_ready_works,
  public.importer_lite_chapter_sources, public.importer_lite_source_health,
  public.importer_lite_chapters, public.importer_lite_uploads TO service_role;
REVOKE ALL ON FUNCTION public.importer_lite_claim_task(text,interval) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.importer_lite_claim_task(text,interval) TO service_role;
REVOKE ALL ON FUNCTION public.importer_lite_refresh_ready_work(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.importer_lite_refresh_ready_work(text) TO service_role;
REVOKE ALL ON FUNCTION public.importer_lite_staff_priority_sync() FROM PUBLIC;
COMMIT;
