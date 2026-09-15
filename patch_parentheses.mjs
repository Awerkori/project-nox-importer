import fs from 'fs';

const p = '../project-nox-manga/supabase/migrations/20260915020000_perfect_gap_and_fairness.sql';
let sql = fs.readFileSync(p, 'utf8');

// We need to wrap each branch of the UNION ALL in parentheses
// (SELECT ... ORDER BY ... LIMIT 25) UNION ALL (SELECT ... ORDER BY ... LIMIT 15) UNION ALL (SELECT ... ORDER BY ... LIMIT 10)

const branch1 = `SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 100 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 25`;

const branch2 = `SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority >= 80 AND q_cand.priority < 100 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 15`;

const branch3 = `SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
      FROM public.importer_queue q_cand
      WHERE q_cand.status in ('QUEUED', 'RETRY') AND q_cand.priority < 80 AND q_cand.next_run_at <= now()
        AND (v_focus_work_id is null or (q_cand.payload->>'workId')::text = v_focus_work_id::text)
        AND (p_source is null or q_cand.source = p_source)
        AND (p_task_type is null or (p_task_type = 'DISCOVERY' and q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK')) or q_cand.task_type = p_task_type)
        AND (coalesce(v_barrier_state, 'OPEN') != 'CLOSED' OR q_cand.task_type in ('DISCOVER_WORKS', 'SYNC_WORK'))
        AND not exists (SELECT 1 FROM public.importer_sources s WHERE s.id = q_cand.source AND (s.enabled = false or s.status in ('PAUSED', 'DISABLED', 'UPSTREAM_BLOCKED') or (s.cooldown_until is not null and s.cooldown_until > now())))
      ORDER BY q_cand.priority DESC, q_cand.next_run_at ASC LIMIT 10`;

sql = sql.replace(branch1, `( ${branch1} )`);
sql = sql.replace(branch2, `( ${branch2} )`);
sql = sql.replace(branch3, `( ${branch3} )`);

fs.writeFileSync(p, sql);
console.log("Added parentheses to UNION ALL branches!");
