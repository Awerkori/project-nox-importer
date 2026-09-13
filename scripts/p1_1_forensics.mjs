import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';

const SUPABASE_MGMT_TOKEN = "sbp_42a1ea952d51ffacf6e1eb1413b8af39638aa244";
const SUPABASE_PROJECT_REF = "izregkwaqdygwioqzwwo";

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

async function runSql(sql) {
  const res = await fetch(`https://api.supabase.com/v1/projects/${SUPABASE_PROJECT_REF}/database/query`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${SUPABASE_MGMT_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query: sql })
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`SQL Error HTTP ${res.status}: ${txt}`);
  }
  return await res.json();
}

async function main() {
  console.log("========================================================");
  console.log("PROJECT NOX — P1.1 PUBLICATION RELEASE FORENSICS");
  console.log(`Timestamp: ${new Date().toISOString()}`);
  console.log("========================================================\n");

  // 1. Current Barrier State
  const { data: barrierSetting } = await sb
    .from('settings')
    .select('key, value')
    .eq('key', 'publication_safety_barrier')
    .maybeSingle();
  console.log(`1. CURRENT BARRIER: ${barrierSetting?.value || 'UNKNOWN'}`);

  // 2. Publication & Processing Telemetry
  const pubTelemetrySql = `
    SELECT
      max(c.published_at) as last_successful_publication_at,
      count(*) FILTER (WHERE c.published_at > now() - interval '5 minutes') as pub_last_5m,
      count(*) FILTER (WHERE c.published_at > now() - interval '30 minutes') as pub_last_30m,
      count(*) FILTER (WHERE c.published_at > now() - interval '1 hour') as pub_last_1h,
      count(*) FILTER (WHERE c.published_at > now() - interval '24 hours') as pub_last_24h
    FROM public.chapters c;
  `;
  const pubTele = await runSql(pubTelemetrySql);
  console.log('\n2. PUBLICATION TELEMETRY:');
  console.table(pubTele);

  // Storage / processing telemetry (pages inserted / media stored)
  const procTelemetrySql = `
    SELECT
      max(m.created_at) as last_media_stored_at,
      count(*) FILTER (WHERE m.created_at > now() - interval '5 minutes') as stored_last_5m,
      count(*) FILTER (WHERE m.created_at > now() - interval '30 minutes') as stored_last_30m,
      count(*) FILTER (WHERE m.created_at > now() - interval '1 hour') as stored_last_1h
    FROM public.media m
    WHERE m.purpose = 'chapter_page';
  `;
  const procTele = await runSql(procTelemetrySql);
  console.log('\n3. STORAGE / PROCESSING TELEMETRY:');
  console.table(procTele);

  // 4. Backlog Status Breakdown in importer_chapter_mappings
  const mappingBreakdownSql = `
    SELECT
      status,
      count(*) as total_count,
      min(created_at) as oldest_record,
      max(created_at) as newest_record,
      min(updated_at) as oldest_update,
      max(updated_at) as newest_update
    FROM public.importer_chapter_mappings
    GROUP BY status
    ORDER BY total_count DESC;
  `;
  const mapBreakdown = await runSql(mappingBreakdownSql);
  console.log('\n4. IMPORTER CHAPTER MAPPINGS BREAKDOWN:');
  console.table(mapBreakdown);

  // 5. Stored but not published chapters (pages exist in public.pages but published_at IS NULL in chapters)
  const storedNotPubSql = `
    SELECT count(distinct c.id) as stored_not_published_count
    FROM public.chapters c
    JOIN public.pages p ON p.chapter_id = c.id
    WHERE c.published_at IS NULL;
  `;
  const storedNotPub = await runSql(storedNotPubSql);
  console.log('\n5. STORED BUT NOT PUBLISHED COUNT:', storedNotPub[0]?.stored_not_published_count);

  // 6. Top works with STAGED chapters
  const topStagedWorksSql = `
    SELECT
      m.work_id,
      w.title,
      count(*) as staged_count,
      min(m.chapter_number) as min_chapter,
      max(m.chapter_number) as max_chapter
    FROM public.importer_chapter_mappings m
    JOIN public.works w ON w.id = m.work_id
    WHERE m.status = 'STAGED'
    GROUP BY m.work_id, w.title
    ORDER BY staged_count DESC
    LIMIT 10;
  `;
  const topStaged = await runSql(topStagedWorksSql);
  console.log('\n6. TOP WORKS WITH STAGED CHAPTERS:');
  console.table(topStaged);

  // 7. Sample of Oldest Stuck Chapters in STAGED
  const stuckSampleSql = `
    SELECT
      m.work_id,
      w.title as work_title,
      m.source,
      m.chapter_number,
      m.chapter_sort_key,
      m.status,
      m.created_at as staged_created_at,
      m.updated_at as staged_updated_at
    FROM public.importer_chapter_mappings m
    JOIN public.works w ON w.id = m.work_id
    WHERE m.status = 'STAGED'
    ORDER BY m.updated_at ASC
    LIMIT 10;
  `;
  const stuckSample = await runSql(stuckSampleSql);
  console.log('\n7. SAMPLE OF OLDEST STUCK CHAPTERS:');
  console.table(stuckSample);

  // 8. Run Barrier RPC on each stuck chapter
  console.log('\n8. EVALUATING BARRIER ON STUCK SAMPLE:');
  for (const s of stuckSample) {
    const barrierSql = `
      SELECT * FROM public.importer_check_publication_barrier('${s.work_id}'::uuid, ${s.chapter_sort_key}::numeric);
    `;
    const res = await runSql(barrierSql);
    const b = res[0] || {};
    console.log(`\n-> Work "${s.work_title}" (Cap ${s.chapter_number} | sort ${s.chapter_sort_key}):`);
    console.log(`   can_publish: ${b.can_publish}`);
    console.log(`   reason: ${b.reason}`);
    console.log(`   blocking_count: ${b.blocking_count}`);
    console.log(`   blocking_sort_keys: ${JSON.stringify(b.blocking_sort_keys)}`);

    // Audit the first 5 blocking sort keys
    if (b.blocking_sort_keys && b.blocking_sort_keys.length > 0) {
      const keysToAudit = b.blocking_sort_keys.slice(0, 5);
      const auditKeysSql = `
        SELECT
          ${s.work_id ? `'${s.work_id}'::uuid` : 'null'} as work_id,
          m.chapter_number,
          m.chapter_sort_key,
          m.status as mapping_status,
          m.source as mapping_source,
          m.is_gap,
          c.id as chapter_id,
          c.published_at,
          q.id as queue_job_id,
          q.status as queue_status,
          q.task_type,
          q.attempts,
          q.max_attempts,
          q.last_error as queue_last_error
        FROM (SELECT unnest(ARRAY[${keysToAudit.join(',')}]) as sort_key) k
        LEFT JOIN public.importer_chapter_mappings m ON (m.work_id = '${s.work_id}' AND m.chapter_sort_key = k.sort_key)
        LEFT JOIN public.chapters c ON (c.work_id = '${s.work_id}' AND c.number = k.sort_key)
        LEFT JOIN public.importer_queue q ON (
          q.task_type = 'IMPORT_CHAPTER'
          AND (q.payload->>'workId')::text = '${s.work_id}'
          AND q.chapter_sort_key = k.sort_key
        );
      `;
      const keyAudit = await runSql(auditKeysSql);
      console.table(keyAudit);
    }
  }

  console.log('\n========================================================');
  console.log('FORENSICS AUDIT COMPLETED');
  console.log('========================================================');
}

main().catch(console.error);
