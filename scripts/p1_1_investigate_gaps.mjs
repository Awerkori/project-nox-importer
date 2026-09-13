import dotenv from 'dotenv';
dotenv.config();

const SUPABASE_MGMT_TOKEN = "sbp_42a1ea952d51ffacf6e1eb1413b8af39638aa244";
const SUPABASE_PROJECT_REF = "izregkwaqdygwioqzwwo";

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
  console.log("=== 1. INVESTIGATING THE 25 ORPHANED CHAPTERS (pages exist, published_at is null) ===");
  const orphanedSql = `
    SELECT
      c.id as chapter_id,
      c.work_id,
      w.title as work_title,
      c.number as chapter_number,
      c.published_at,
      count(*) as page_count,
      m.id as mapping_id,
      m.source as mapping_source,
      m.status as mapping_status,
      m.is_gap as mapping_is_gap
    FROM public.chapters c
    JOIN public.pages p ON p.chapter_id = c.id
    JOIN public.works w ON w.id = c.work_id
    LEFT JOIN public.importer_chapter_mappings m ON (m.work_id = c.work_id AND (m.chapter_id = c.id OR m.chapter_sort_key = c.number))
    WHERE c.published_at IS NULL
    GROUP BY c.id, c.work_id, w.title, c.number, c.published_at, m.id, m.source, m.status, m.is_gap
    ORDER BY w.title, c.number;
  `;
  const orphaned = await runSql(orphanedSql);
  console.table(orphaned);

  console.log("\n=== 2. INVESTIGATING Deus das Artes Marciais (9ca785a4-a6dd-489a-8819-5d51ee022ddd) ===");
  const martialGodSql = `
    SELECT
      m.id as mapping_id,
      m.chapter_number,
      m.chapter_sort_key,
      m.source,
      m.status as mapping_status,
      m.is_gap,
      c.id as chapter_id,
      c.published_at,
      count(p.page_number) as page_count
    FROM public.importer_chapter_mappings m
    LEFT JOIN public.chapters c ON (c.work_id = m.work_id AND (c.id = m.chapter_id OR c.number = m.chapter_sort_key))
    LEFT JOIN public.pages p ON p.chapter_id = c.id
    WHERE m.work_id = '9ca785a4-a6dd-489a-8819-5d51ee022ddd'
      AND m.chapter_sort_key BETWEEN 136 AND 140
    GROUP BY m.id, m.chapter_number, m.chapter_sort_key, m.source, m.status, m.is_gap, c.id, c.published_at
    ORDER BY m.chapter_sort_key ASC;
  `;
  const martialGod = await runSql(martialGodSql);
  console.table(martialGod);

  console.log("\n=== 3. INVESTIGATING Ascensao no Pico Marcial (e74aa68d-149f-400f-b65f-c2535e04845b) Chapter 101 ===");
  const martialPeakSql = `
    SELECT
      m.id as mapping_id,
      m.chapter_number,
      m.chapter_sort_key,
      m.source,
      m.status as mapping_status,
      m.is_gap,
      c.id as chapter_id,
      c.published_at,
      q.id as queue_id,
      q.status as queue_status,
      q.attempts,
      q.last_error
    FROM public.importer_chapter_mappings m
    LEFT JOIN public.chapters c ON (c.work_id = m.work_id AND (c.id = m.chapter_id OR c.number = m.chapter_sort_key))
    LEFT JOIN public.importer_queue q ON (
      q.task_type = 'IMPORT_CHAPTER'
      AND (q.payload->>'workId')::text = m.work_id::text
      AND q.chapter_sort_key = m.chapter_sort_key
    )
    WHERE m.work_id = 'e74aa68d-149f-400f-b65f-c2535e04845b'
      AND m.chapter_sort_key BETWEEN 99 AND 103
    ORDER BY m.chapter_sort_key ASC;
  `;
  const martialPeak = await runSql(martialPeakSql);
  console.table(martialPeak);

  console.log("\n=== 4. INVESTIGATING Imperador Magico (4b1f452c-b223-404c-83f4-e0d626563397) Chapter 2-3 / Nexus ===");
  const magicEmpSql = `
    SELECT
      m.id as mapping_id,
      m.chapter_number,
      m.chapter_sort_key,
      m.source,
      m.status as mapping_status,
      m.is_gap,
      c.id as chapter_id,
      c.published_at,
      q.id as queue_id,
      q.source as queue_source,
      q.status as queue_status,
      q.attempts,
      q.last_error
    FROM public.importer_chapter_mappings m
    LEFT JOIN public.chapters c ON (c.work_id = m.work_id AND (c.id = m.chapter_id OR c.number = m.chapter_sort_key))
    LEFT JOIN public.importer_queue q ON (
      q.task_type = 'IMPORT_CHAPTER'
      AND (q.payload->>'workId')::text = m.work_id::text
      AND q.chapter_sort_key = m.chapter_sort_key
    )
    WHERE m.work_id = '4b1f452c-b223-404c-83f4-e0d626563397'
      AND m.chapter_sort_key BETWEEN 1 AND 5
    ORDER BY m.chapter_sort_key ASC;
  `;
  const magicEmp = await runSql(magicEmpSql);
  console.table(magicEmp);
}

main().catch(console.error);
