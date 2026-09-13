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
  const topWorks = [
    { name: 'Deus das Artes Marciais', id: '9ca785a4-a6dd-489a-8819-5d51ee022ddd' },
    { name: 'Ascensão no Pico Marcial', id: 'e74aa68d-149f-400f-b65f-c2535e04845b' },
    { name: 'Imperador Mágico', id: '4b1f452c-b223-404c-83f4-e0d626563397' },
    { name: 'What Happens Inside the Dungeon', id: '20f19fd1-9d75-45f5-8bc8-03bdd0afb19b' },
    { name: 'Stop Smoking', id: '9290e0eb-fea1-4bea-a9f0-961d3cfd1df4' }
  ];

  for (const w of topWorks) {
    console.log(`\n======================================================`);
    console.log(`WORK: ${w.name} (${w.id})`);
    
    // Find the min staged chapter
    const minStagedSql = `
      SELECT chapter_number, chapter_sort_key, source, status, created_at
      FROM public.importer_chapter_mappings
      WHERE work_id = '${w.id}' AND status = 'STAGED'
      ORDER BY chapter_sort_key ASC
      LIMIT 1;
    `;
    const minStaged = await runSql(minStagedSql);
    if (!minStaged.length) {
      console.log("No staged chapters found!");
      continue;
    }
    const target = minStaged[0];
    console.log(`Earliest STAGED chapter: ${target.chapter_number} (sort: ${target.chapter_sort_key}) from ${target.source}`);

    // Call barrier RPC
    const barrierSql = `
      SELECT * FROM public.importer_check_publication_barrier('${w.id}'::uuid, ${target.chapter_sort_key}::numeric);
    `;
    const barrier = await runSql(barrierSql);
    console.log("Barrier Result:", JSON.stringify(barrier[0]));

    if (barrier[0]?.blocking_sort_keys?.length) {
      const keys = barrier[0].blocking_sort_keys.slice(0, 5);
      console.log(`Blocking keys (first ${keys.length}): ${keys.join(', ')}`);
      
      const inspectSql = `
        SELECT
          k.sort_key,
          m.id as mapping_id,
          m.source as mapping_source,
          m.status as mapping_status,
          m.is_gap,
          c.id as chapter_id,
          c.published_at,
          (SELECT count(*) FROM public.pages p WHERE p.chapter_id = c.id) as page_count,
          q.id as queue_id,
          q.source as queue_source,
          q.status as queue_status,
          q.attempts,
          q.last_error
        FROM (SELECT unnest(ARRAY[${keys.join(',')}]) as sort_key) k
        LEFT JOIN public.importer_chapter_mappings m ON (m.work_id = '${w.id}' AND coalesce(m.chapter_sort_key, m.chapter_number) = k.sort_key)
        LEFT JOIN public.chapters c ON (c.work_id = '${w.id}' AND c.number = k.sort_key)
        LEFT JOIN public.importer_queue q ON (
          q.task_type = 'IMPORT_CHAPTER'
          AND (q.payload->>'workId')::text = '${w.id}'
          AND coalesce(q.chapter_sort_key, (q.payload->>'chapterNumber')::numeric) = k.sort_key
        );
      `;
      const inspect = await runSql(inspectSql);
      console.table(inspect);
    }
  }
}

main().catch(console.error);
