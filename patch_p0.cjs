const fs = require('fs');
let code = fs.readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');

const p0QueryOld = `
    // 2. NO_P0_WAITING (Real P0 releases: 100 <= priority < 1000; staff-forced >= 1000 belongs to chapter data plane)
    const p0Res = await this.runQuery(\`
      SELECT COUNT(*) as p0_cnt
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND status IN ('QUEUED', 'RETRY')
        AND priority >= 100 AND priority < 1000
    \`);
    const p0Waiting = parseInt(p0Res.rows[0]?.p0_cnt || '0', 10);
    if (p0Waiting > 0 && activeWorksCount > 0) {
`;

const p0QueryNew = `
    // 2. NO_P0_WAITING (Real P0 releases: 100 <= priority < 1000; staff-forced >= 1000 belongs to chapter data plane)
    // We must use a frontier/eligibility check so that works blocked by upstream gaps
    // do not falsely register as "waiting" and starve P2 admission indefinitely.
    const p0Res = await this.runQuery(\`
      SELECT q.id
      FROM importer_queue q
      JOIN importer_sources s ON s.id = q.source
      CROSS JOIN LATERAL (
        SELECT MAX(c.number) AS max_published
        FROM chapters c
        WHERE c.work_id = (q.payload->>'workId')::uuid
          AND c.published_at IS NOT NULL
      ) pub
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        AND q.priority >= 100 AND q.priority < 1000
        AND s.enabled = true
        AND \${SOURCE_EXECUTION_ELIGIBILITY_SQL}
        AND NOT EXISTS (
          SELECT 1 FROM importer_queue predecessor
          WHERE predecessor.task_type = 'IMPORT_CHAPTER'
            AND predecessor.payload->>'workId' = q.payload->>'workId'
            AND predecessor.chapter_sort_key < q.chapter_sort_key
            AND predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')
            AND NOT EXISTS (
              SELECT 1 FROM chapters predecessor_canonical
              WHERE predecessor_canonical.work_id = (q.payload->>'workId')::uuid
                AND predecessor_canonical.published_at IS NOT NULL
                AND (predecessor_canonical.number = NULLIF(predecessor.payload->>'chapterNumber', '')::numeric OR predecessor_canonical.number = predecessor.chapter_sort_key)
            )
        )
        AND (
          (pub.max_published IS NOT NULL AND q.chapter_sort_key <= pub.max_published + 1.5)
          OR (pub.max_published IS NOT NULL AND EXISTS (
              SELECT 1 FROM importer_confirmed_gaps gap
              WHERE gap.work_id = (q.payload->>'workId')::uuid AND gap.start_sort_key <= pub.max_published + 1 AND gap.end_sort_key >= q.chapter_sort_key - 1
            )
          )
          OR (pub.max_published IS NULL AND q.chapter_sort_key <= 1.5 AND NOT EXISTS (
              SELECT 1 FROM importer_chapter_mappings predecessor_mapping
              WHERE predecessor_mapping.work_id = (q.payload->>'workId')::uuid AND predecessor_mapping.chapter_sort_key < q.chapter_sort_key AND predecessor_mapping.is_gap = false AND predecessor_mapping.status NOT IN ('STAGED', 'WAITING_FOR_GAP')
            )
          )
        )
        AND NOT EXISTS (
          SELECT 1 FROM chapters canonical_chapter
          WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
            AND canonical_chapter.published_at IS NOT NULL
            AND (canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key))
        )
      LIMIT 1
    \`);
    const p0Waiting = p0Res.rows.length > 0 ? 1 : 0;
    if (p0Waiting > 0 && activeWorksCount > 0) {
`;

if (code.includes(p0QueryOld.trim().split('\n')[0])) {
    code = code.replace(
      // We'll replace lines from the NO_P0_WAITING comment down to `if (p0Waiting > 0 && activeWorksCount > 0) {`
      /\/\/ 2\. NO_P0_WAITING[\s\S]*?if \(p0Waiting > 0 && activeWorksCount > 0\) \{/,
      p0QueryNew.trim() + ' {'
    );
    fs.writeFileSync('src/core/scheduler/admission-controller.ts', code);
    console.log('Patched admission-controller.ts');
} else {
    console.log('Could not find P0_WAITING block');
}
