const fs = require('fs');
let code = fs.readFileSync('src/core/auto-heal-watchdog.ts', 'utf8');

const oldPredecessorCheck = `AND NOT EXISTS (
              SELECT 1 FROM importer_queue predecessor
              WHERE predecessor.task_type = 'IMPORT_CHAPTER'
                AND predecessor.payload->>'workId' = q.payload->>'workId'
                AND predecessor.chapter_sort_key < q.chapter_sort_key
                AND predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')
                AND NOT EXISTS (
                  SELECT 1 FROM chapters predecessor_canonical
                  WHERE predecessor_canonical.work_id = (predecessor.payload->>'workId')::uuid
                    AND predecessor_canonical.published_at IS NOT NULL
                    AND predecessor_canonical.number = COALESCE(NULLIF(predecessor.payload->>'chapterNumber', '')::numeric, predecessor.chapter_sort_key)
                )
            )`;

const newPredecessorCheck = `AND (
              q.chapter_sort_key <= COALESCE((
                SELECT MAX(c.number) 
                FROM chapters c 
                WHERE c.work_id = (q.payload->>'workId')::uuid 
                  AND c.published_at IS NOT NULL
              ), -1) + 1
            )`;

code = code.replace(oldPredecessorCheck, newPredecessorCheck);
fs.writeFileSync('src/core/auto-heal-watchdog.ts', code);
console.log('Patched watchdog');
