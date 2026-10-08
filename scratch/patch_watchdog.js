const fs = require('fs');
const file = 'src/core/auto-heal-watchdog.ts';
let content = fs.readFileSync(file, 'utf8');

const targetStr = `      SELECT 
        COUNT(CASE WHEN status IN ('QUEUED', 'RETRY') AND (next_run_at IS NULL OR next_run_at <= NOW()) AND task_type = 'IMPORT_CHAPTER' THEN 1 END) as eligible_cnt,
        COUNT(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing_cnt,
        COUNT(CASE WHEN status = 'RETRY' THEN 1 END) as retry_cnt
      FROM importer_queue
      WHERE status IN ('QUEUED', 'RETRY', 'IMPORTING')`;

const replaceStr = `      SELECT 
        COUNT(CASE WHEN q.status IN ('QUEUED', 'RETRY') AND (q.next_run_at IS NULL OR q.next_run_at <= NOW()) AND q.task_type = 'IMPORT_CHAPTER' THEN 1 END) as eligible_cnt,
        COUNT(CASE WHEN q.status = 'IMPORTING' THEN 1 END) as importing_cnt,
        COUNT(CASE WHEN q.status = 'RETRY' THEN 1 END) as retry_cnt
      FROM importer_queue q
      LEFT JOIN importer_sources s ON q.source = s.id
      WHERE q.status IN ('QUEUED', 'RETRY', 'IMPORTING')
        AND (
          q.status = 'IMPORTING' 
          OR s.id IS NULL 
          OR (
            s.enabled = true AND (
              (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
              OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL) AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW()))
            )
          )
        )`;

if (content.includes(targetStr)) {
  content = content.replace(targetStr, replaceStr);
  fs.writeFileSync(file, content);
  console.log("Patched successfully");
} else {
  console.log("Could not find target string");
}
