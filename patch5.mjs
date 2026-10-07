import fs from 'fs';

let content = fs.readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');

// Add HAVING to periodic
content = content.replace(
  "          GROUP BY q.payload->>'workId', q.source\n         ),\n         queue_candidates AS MATERIALIZED",
  "          GROUP BY q.payload->>'workId', q.source\n           HAVING COUNT(*) FILTER (WHERE q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())) > 0\n         ),\n         queue_candidates AS MATERIALIZED"
);

// Add HAVING to on-demand
content = content.replace(
  "          GROUP BY q.payload->>'workId', q.source\n        ), p1_rotation AS MATERIALIZED",
  "          GROUP BY q.payload->>'workId', q.source\n          HAVING COUNT(*) FILTER (WHERE q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())) > 0\n        ), p1_rotation AS MATERIALIZED"
);

// Remove the JS checks
content = content.replace(/if \(parseInt\(cand\.queued_count \|\| '0', 10\) === 0\) return false;/g, '');
content = content.replace(/if \(parseInt\(cand\.queued_count \|\| '0', 10\) === 0\) continue;/g, '');

fs.writeFileSync('src/core/scheduler/admission-controller.ts', content, 'utf8');
console.log('Patch applied!');
