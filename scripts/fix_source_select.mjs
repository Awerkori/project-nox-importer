import fs from 'fs';
let sql = fs.readFileSync('scripts/fix_rpc_direct2.mjs', 'utf8');

// Replace SELECT lists in the UNION ALLs to include source
sql = sql.replace(/SELECT cand_batch\.id, cand_batch\.payload, cand_batch\.task_type, cand_batch\.chapter_sort_key, cand_batch\.priority, cand_batch\.created_at/g, 
  'SELECT cand_batch.id, cand_batch.payload, cand_batch.task_type, cand_batch.chapter_sort_key, cand_batch.priority, cand_batch.created_at, cand_batch.source');

sql = sql.replace(/SELECT q1\.id, q1\.task_type, q1\.priority, q1\.payload, q1\.chapter_sort_key, q1\.created_at/g,
  'SELECT q1.id, q1.task_type, q1.priority, q1.payload, q1.chapter_sort_key, q1.created_at, q1.source');

sql = sql.replace(/SELECT q2\.id, q2\.task_type, q2\.priority, q2\.payload, q2\.chapter_sort_key, q2\.created_at/g,
  'SELECT q2.id, q2.task_type, q2.priority, q2.payload, q2.chapter_sort_key, q2.created_at, q2.source');

sql = sql.replace(/SELECT q3\.id, q3\.task_type, q3\.priority, q3\.payload, q3\.chapter_sort_key, q3\.created_at/g,
  'SELECT q3.id, q3.task_type, q3.priority, q3.payload, q3.chapter_sort_key, q3.created_at, q3.source');

sql = sql.replace(/SELECT q4\.id, q4\.task_type, q4\.priority \+ 5000 as priority, q4\.payload, q4\.chapter_sort_key, q4\.created_at/g,
  'SELECT q4.id, q4.task_type, q4.priority + 5000 as priority, q4.payload, q4.chapter_sort_key, q4.created_at, q4.source');

fs.writeFileSync('scripts/fix_rpc_direct3.mjs', sql);
