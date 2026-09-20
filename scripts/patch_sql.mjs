import fs from 'fs';
let sql = fs.readFileSync('src/fix_rpc.ts', 'utf8');

// Replace the UNION ALL block to add the priority directly
sql = sql.replace(
  /SELECT q_cand.id, q_cand.task_type, q_cand.priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at\n\s*FROM staged_works sw/,
  `SELECT q_cand.id, q_cand.task_type, q_cand.priority + 5000 as priority, q_cand.payload, q_cand.chapter_sort_key, q_cand.created_at
        FROM staged_works sw`
);

// Remove the EXISTS check from ORDER BY
sql = sql.replace(
  /\+ CASE WHEN cand_batch\.task_type = 'IMPORT_CHAPTER'.*?THEN 5000 ELSE 0 END/s,
  ''
);

// Add #variable_conflict use_column to avoid the ambiguous column error
sql = sql.replace(
  /BEGIN\n  SELECT value INTO v_barrier_state/,
  `#variable_conflict use_column\nBEGIN\n  SELECT value INTO v_barrier_state`
);

fs.writeFileSync('src/fix_rpc.ts', sql);
