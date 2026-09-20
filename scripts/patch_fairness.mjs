import fs from 'fs';
let sql = fs.readFileSync('scripts/fix_rpc_direct.mjs', 'utf8');

const workPenalty = `(
              SELECT count(*) * 1000
              FROM public.importer_queue active_q
              WHERE active_q.status = 'IMPORTING' 
                AND active_q.task_type = 'IMPORT_CHAPTER'
                AND (active_q.payload->>'workId')::text = (cand_batch.payload->>'workId')::text
            )`;

const sourcePenalty = `(
              SELECT count(*) * 1000
              FROM public.importer_queue active_q
              WHERE active_q.status = 'IMPORTING' 
                AND active_q.source = cand_batch.source
            )`;

sql = sql.replace(
  / - CASE WHEN cand_batch\.payload->>'workId' is not null THEN \([\s\S]*?\) ELSE 0 END/,
  ` - CASE WHEN cand_batch.payload->>'workId' is not null THEN ${workPenalty} ELSE 0 END
          - CASE WHEN cand_batch.source is not null THEN ${sourcePenalty} ELSE 0 END`
);

fs.writeFileSync('scripts/fix_rpc_direct2.mjs', sql);
