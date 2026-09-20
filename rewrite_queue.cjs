const fs = require('fs');

let code = fs.readFileSync('src/core/queue.ts', 'utf8');

// Imports
code = code.replace("import type { SupabaseClient } from '@supabase/supabase-js';", 
`import { db, schema } from '../db/index.js';
import { eq, and, or, lt, lte, desc, asc, inArray, sql } from 'drizzle-orm';`);

// Constructor
code = code.replace("private supabase: SupabaseClient;", "");
code = code.replace("this.supabase = supabase;", "");
code = code.replace("supabase: SupabaseClient,", "");

// enqueue
code = code.replace(/const { error } = await this\.supabase\.from\('importer_queue'\)\.insert\(insertRow\);/,
`const { error } = await db.insert(schema.importerQueue).values(insertRow as any).then(() => ({ error: null })).catch((error) => ({ error }));`);

code = code.replace(/const { data: existing } = await this\.supabase\s*\.from\('importer_queue'\)\s*\.select\('id, status, priority'\)\s*\.eq\('dedupe_key', dedupeKey\)\s*\.maybeSingle\(\);/m,
`const existing = await db.select({ id: schema.importerQueue.id, status: schema.importerQueue.status, priority: schema.importerQueue.priority }).from(schema.importerQueue).where(eq(schema.importerQueue.dedupeKey, dedupeKey)).limit(1).then(r => r[0]);`);

code = code.replace(/const { error: revErr } = await this\.supabase\s*\.from\('importer_queue'\)\s*\.update\({[^}]*}\)\s*\.eq\('id', existing\.id\);/m,
`const { error: revErr } = await db.update(schema.importerQueue).set({ priority: Math.max(existing.priority, priority) }).where(eq(schema.importerQueue.id, existing.id)).then(() => ({ error: null })).catch((error) => ({ error }));`);

code = code.replace(/const { error } = await this\.supabase\s*\.from\('importer_queue'\)\s*\.update\({[^}]*}\)\s*\.eq\('id', jobId\);/m,
`const { error } = await db.update(schema.importerQueue).set({ next_run_at: new Date(Date.now() + minutes * 60000).toISOString() } as any).where(eq(schema.importerQueue.id, jobId)).then(() => ({ error: null })).catch((error) => ({ error }));`);

// acquire job
code = code.replace(/const { data, error } = await this\.supabase\.rpc\('importer_acquire_job', params\);/,
`// Custom Atomic Lock for Turso
    const { data, error } = await (async () => {
      try {
        const result = await db.$client.execute({
          sql: \`
            UPDATE importer_queue
            SET status = 'IMPORTING', locked_by = :workerId, locked_at = :now, lease_expires_at = :expiresAt, attempts = attempts + 1
            WHERE id = (
              SELECT q.id FROM importer_queue q
              LEFT JOIN settings s ON s.key = 'publication_safety_barrier'
              LEFT JOIN importer_staff_requests sr ON sr.status IN ('QUEUED', 'IMPORTING', 'RETRYING')
              WHERE (q.status = 'QUEUED' AND q.next_run_at <= :now)
                 OR (q.status = 'IMPORTING' AND q.lease_expires_at < :now)
                 -- Advanced filters
                 AND (:source IS NULL OR q.source = :source)
                 AND (:taskType IS NULL OR q.task_type = :taskType)
                 -- Barrier checks
                 AND (
                   s.value IS NULL 
                   OR json_extract(s.value, '$.open') = 1 
                   OR (q.task_type != 'SYNC_WORK' AND q.task_type != 'IMPORT_CHAPTER')
                 )
                 -- Focus mode
                 AND (
                   sr.id IS NULL 
                   OR (
                     q.payload LIKE '%' || sr.work_id || '%' 
                   )
                 )
              ORDER BY q.priority DESC, q.next_run_at ASC
              LIMIT 1
            )
            RETURNING *;
          \`,
          args: {
            workerId: this.workerId,
            now: new Date().toISOString(),
            expiresAt: new Date(Date.now() + 5 * 60000).toISOString(),
            source: params.p_source || null,
            taskType: params.p_task_type || null
          }
        });
        return { data: result.rows, error: null };
      } catch (e) {
        return { data: null, error: e };
      }
    })();
`);

code = code.replace(/const { data, error } = await this\.supabase\.rpc\('importer_renew_lease', {[^}]*}\);/m,
`const { data, error } = await (async () => {
      try {
        const result = await db.$client.execute({
          sql: "UPDATE importer_queue SET lease_expires_at = :expiresAt WHERE id = :jobId AND locked_by = :workerId RETURNING id",
          args: { jobId: params.job_id, workerId: this.workerId, expiresAt: new Date(Date.now() + 5 * 60000).toISOString() }
        });
        return { data: result.rows.length > 0 ? true : false, error: null };
      } catch(e) { return { data: null, error: e }; }
    })();`);

code = code.replace(/const { data, error } = await this\.supabase\.rpc\('importer_release_job', {[^}]*}\);/m,
`const { data, error } = await (async () => {
      try {
        const result = await db.$client.execute({
          sql: "UPDATE importer_queue SET status = :status, next_run_at = :nextRunAt, last_error = :err, locked_by = NULL, locked_at = NULL, lease_expires_at = NULL WHERE id = :jobId RETURNING id",
          args: { jobId: params.job_id, status: params.status, nextRunAt: params.next_run_at || new Date().toISOString(), err: params.error_msg || null }
        });
        return { data: true, error: null };
      } catch(e) { return { data: null, error: e }; }
    })();`);

// stalled leases
code = code.replace(/const { data: rpcRes, error: rpcErr } = await this\.supabase\.rpc\('importer_recover_stalled_leases'\);/,
`const { data: rpcRes, error: rpcErr } = await (async () => {
      try {
        const result = await db.$client.execute({
          sql: "UPDATE importer_queue SET status = 'QUEUED', locked_by = NULL, locked_at = NULL, lease_expires_at = NULL WHERE status = 'IMPORTING' AND lease_expires_at < :now RETURNING id",
          args: { now: new Date().toISOString() }
        });
        return { data: result.rows.length, error: null };
      } catch(e) { return { data: null, error: e }; }
    })();`);

code = code.replace(/const { data: stalled, error: fetchErr } = await this\.supabase\s*\.from\('importer_queue'\)\s*\.select\('id'\)\s*\.eq\('status', 'IMPORTING'\)\s*\.lt\('lease_expires_at', new Date\(\)\.toISOString\(\)\);/m,
`const stalled = await db.select({ id: schema.importerQueue.id }).from(schema.importerQueue).where(and(eq(schema.importerQueue.status, 'IMPORTING'), lt(schema.importerQueue.leaseExpiresAt, new Date().toISOString()))).then(r => r).catch(() => null);
const fetchErr = stalled ? null : new Error('fetch error');`);

code = code.replace(/const { error: requeueErr } = await this\.supabase\s*\.from\('importer_queue'\)\s*\.update\({[^}]*}\)\s*\.in\('id', ids\);/m,
`const { error: requeueErr } = await db.update(schema.importerQueue).set({ status: 'QUEUED', locked_by: null, locked_at: null, lease_expires_at: null } as any).where(inArray(schema.importerQueue.id, ids)).then(() => ({ error: null })).catch(error => ({ error }));`);

code = code.replace(/const { data, error } = await this\.supabase\s*\.from\('importer_queue'\)\s*\.select\('id, next_run_at'\)\s*\.in\('id', Array\.from\(this\.stalledLocks\.keys\(\)\)\);/m,
`const idsArr = Array.from(this.stalledLocks.keys());
    const res = idsArr.length > 0 ? await db.select({ id: schema.importerQueue.id, next_run_at: schema.importerQueue.nextRunAt }).from(schema.importerQueue).where(inArray(schema.importerQueue.id, idsArr)).then(d => ({ data: d, error: null })).catch(e => ({ data: null, error: e })) : { data: [], error: null };
    const { data, error } = res;`);

fs.writeFileSync('src/core/queue.ts', code);
console.log('Queue rewritten');
