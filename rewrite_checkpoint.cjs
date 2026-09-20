const fs = require('fs');
let code = fs.readFileSync('src/core/checkpoint.ts', 'utf8');

code = code.replace("import type { SupabaseClient } from '@supabase/supabase-js';",
`import { db, schema } from '../db/index.js';
import { eq, sql } from 'drizzle-orm';`);

code = code.replace("private supabase: SupabaseClient;", "");
code = code.replace("this.supabase = supabase;", "");
code = code.replace("supabase: SupabaseClient,", "");

code = code.replace(/const { data, error } = await this\.supabase\s*\.from\('importer_checkpoints'\)\s*\.select\('state, last_successful_run, sync_stats'\)\s*\.eq\('source', source\)\s*\.eq\('task_type', taskType\)\s*\.maybeSingle\(\);/m,
`const res = await db.select({ state: schema.importerCheckpoints.state, last_successful_run: schema.importerCheckpoints.lastSuccessfulRun, sync_stats: schema.importerCheckpoints.syncStats }).from(schema.importerCheckpoints).where(sql\`source = \${source} AND task_type = \${taskType}\`).limit(1).then(r => ({ data: r[0], error: null })).catch(error => ({ data: null, error }));
const { data, error } = res;`);

code = code.replace(/const { error } = await this\.supabase\s*\.from\('importer_checkpoints'\)\s*\.upsert\(\{\s*source,\s*task_type: taskType,\s*state,\s*last_successful_run: lastSuccessfulRun \? lastSuccessfulRun\.toISOString\(\) : null,\s*sync_stats: syncStats,\s*updated_at: new Date\(\)\.toISOString\(\)\s*\}\);/m,
`const { error } = await db.insert(schema.importerCheckpoints).values({
        source,
        taskType,
        state: state as any,
        lastSuccessfulRun: lastSuccessfulRun ? lastSuccessfulRun.toISOString() : null,
        syncStats: syncStats as any,
        updatedAt: new Date().toISOString()
      }).onConflictDoUpdate({
        target: [schema.importerCheckpoints.source, schema.importerCheckpoints.taskType],
        set: {
          state: state as any,
          lastSuccessfulRun: lastSuccessfulRun ? lastSuccessfulRun.toISOString() : null,
          syncStats: syncStats as any,
          updatedAt: new Date().toISOString()
        }
      }).then(() => ({ error: null })).catch(error => ({ error }));`);

fs.writeFileSync('src/core/checkpoint.ts', code);
