import { DirectSupabaseClient } from '../src/db/direct-supabase-client.js';
import { ImporterQueue } from '../src/core/queue.js';
import { ExistingWorksReconciler } from '../src/core/reconciliation.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import * as dotenv from 'dotenv';
dotenv.config();

async function main() {
  const supabase = new DirectSupabaseClient();
  const queue = new ImporterQueue(supabase as any);
  const rateLimiter = new HostRateLimiter(5);
  const registry = new SourceRegistry(rateLimiter, process.env.NOX_STORAGE_BRIDGE_TOKEN || '', process.env.NOX_MANGA_URL || '');
  const reconciler = new ExistingWorksReconciler(supabase as any, queue, registry, process.env.NOX_MANGA_URL);

  const { data } = await supabase.from('importer_work_health')
    .select('work_id')
    .eq('health_status', 'RECONCILING');

  if (data) {
    for (const d of data) {
      console.log(`Manually reconciling ${d.work_id}`);
      await reconciler.reconcileWorkManifest(d.work_id);
    }
  } else {
    console.log("No data returned");
  }
}
main().catch(console.error).then(() => process.exit(0));
