import { getConfig } from '../src/config.js';
import { createClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../src/sources/registry.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

async function main() {
  const cfg = getConfig();
  const sb = createClient(cfg.SUPABASE_URL, cfg.SUPABASE_SERVICE_ROLE_KEY);
  const reg = new SourceRegistry(new HostRateLimiter(2.0));
  const allAdapters = reg.getAll();

  console.log(`Upserting ${allAdapters.length} active sources into importer_sources...`);

  for (const a of allAdapters) {
    const isBlockedPolicy = a.id === 'hanamiheaven' || a.id === 'kuro';
    const payload = {
      id: a.id,
      name: a.name,
      base_url: a.baseUrl,
      status: isBlockedPolicy ? 'UPSTREAM_BLOCKED' : 'ACTIVE',
      enabled: !isBlockedPolicy,
      updated_at: new Date().toISOString(),
    };

    const { error } = await sb.from('importer_sources').upsert(payload, { onConflict: 'id' });
    if (error) {
      console.error(`  ✗ Error upserting ${a.id}:`, error.message);
    } else {
      console.log(`  ✓ Upserted ${a.id.padEnd(20)}: ${payload.status} (enabled=${payload.enabled})`);
    }
  }

  // Ensure policy-excluded remain EXCLUDED_BY_POLICY
  await sb.from('importer_sources').upsert([
    { id: 'nexus_toons', name: 'Nexus Toons', base_url: 'https://nx-toons.xyz', status: 'EXCLUDED_BY_POLICY', enabled: false, updated_at: new Date().toISOString() },
    { id: 'toonlivre', name: 'Toon Livre', base_url: 'https://toonlivre.net', status: 'EXCLUDED_BY_POLICY', enabled: false, updated_at: new Date().toISOString() }
  ], { onConflict: 'id' });

  // Query final counts
  const { data: allSources } = await sb.from('importer_sources').select('id, name, status, enabled').order('status');
  console.log('\n=============================================');
  console.log(`Total sources in DB: ${allSources?.length}`);
  const byStatus: Record<string, number> = {};
  allSources?.forEach((s: any) => {
    byStatus[s.status] = (byStatus[s.status] || 0) + 1;
  });
  console.log('Breakdown by status:', byStatus);
  console.log('=============================================');
}

main().catch(console.error);
