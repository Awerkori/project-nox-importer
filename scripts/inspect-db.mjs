import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function main() {
  const { data: sources, error: sErr } = await sb
    .from('importer_sources')
    .select('id, name, status, base_url, blocked_reason, blocked_details')
    .order('name');
    
  if (sErr) throw sErr;
  
  console.log(`TOTAL SOURCES IN DB: ${sources.length}`);
  const byStatus = {};
  for (const s of sources) {
    byStatus[s.status] = byStatus[s.status] || [];
    byStatus[s.status].push(s);
  }
  
  for (const [st, list] of Object.entries(byStatus)) {
    console.log(`\n=== STATUS: ${st} (${list.length}) ===`);
    for (const s of list) {
      console.log(`- ${s.id.padEnd(20)} | ${s.name.padEnd(25)} | ${s.base_url}`);
    }
  }

  // Count blocked_by_upstream jobs across whole queue
  const { count: totalBlocked, error: bErr } = await sb
    .from('importer_queue')
    .select('id', { count: 'exact', head: true })
    .eq('status', 'BLOCKED_BY_UPSTREAM');
    
  console.log(`\nTotal BLOCKED_BY_UPSTREAM in queue: ${totalBlocked}`);

  // Get all blocked jobs to see their sources
  let allBlocked = [];
  let page = 0;
  while (true) {
    const { data, error } = await sb
      .from('importer_queue')
      .select('id, source, status, last_error, updated_at')
      .eq('status', 'BLOCKED_BY_UPSTREAM')
      .range(page * 1000, (page + 1) * 1000 - 1);
    if (error) throw error;
    if (!data || data.length === 0) break;
    allBlocked.push(...data);
    if (data.length < 1000) break;
    page++;
  }

  console.log(`Fetched ${allBlocked.length} BLOCKED_BY_UPSTREAM records`);
  const countsBySource = {};
  for (const j of allBlocked) {
    countsBySource[j.source] = (countsBySource[j.source] || 0) + 1;
  }
  console.log('BLOCKED_BY_UPSTREAM counts by source:', countsBySource);

  // Also check all queue statuses total
  for (const st of ['QUEUED', 'IMPORTING', 'RETRY', 'BLOCKED_BY_UPSTREAM', 'PAUSED_BY_STAFF', 'CANCELLED_BY_STAFF', 'COMPLETED', 'FAILED', 'SUPERSEDED']) {
    const { count } = await sb.from('importer_queue').select('id', { count: 'exact', head: true }).eq('status', st);
    console.log(`Status ${st}: ${count}`);
  }
}

main().catch(console.error);
