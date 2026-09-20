import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function check() {
  console.log("=== DB CPU & CONNECTIONS ===");
  try {
    // Check pg_stat_activity directly if possible
    const { data: activity, error } = await sb.from('pg_stat_activity').select('*');
    if (error) {
       console.log("Cannot select pg_stat_activity:", error.message);
    } else {
       console.log(`Active connections: ${activity?.length}`);
       const waiting = activity?.filter(a => a.wait_event_type === 'Lock');
       console.log(`Waiting connections: ${waiting?.length}`);
    }
  } catch(e) {}
  
  console.log("=== RELEASES RPC ===");
  const start = Date.now();
  const { data: releases, error: relError } = await sb.rpc('get_recent_releases', { limit_val: 20 });
  const latency = Date.now() - start;
  console.log(`Latency: ${latency}ms, Error: ${relError?.message || 'none'}`);

  console.log("=== TELEMETRY ===");
  const { data: telemetry } = await sb.from('importer_telemetry').select('concurrency, active_jobs, rss_mb, heap_used_mb, event_loop_lag_ms').order('created_at', { ascending: false }).limit(2);
  console.log(telemetry);
}
check();
