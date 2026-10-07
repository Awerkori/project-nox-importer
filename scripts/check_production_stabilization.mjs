import pg from 'pg';
import fs from 'fs';

const { Pool } = pg;

// Read Yugabyte env credentials
const envPath = '/home/awerkori/.config/project-nox/yugabyte.env';
let envVars = {};
if (fs.existsSync(envPath)) {
  const content = fs.readFileSync(envPath, 'utf8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#') && trimmed.includes('=')) {
      const idx = trimmed.indexOf('=');
      const k = trimmed.substring(0, idx).trim();
      let v = trimmed.substring(idx + 1).trim();
      if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
        v = v.substring(1, v.length - 1);
      }
      envVars[k] = v;
    }
  }
}

const pool = new Pool({
  host: envVars.YUGABYTE_HOST || process.env.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || process.env.YUGABYTE_PORT || '5433', 10),
  user: envVars.YUGABYTE_USER || process.env.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD || process.env.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE || process.env.YUGABYTE_DATABASE,
  ssl: {
    rejectUnauthorized: false
  },
  max: 2,
  connectionTimeoutMillis: 10000,
  statement_timeout: 15000
});

async function main() {
  console.log('=== PROJECT NOX PRODUCTION STABILIZATION AUDIT ===\n');

  try {
    // 1. Check connections
    const connRes = await pool.query(`
      SELECT 
        count(*) as total_conns,
        count(*) FILTER (WHERE state = 'active') as active,
        count(*) FILTER (WHERE state = 'idle') as idle,
        count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
      FROM pg_stat_activity 
      WHERE datname = current_database();
    `);
    console.log('📊 YUGABYTE YSQL CONNECTIONS:');
    console.log(`  Total: ${connRes.rows[0].total_conns} / Limit: 13 (Tripwire: 12)`);
    console.log(`  Active: ${connRes.rows[0].active}`);
    console.log(`  Idle: ${connRes.rows[0].idle}`);
    console.log(`  Idle in Tx: ${connRes.rows[0].idle_in_tx}\n`);

    // 2. Connections breakdown
    const appRes = await pool.query(`
      SELECT application_name, client_addr, state, count(*) as count
      FROM pg_stat_activity
      WHERE datname = current_database()
      GROUP BY application_name, client_addr, state
      ORDER BY count DESC;
    `);
    console.log('📋 CONNECTIONS BREAKDOWN:');
    for (const r of appRes.rows) {
      console.log(`  ${r.application_name || '[unnamed]'} (${r.client_addr || 'local'}) [${r.state}]: ${r.count}`);
    }
    console.log();

    // 3. Staged Chapters
    const stagedRes = await pool.query(`
      SELECT count(*) as staged_count
      FROM importer_chapter_mappings
      WHERE status = 'STAGED';
    `);
    console.log(`📦 STAGED CHAPTERS BACKLOG: ${stagedRes.rows[0].staged_count}`);

    // 4. Publication throughput (last 15m, 30m, 60m)
    const pubRes = await pool.query(`
      SELECT 
        count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '15 minutes') as pub_15m,
        count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '30 minutes') as pub_30m,
        count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '60 minutes') as pub_60m
      FROM chapters;
    `);
    const p15 = parseInt(pubRes.rows[0].pub_15m, 10);
    const p30 = parseInt(pubRes.rows[0].pub_30m, 10);
    const p60 = parseInt(pubRes.rows[0].pub_60m, 10);
    console.log('⚡ PUBLICATION THROUGHPUT:');
    console.log(`  Last 15m: ${p15} chapters (${(p15 / 15).toFixed(2)} ch/min)`);
    console.log(`  Last 30m: ${p30} chapters (${(p30 / 30).toFixed(2)} ch/min)`);
    console.log(`  Last 60m: ${p60} chapters (${(p60 / 60).toFixed(2)} ch/min)\n`);

    // 5. Queue Status
    const qRes = await pool.query(`
      SELECT status, count(*) as count
      FROM importer_queue
      GROUP BY status
      ORDER BY count DESC;
    `);
    console.log('📥 QUEUE BREAKDOWN:');
    for (const r of qRes.rows) {
      console.log(`  ${r.status}: ${r.count}`);
    }
    console.log();

    // 6. STAFF_FORCED Active Priorities
    const prioRes = await pool.query(`
      SELECT r.id, r.work_id, w.title, r.status, r.priority_boost, r.reason
      FROM importer_staff_requests r
      JOIN works w ON r.work_id = w.id
      WHERE r.status = 'ACTIVE'
      ORDER BY r.created_at DESC;
    `);
    console.log(`🎯 ACTIVE STAFF_FORCED PRIORITIES (${prioRes.rows.length}):`);
    for (const r of prioRes.rows) {
      console.log(`  • ${r.title} (ID: ${r.work_id}) - Boost: ${r.priority_boost} - Reason: ${r.reason || 'N/A'}`);
    }
    console.log();

    // 7. Test Route Latencies (Worker Production)
    console.log('🌐 MEASURING ROUTE LATENCIES (https://manga.project-nox-awerkori.workers.dev):');
    const routes = [
      { name: 'Home (/)', path: '/' },
      { name: 'Obra Reader Details (/obra/mago-infinito)', path: '/obra/mago-infinito' },
      { name: 'Ranking (/ranking)', path: '/ranking' },
      { name: 'Catalogo (/catalogo)', path: '/catalogo' },
      { name: 'Admin Importer (/admin/importer)', path: '/admin/importer' },
      { name: 'Admin Prioridades (/admin/importer/prioridades)', path: '/admin/importer/prioridades' },
      { name: 'Admin Erros (/admin/importer/erros)', path: '/admin/importer/erros' },
      { name: 'Me (/me)', path: '/me' }
    ];

    for (const route of routes) {
      const times = [];
      let status = 0;
      for (let i = 0; i < 3; i++) {
        const start = performance.now();
        const res = await fetch(`https://manga.project-nox-awerkori.workers.dev${route.path}`, {
          headers: {
            'User-Agent': 'NoxAuditor/1.0',
            'Accept': 'text/html,application/json'
          },
          redirect: 'manual'
        });
        const elapsed = performance.now() - start;
        times.push(elapsed);
        status = res.status;
      }
      times.sort((a, b) => a - b);
      const p50 = times[1].toFixed(1);
      console.log(`  ${route.name.padEnd(52)} -> HTTP ${status} | P50: ${p50}ms (min: ${times[0].toFixed(1)}ms, max: ${times[2].toFixed(1)}ms)`);
    }

  } catch (err) {
    console.error('Audit failed with error:', err);
  } finally {
    await pool.end();
    console.log('\nAudit complete. Database pool closed.');
  }
}

main().catch((e) => {
  console.error('Fatal:', e);
  process.exit(1);
});
