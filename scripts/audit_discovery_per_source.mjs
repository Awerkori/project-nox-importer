import { getYugabytePool } from '../build/db/yugabyte-direct.js';

async function run() {
  const pool = getYugabytePool();
  try {
    const res = await pool.query(`
      SELECT 
        s.id as source_id,
        s.name,
        s.enabled,
        s.status,
        s.catalog_discovery_enabled,
        s.last_sync_at,
        s.cooldown_until,
        c.metadata,
        c.updated_at as checkpoint_updated_at,
        (
          SELECT count(*) 
          FROM importer_work_mappings m 
          WHERE m.source = s.id
        ) as candidates_found,
        (
          SELECT last_error 
          FROM importer_queue q 
          WHERE q.source = s.id AND q.task_type IN ('DISCOVER_WORKS', 'SYNC_WORK') AND q.last_error IS NOT NULL 
          ORDER BY q.updated_at DESC 
          LIMIT 1
        ) as recent_error
      FROM importer_sources s
      LEFT JOIN importer_checkpoints c ON c.source = s.id
      WHERE s.enabled = true
      ORDER BY s.id;
    `);

    const sourcesAudit = res.rows.map(r => ({
      SOURCE: `${r.name || r.source_id} (${r.source_id})`,
      DISCOVERY_ENABLED: (r.catalog_discovery_enabled && r.enabled && r.status === 'ACTIVE') ? 'YES' : 'NO',
      LAST_DISCOVERY: r.last_sync_at ? new Date(r.last_sync_at).toISOString() : (r.checkpoint_updated_at ? new Date(r.checkpoint_updated_at).toISOString() : 'NUNCA'),
      CANDIDATES_FOUND: Number(r.candidates_found || 0),
      ERROR: r.recent_error || 'Nenhum',
      COOLDOWN: r.cooldown_until ? (new Date(r.cooldown_until) > new Date() ? `ATÉ ${new Date(r.cooldown_until).toISOString()}` : 'EXPIRADO') : 'NÃO',
    }));

    console.log(JSON.stringify(sourcesAudit, null, 2));

  } catch (err) {
    console.error('Erro na auditoria por fonte:', err);
  } finally {
    await pool.end();
  }
}

run();
