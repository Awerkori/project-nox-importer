import { getYugabytePool } from '../build/db/yugabyte-direct.js';

// Global script safety timeout (45s)
const globalTimer = setTimeout(() => {
  console.error('❌ [TIMEOUT] Script global timeout atingido (45s). Abortando.');
  process.exit(1);
}, 45000);
globalTimer.unref();

async function run() {
  console.log('▶ [ETAPA 1/3] Obtendo pool e conectando com connectionTimeout = 10s...');
  const pool = getYugabytePool();

  try {
    console.log('▶ [ETAPA 2/3] Executando query pg_stat_activity com queryTimeout = 15s...');
    
    // Detailed connections breakdown
    const detailRes = await pool.query({
      text: `
        SELECT 
          pid,
          datname,
          usename,
          application_name,
          client_addr,
          backend_type,
          state,
          wait_event_type,
          wait_event,
          ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - query_start))::numeric, 2) AS query_duration_sec,
          ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - state_change))::numeric, 2) AS state_age_sec,
          LEFT(query, 80) AS query_snippet
        FROM pg_stat_activity
        ORDER BY state_change DESC NULLS LAST;
      `,
      statement_timeout: 15000
    });

    console.log(`✅ [SUCESSO] ${detailRes.rows.length} conexões encontradas no cluster.`);

    console.log('▶ [ETAPA 3/3] Calculando métricas consolidadas...');
    const summaryRes = await pool.query({
      text: `
        SELECT 
          count(*)::int AS total_cluster,
          count(*) FILTER (WHERE datname = current_database())::int AS total_project_nox,
          count(*) FILTER (WHERE datname = current_database() AND state = 'active')::int AS active_nox,
          count(*) FILTER (WHERE datname = current_database() AND state = 'idle')::int AS idle_nox,
          count(*) FILTER (WHERE datname = current_database() AND state = 'idle in transaction')::int AS idle_in_tx_nox,
          count(*) FILTER (WHERE datname != current_database() OR datname IS NULL)::int AS system_background
        FROM pg_stat_activity;
      `,
      statement_timeout: 15000
    });

    console.log('\n📊 === CONEXÕES CONSOLIDADAS ===');
    console.table(summaryRes.rows[0]);

    console.log('\n📋 === TODAS AS CONEXÕES DETALHADAS ===');
    console.table(detailRes.rows.map(r => ({
      pid: r.pid,
      datname: r.datname || '(null / system)',
      app: r.application_name || '(none)',
      client: r.client_addr || 'local/internal',
      type: r.backend_type,
      state: r.state || '(system)',
      state_sec: r.state_age_sec,
      query: r.query_snippet || '(idle)'
    })));

  } catch (err) {
    console.error('❌ [ERRO] Falha durante execução:', err.message || err);
    process.exit(1);
  } finally {
    console.log('⏹ [FINALLY] Encerrando pool de conexões...');
    try {
      await pool.end();
      console.log('✅ Pool finalizado com sucesso.');
    } catch (closeErr) {
      console.warn('Aviso ao fechar pool:', closeErr.message);
    }
  }

  process.exit(0);
}

run();
