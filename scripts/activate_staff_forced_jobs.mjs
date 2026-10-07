import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000,
  query_timeout: 15000,
});

async function run() {
  await client.connect();
  try {
    const magoId = '623c6884-749c-4ebf-94ea-3fe9f9fc501c';
    const imperadorId = '0f5cc9e2-8d56-4fae-af23-4e9301c3d19d';

    console.log('--- 1. UNPAUSING AND ELEVATING MAGO INFINITO JOBS ---');
    const magoRes = await client.query(`
      UPDATE importer_queue
      SET status = 'QUEUED',
          priority = 1090,
          payload = jsonb_set(
            jsonb_set(
              COALESCE(payload::jsonb, '{}'::jsonb),
              '{originalPriority}',
              CASE 
                WHEN (payload::jsonb->>'originalPriority') IS NOT NULL AND (payload::jsonb->>'originalPriority')::int < 1000
                  THEN (payload::jsonb->'originalPriority')
                ELSE to_jsonb(priority)
              END
            ),
            '{staffForced}',
            'true'::jsonb
          ),
          next_run_at = NOW(),
          updated_at = NOW()
      WHERE (payload->>'workId') = $1
        AND status IN ('PAUSED_BY_STAFF', 'PAUSED', 'QUEUED', 'RETRY')
      RETURNING id;
    `, [magoId]);
    console.log(`Updated ${magoRes.rowCount} jobs for Mago Infinito to QUEUED with priority=1090 and staffForced=true`);

    console.log('--- 2. ENQUEUING SYNC_WORK FOR O IMPERADOR ESTA GRAVIDO ---');
    const impRes = await client.query(`
      INSERT INTO importer_queue (
        id, task_type, source, dedupe_key, priority, status, payload, next_run_at, created_at, updated_at
      ) VALUES (
        gen_random_uuid(),
        'SYNC_WORK',
        'nebulosascan',
        'nebulosascan:sync:o-imperador-esta-gravido',
        1080,
        'QUEUED',
        jsonb_build_object(
          'workId', $1::text,
          'sourceWorkId', 'o-imperador-esta-gravido',
          'staffRequested', true,
          'staffForced', true,
          'workTitle', 'O imperador está grávido'
        ),
        NOW(),
        NOW(),
        NOW()
      ) ON CONFLICT (dedupe_key) DO UPDATE
      SET status = 'QUEUED',
          priority = 1080,
          payload = jsonb_set(
            jsonb_set(
              importer_queue.payload,
              '{staffForced}',
              'true'::jsonb
            ),
            '{staffRequested}',
            'true'::jsonb
          ),
          next_run_at = NOW(),
          updated_at = NOW()
      RETURNING id, status, priority;
    `, [imperadorId]);
    console.log('SYNC_WORK job for O imperador:', impRes.rows[0]);

    console.log('--- 3. CHECKING TOP 10 HIGHEST PRIORITY RUNNABLE JOBS IN QUEUE ---');
    const topJobs = await client.query(`
      SELECT q.id, q.source, q.task_type, q.priority, (q.payload->>'staffForced') as staff_forced,
             (q.payload->>'workId') as work_id, q.chapter_sort_key, q.status, q.next_run_at
      FROM importer_queue q
      WHERE (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
      ORDER BY 
        CASE 
          WHEN (q.payload->>'staffForced')::boolean = true OR q.priority >= 1000 THEN 0 
          ELSE 1 
        END ASC,
        q.priority DESC, 
        q.chapter_sort_key ASC NULLS LAST, 
        q.next_run_at ASC
      LIMIT 10;
    `);
    console.log(JSON.stringify(topJobs.rows, null, 2));

  } finally {
    await client.end();
  }
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
