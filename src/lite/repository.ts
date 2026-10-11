import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient, QueryResultRow } from 'pg';
import { LITE_PRIORITY_RANK, type LitePriority, type LiteTask, type LiteTaskState, type LiteTaskType } from './types.js';

type Db = Pick<Pool, 'query' | 'connect'>;
type Row = QueryResultRow & Record<string, any>;

const priorityName = (value: number): LitePriority => (['STAFF', 'P0', 'P1', 'P2'] as LitePriority[])[value] || 'P2';
const asTask = (row: Row): LiteTask => ({
  id: row.id, taskType: row.task_type, workId: row.work_id, workKey: row.work_key,
  chapterKey: row.chapter_key, priority: priorityName(row.priority), basePriority: priorityName(row.base_priority),
  state: row.state, idempotencyKey: row.idempotency_key, nextRunAt: new Date(row.next_run_at),
  attempts: row.attempts, maxAttempts: row.max_attempts, leaseOwner: row.lease_owner,
  leaseUntil: row.lease_until ? new Date(row.lease_until) : null, reason: row.reason,
  payload: row.payload || {}, checkpoint: row.checkpoint || {},
});

export interface EnqueueLiteTask {
  taskType: LiteTaskType;
  workId?: string | null;
  workKey: string;
  chapterKey?: string | null;
  priority: Exclude<LitePriority, 'STAFF'>;
  idempotencyKey: string;
  nextRunAt?: Date;
  maxAttempts?: number;
  payload?: Record<string, unknown>;
  checkpoint?: Record<string, unknown>;
}

/** Direct YSQL persistence. Network, adapters and Telegram are never invoked here. */
export class LiteRepository {
  constructor(private readonly db: Db) {}

  private async refreshReadyWork(client: Pick<PoolClient, 'query'>, workKey: string): Promise<void> {
    await client.query('SELECT public.importer_lite_refresh_ready_work($1)', [workKey]);
  }

  async enqueue(input: EnqueueLiteTask): Promise<{ inserted: boolean; task: LiteTask | null }> {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      // STAFF remains an overlay. A newly-materialized task inherits an
      // already-active staff request rather than waiting for another event.
      const staff = input.workId ? await client.query(
        `SELECT 1 FROM importer_staff_requests
         WHERE work_id=$1::uuid AND status IN ('QUEUED','IMPORTING','RETRYING','ACTIVE') LIMIT 1`, [input.workId],
      ) : { rowCount: 0 };
      const priority = staff.rowCount ? 0 : LITE_PRIORITY_RANK[input.priority];
      const result = await client.query<Row>(
        `INSERT INTO importer_lite_tasks
          (task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,next_run_at,max_attempts,payload,checkpoint)
         VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb)
         ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`,
        [input.taskType, input.workId || null, input.workKey, input.chapterKey || null, priority,
          LITE_PRIORITY_RANK[input.priority], input.idempotencyKey, input.nextRunAt || new Date(),
          input.maxAttempts || 8, JSON.stringify(input.payload || {}), JSON.stringify(input.checkpoint || {})],
      );
      await this.refreshReadyWork(client, input.workKey);
      await client.query('COMMIT');
      return { inserted: Boolean(result.rowCount), task: result.rows[0] ? asTask(result.rows[0]) : null };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally { client.release(); }
  }

  async claim(workerId: string, leaseSeconds: number): Promise<LiteTask | null> {
    const result = await this.db.query<Row>(
      'SELECT * FROM public.importer_lite_claim_task($1,$2::interval)',
      [workerId, `${Math.max(30, leaseSeconds)} seconds`],
    );
    return result.rows[0] ? asTask(result.rows[0]) : null;
  }

  async checkpoint(task: LiteTask, workerId: string, patch: Record<string, unknown>): Promise<boolean> {
    const result = await this.db.query(
      `UPDATE importer_lite_tasks SET checkpoint=checkpoint || $3::jsonb, updated_at=now()
       WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2 AND lease_until>now()`,
      [task.id, workerId, JSON.stringify(patch)],
    );
    return result.rowCount === 1;
  }

  async finish(task: LiteTask, workerId: string, outcome: {
    state: Extract<LiteTaskState, 'DONE' | 'RETRY_WAIT' | 'BLOCKED'>;
    reason?: string | null;
    nextRunAt?: Date;
    checkpoint?: Record<string, unknown>;
    priority?: Exclude<LitePriority, 'STAFF'>;
  }): Promise<boolean> {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      const result = await client.query(
        `UPDATE importer_lite_tasks
          SET state=$3, reason=$4, next_run_at=COALESCE($5::timestamptz,next_run_at),
              base_priority=COALESCE($7::smallint,base_priority),
              priority=CASE WHEN priority=0 THEN 0 ELSE COALESCE($7::smallint,priority) END,
              checkpoint=checkpoint || $6::jsonb, lease_owner=NULL, lease_until=NULL, updated_at=now()
         WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2`,
        [task.id, workerId, outcome.state, outcome.reason || null, outcome.nextRunAt || null, JSON.stringify(outcome.checkpoint || {}), outcome.priority === undefined ? null : LITE_PRIORITY_RANK[outcome.priority]],
      );
      await this.refreshReadyWork(client, task.workKey);
      await client.query('COMMIT');
      return result.rowCount === 1;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally { client.release(); }
  }

  /** One bounded recovery turn; it cannot consume the claim path or scan works. */
  async reconcile(limit = 50): Promise<{ leases: number; retries: number; publishable: number }> {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      const leases = await client.query<Row>(
        `WITH picked AS (
           SELECT id,work_key FROM importer_lite_tasks WHERE state='RUNNING' AND lease_until<now()
           ORDER BY lease_until LIMIT $1 FOR UPDATE SKIP LOCKED
         ) UPDATE importer_lite_tasks t SET state='RETRY_WAIT', lease_owner=NULL, lease_until=NULL,
           next_run_at=now() + interval '30 seconds', reason='LEASE_EXPIRED', updated_at=now()
         FROM picked WHERE t.id=picked.id RETURNING picked.work_key`, [limit],
      );
      const retries = await client.query<Row>(
        `WITH picked AS (
           SELECT id,work_key FROM importer_lite_tasks WHERE state='RETRY_WAIT' AND next_run_at<=now()
           ORDER BY next_run_at LIMIT $1 FOR UPDATE SKIP LOCKED
         ) UPDATE importer_lite_tasks t SET state='READY', updated_at=now()
         FROM picked WHERE t.id=picked.id RETURNING picked.work_key`, [limit],
      );
      // A completed media checkpoint never waits for a global sweep: it gains
      // one durable PUBLISH task. The unique key makes this idempotent.
      const ready = await client.query<Row>(
        `SELECT id,work_id,work_key,chapter_key,priority,base_priority FROM importer_lite_tasks
          WHERE state='DONE' AND task_type='IMPORT_CHAPTER'
            AND checkpoint ? 'mediaIds' AND NOT checkpoint ? 'publishedAt'
          ORDER BY updated_at LIMIT $1`, [limit],
      );
      for (const row of ready.rows) {
        await client.query(
          `INSERT INTO importer_lite_tasks(task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key,payload,checkpoint)
           VALUES ('PUBLISH',$1,$2,$3,$4,$5,'lite:publish:'||$1::text||':'||$3,$6::jsonb,$7::jsonb)
           ON CONFLICT(idempotency_key) DO NOTHING`,
          [row.work_id, row.work_key, row.chapter_key, row.priority, row.base_priority,
            JSON.stringify({ importTaskId: row.id }), JSON.stringify({})],
        );
      }
      for (const key of new Set([...leases.rows, ...retries.rows, ...ready.rows].map((row) => row.work_key))) {
        await this.refreshReadyWork(client, key);
      }
      await client.query('COMMIT');
      return { leases: leases.rowCount || 0, retries: retries.rowCount || 0, publishable: ready.rowCount || 0 };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally { client.release(); }
  }

  async operationalSnapshot(): Promise<Array<{ priority: LitePriority; state: LiteTaskState; count: number; oldestReadyAt: Date | null }>> {
    const result = await this.db.query<Row>(`SELECT priority,state,count(*)::int AS count,
      min(created_at) FILTER (WHERE state='READY') AS oldest_ready_at
      FROM importer_lite_tasks GROUP BY priority,state ORDER BY priority,state`);
    return result.rows.map((row) => ({ priority: priorityName(row.priority), state: row.state,
      count: Number(row.count), oldestReadyAt: row.oldest_ready_at ? new Date(row.oldest_ready_at) : null }));
  }

  /** Stable ID is reserved before the non-transactional Telegram call. */
  async reserveUpload(taskId: string, pagePosition: number, sha256: string): Promise<{ mediaId: string; state: string }> {
    const reusable = await this.db.query<Row>(`SELECT id FROM media WHERE sha256=$1 AND storage_ready=true
      AND bytes>0 AND mime LIKE 'image/%' ORDER BY created_at LIMIT 1`, [sha256]);
    if (reusable.rows[0]?.id) {
      const reused = await this.db.query<Row>(
        `INSERT INTO importer_lite_uploads(task_id,page_position,media_id,sha256,state)
         VALUES($1::uuid,$2,$3::uuid,$4,'PERSISTED')
         ON CONFLICT(task_id,page_position) DO UPDATE SET updated_at=now()
         RETURNING media_id,state`, [taskId,pagePosition,reusable.rows[0].id,sha256],
      );
      return { mediaId: reused.rows[0].media_id, state: reused.rows[0].state };
    }
    const id = randomUUID();
    const result = await this.db.query<Row>(
      `INSERT INTO importer_lite_uploads(task_id,page_position,media_id,sha256)
       VALUES($1::uuid,$2,$3::uuid,$4)
       ON CONFLICT(task_id,page_position) DO UPDATE SET updated_at=now()
       RETURNING media_id,state`, [taskId, pagePosition, id, sha256],
    );
    return { mediaId: result.rows[0].media_id, state: result.rows[0].state };
  }

  async recordUploadReceipt(input: {
    taskId: string; pagePosition: number; mediaId: string; sha256: string; bytes: number; mime: string; width: number; height: number;
    providerKey: string; botReference: string | null; shardId: string | null; channelId: string | null; createdBy: string;
  }): Promise<void> {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO media(id,provider,provider_key,bot_reference,storage_shard_id,mime,width,height,bytes,sha256,created_by,storage_ready,purpose)
         SELECT $1::uuid,'telegram',$2,$3,$4,$5,$6,$7,$8,$9,$10::uuid,true,'chapter'
         WHERE NOT EXISTS (SELECT 1 FROM media WHERE id=$1::uuid)`,
        [input.mediaId, input.providerKey, input.botReference, input.shardId, input.mime, input.width, input.height, input.bytes, input.sha256, input.createdBy],
      );
      await client.query(
        `UPDATE importer_lite_uploads SET state='PERSISTED',provider_key=$4,bot_reference=$5,storage_shard_id=$6,
           channel_id=$7,bytes=$8,mime=$9,updated_at=now()
         WHERE task_id=$1::uuid AND page_position=$2 AND media_id=$3::uuid`,
        [input.taskId,input.pagePosition,input.mediaId,input.providerKey,input.botReference,input.shardId,input.channelId,input.bytes,input.mime],
      );
      await client.query('COMMIT');
    } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; } finally { client.release(); }
  }

  async markUploadUnknown(taskId: string, position: number): Promise<void> {
    await this.db.query(`UPDATE importer_lite_uploads SET state='UNKNOWN',updated_at=now()
      WHERE task_id=$1::uuid AND page_position=$2 AND state IN ('INTENT','UPLOADED')`, [taskId, position]).catch(() => {});
  }

  async publish(input: { task: LiteTask; workerId: string; chapterId: string; workId: string; mediaIds: string[]; fresh: boolean }): Promise<boolean> {
    const client = await this.db.connect();
    try {
      await client.query('BEGIN');
      const presentation = await client.query<Row>(`SELECT w.cover_id,w.title,w.slug,m.storage_ready,m.bytes
        FROM works w LEFT JOIN media m ON m.id=w.cover_id WHERE w.id=$1::uuid FOR UPDATE`, [input.workId]);
      const work = presentation.rows[0];
      if (!work?.title || !work?.slug || !work?.cover_id || work.storage_ready !== true || Number(work.bytes) < 1500) {
        throw new Error('WORK_PRESENTATION_INCOMPLETE');
      }
      const integrity = await client.query<{ count: string }>(
        `SELECT count(*) FROM media WHERE id=ANY($1::uuid[]) AND storage_ready=true AND bytes>0 AND mime LIKE 'image/%'`, [input.mediaIds],
      );
      if (Number(integrity.rows[0]?.count) !== input.mediaIds.length || input.mediaIds.length === 0) {
        throw new Error('PUBLISH_MEDIA_INCOMPLETE');
      }
      const pageRows = input.mediaIds.map((mediaId, index) => ({ media_id: mediaId, position: index + 1 }));
      await client.query('SELECT public.importer_replace_pages($1::uuid,$2::jsonb)', [input.chapterId, JSON.stringify(pageRows)]);
      const visible = await client.query<Row>(
        `UPDATE chapters SET published_at=clock_timestamp(),is_fresh_release=$2
          WHERE id=$1::uuid AND published_at IS NULL RETURNING published_at`, [input.chapterId,input.fresh],
      );
      const publishedAt = visible.rows[0]?.published_at || null;
      if (publishedAt) {
        await client.query(`INSERT INTO importer_publication_events(chapter_id,transition_at,recorded_at,bucket_minute,is_fresh_release)
          VALUES($1::uuid,$2,clock_timestamp(),date_trunc('minute',$2),$3) ON CONFLICT(chapter_id) DO NOTHING`, [input.chapterId,publishedAt,input.fresh]);
        await client.query(`UPDATE works SET published=true,latest_chapter_published_at=GREATEST(COALESCE(latest_chapter_published_at,$2),$2),updated_at=now() WHERE id=$1::uuid`, [input.workId,publishedAt]);
      }
      const done = await client.query(`UPDATE importer_lite_tasks SET state='DONE',lease_owner=NULL,lease_until=NULL,
        checkpoint=checkpoint || jsonb_build_object('publishedAt',COALESCE($3::text,checkpoint->>'publishedAt')),updated_at=now()
        WHERE id=$1::uuid AND state='RUNNING' AND lease_owner=$2`, [input.task.id,input.workerId,publishedAt]);
      await this.refreshReadyWork(client, input.task.workKey);
      await client.query('COMMIT');
      return done.rowCount === 1;
    } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; } finally { client.release(); }
  }
}
