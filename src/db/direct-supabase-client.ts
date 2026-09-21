import type pg from 'pg';
import {
  getYugabytePool,
  acquireJobsDirect,
  heartbeatDirect,
  failBatchDirect,
  recoverStalledLeasesDirect,
} from './yugabyte-direct.js';
import {
  QueryBuilder,
  type PostgrestResponse,
  type SqlClient,
} from '../core/gateway-supabase.js';
import { Logger } from '../core/logger.js';

export class DirectSupabaseClient implements SqlClient {
  private logger = new Logger('DirectSupabase');
  private pool: pg.Pool;

  constructor(pool?: pg.Pool) {
    this.pool = pool || getYugabytePool();
  }

  from<T = any>(table: string): QueryBuilder<T> {
    return new QueryBuilder<T>(this, table);
  }

  async sql<T = any>(query: string, params: any[] = []): Promise<{ rows: T[]; rowCount: number }> {
    const res = await this.pool.query(query, params);
    return {
      rows: res.rows || [],
      rowCount: res.rowCount ?? (res.rows ? res.rows.length : 0),
    };
  }

  async rpc(fn: string, args: Record<string, any> = {}): Promise<PostgrestResponse<any>> {
    try {
      if (fn === 'importer_check_publication_barrier') {
        const barrierSetting = await this.pool.query(
          "SELECT value FROM settings WHERE key = 'publication_safety_barrier' LIMIT 1"
        );
        const state = (barrierSetting.rows[0]?.value || 'CLOSED').toUpperCase();
        if (state === 'EMERGENCY_HALT') {
          return {
            data: [{ can_publish: false, reason: 'EMERGENCY_HALT_ACTIVE', blocking_count: 1, blocking_sort_keys: [] }],
            error: null,
          };
        }

        const targetSortKey = Number(args.p_target_sort_key);
        const workId = args.p_work_id;

        // 1. Get current highest published chapter in the canonical chapters table
        const pubRes = await this.pool.query(`
          SELECT COALESCE(MAX(number), -1) as max_published
          FROM chapters
          WHERE work_id = $1::uuid
            AND published_at IS NOT NULL
        `, [workId]);

        const maxPublished = parseFloat(pubRes.rows[0]?.max_published ?? '-1');

        // Case A: Initial publication (no published chapters yet)
        if (maxPublished < 0) {
          if (targetSortKey > 1.5) {
            // Check if initial chapter (1.0 or <= 1.5) exists and is pending
            const initCheck = await this.pool.query(`
              SELECT
                COUNT(CASE WHEN chapter_sort_key <= 1.5 AND is_gap IS NOT TRUE AND status != 'COMPLETED' THEN 1 END) as init_count,
                COUNT(CASE WHEN chapter_sort_key <= 1.5 AND is_gap IS TRUE THEN 1 END) as gap_count
              FROM importer_chapter_mappings
              WHERE work_id = $1::uuid
            `, [workId]);

            const initCount = parseInt(initCheck.rows[0]?.init_count || '0', 10);
            const gapCount = parseInt(initCheck.rows[0]?.gap_count || '0', 10);

            if (initCount > 0 && gapCount === 0) {
              return {
                data: [{
                  can_publish: false,
                  reason: 'WAITING_FOR_INITIAL_CHAPTERS',
                  blocking_count: 1,
                  blocking_sort_keys: [1.0],
                }],
                error: null,
              };
            }
          }

          return {
            data: [{
              can_publish: true,
              reason: 'INITIAL_CHAPTER_CLEAR',
              blocking_count: 0,
              blocking_sort_keys: [],
            }],
            error: null,
          };
        }

        // Case B: Backfill / Repair publication (targetSortKey <= maxPublished)
        // Backfills fill in past missing slots and must always be allowed to publish.
        if (targetSortKey <= maxPublished) {
          return {
            data: [{
              can_publish: true,
              reason: 'BACKFILL_CLEAR',
              blocking_count: 0,
              blocking_sort_keys: [],
            }],
            error: null,
          };
        }

        // Case C: Frontier advancement (targetSortKey > maxPublished)
        const step = targetSortKey - maxPublished;

        // C1: Normal sequential progression (step <= 1.05)
        if (step <= 1.05) {
          return {
            data: [{
              can_publish: true,
              reason: 'BARRIER_CLEAR',
              blocking_count: 0,
              blocking_sort_keys: [],
            }],
            error: null,
          };
        }

        // C2: Potential gap (step > 1.05). Check only intermediate chapters strictly between maxPublished and targetSortKey.
        const gapCheck = await this.pool.query(`
          SELECT DISTINCT m.chapter_sort_key
          FROM importer_chapter_mappings m
          WHERE m.work_id = $1::uuid
            AND m.chapter_sort_key > $2::numeric
            AND m.chapter_sort_key < $3::numeric
            AND m.is_gap IS NOT TRUE
            -- Exclude if already published in canonical chapters table
            AND NOT EXISTS (
              SELECT 1 FROM chapters c
              WHERE c.work_id = m.work_id
                AND c.number = m.chapter_sort_key
                AND c.published_at IS NOT NULL
            )
            -- Exclude if already completed by ANY provider mapping
            AND NOT EXISTS (
              SELECT 1 FROM importer_chapter_mappings m2
              WHERE m2.work_id = m.work_id
                AND m2.chapter_sort_key = m.chapter_sort_key
                AND m2.status = 'COMPLETED'
            )
          ORDER BY m.chapter_sort_key ASC
        `, [workId, maxPublished, targetSortKey]);

        if (gapCheck.rows.length > 0) {
          const blockingKeys = gapCheck.rows.map((r: any) => parseFloat(r.chapter_sort_key));
          return {
            data: [{
              can_publish: false,
              reason: 'UNPUBLISHED_PRECEDING_CHAPTERS',
              blocking_count: gapCheck.rows.length,
              blocking_sort_keys: blockingKeys,
            }],
            error: null,
          };
        }

        // If no uncompleted mapping rows exist between maxPublished and targetSortKey,
        // but step is large (e.g. integer skip with no mappings at all), check if an explicit gap is registered.
        if (step > 1.5) {
          const explicitGapCheck = await this.pool.query(`
            SELECT COUNT(*) as gap_count
            FROM importer_chapter_mappings
            WHERE work_id = $1::uuid
              AND chapter_sort_key > $2::numeric
              AND chapter_sort_key < $3::numeric
              AND is_gap IS TRUE
          `, [workId, maxPublished, targetSortKey]);

          const gapCount = parseInt(explicitGapCheck.rows[0]?.gap_count || '0', 10);
          if (gapCount === 0) {
            return {
              data: [{
                can_publish: false,
                reason: 'UNRESOLVED_GAP',
                blocking_count: Math.max(1, Math.floor(step) - 1),
                blocking_sort_keys: [],
              }],
              error: null,
            };
          }
        }

        return {
          data: [{
            can_publish: true,
            reason: 'BARRIER_CLEAR',
            blocking_count: 0,
            blocking_sort_keys: [],
          }],
          error: null,
        };
      }

      if (fn === 'importer_prune_telemetry') {
        return { data: true, error: null };
      }

      if (fn === 'importer_acquire_job') {
        const leaseMinutes = args.p_lease_duration
          ? parseInt(String(args.p_lease_duration).replace(/\D+/g, ''), 10) || 5
          : (args.p_lease_minutes || 5);
        const jobs = await acquireJobsDirect({
          workerId: args.p_worker_id,
          source: typeof args.p_source === 'string' ? args.p_source : undefined,
          allowedSources: args.p_allowed_sources || (Array.isArray(args.p_source) ? args.p_source : undefined),
          taskType: args.p_task_type,
          batchSize: args.p_batch_size || 1,
          leaseDurationMinutes: leaseMinutes,
        });
        return { data: jobs, error: null };
      }

      if (fn === 'importer_renew_lease') {
        const leaseMinutes = args.p_lease_duration
          ? parseInt(String(args.p_lease_duration).replace(/\D+/g, ''), 10) || 5
          : (args.p_lease_minutes || 5);
        const updates = await heartbeatDirect(args.p_worker_id, [{
          jobId: args.p_job_id,
          leaseDurationMinutes: leaseMinutes,
        }]);
        return { data: updates.length > 0 ? updates[0].renewed : false, error: null };
      }

      if (fn === 'importer_release_job') {
        const delaySeconds = typeof args.p_retry_delay_seconds === 'number'
          ? args.p_retry_delay_seconds
          : (args.p_retry_delay ? parseInt(String(args.p_retry_delay).replace(/\D+/g, ''), 10) : undefined);
        await failBatchDirect([{
          jobId: args.p_job_id,
          status: args.p_status || 'RETRY',
          error: args.p_error,
          retryDelaySeconds: delaySeconds,
          retryReason: args.p_retry_class || args.p_retry_reason,
          workerId: args.p_worker_id,
        }]);
        return { data: true, error: null };
      }

      if (fn === 'importer_recover_stalled_leases') {
        const res = await recoverStalledLeasesDirect();
        return { data: [{ recovered_count: res.recoveredCount, failed_count: res.failedCount }], error: null };
      }

      if (fn === 'importer_replace_pages') {
        const res = await this.pool.query(
          'SELECT importer_replace_pages($1::uuid, $2::jsonb)',
          [args.p_chapter_id, JSON.stringify(args.p_pages)]
        );
        return { data: res.rows, error: null };
      }

      // Default: invoke RPC as SQL function
      const paramKeys = Object.keys(args);
      const placeholders = paramKeys.map((_, i) => `$${i + 1}`).join(', ');
      const sqlQuery = `SELECT * FROM ${fn}(${placeholders})`;
      const params = paramKeys.map(k => {
        const val = args[k];
        if (typeof val === 'object' && val !== null) {
          return JSON.stringify(val);
        }
        return val;
      });
      const res = await this.pool.query(sqlQuery, params);
      return { data: res.rows, error: null };
    } catch (err: any) {
      this.logger.error(`Error executing direct RPC ${fn}`, { error: err?.message });
      return { data: null, error: { message: err?.message || `RPC ${fn} failed` } };
    }
  }
}
