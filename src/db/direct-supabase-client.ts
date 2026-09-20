import type pg from 'pg';
import {
  getYugabytePool,
  acquireJobsDirect,
  heartbeatDirect,
  failBatchDirect,
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
        const state = barrierSetting.rows[0]?.value || 'CLOSED';
        if (state === 'OPEN') {
          return {
            data: [{ can_publish: true, reason: 'SAFETY_BARRIER_OPEN', blocking_count: 0, blocking_sort_keys: [] }],
            error: null,
          };
        }

        const res = await this.pool.query(`
          SELECT
            CASE
              WHEN COUNT(*) = 0 THEN TRUE
              ELSE FALSE
            END AS can_publish,
            COUNT(*)::int AS blocking_count,
            COALESCE(ARRAY_AGG(chapter_sort_key), ARRAY[]::numeric[]) AS blocking_sort_keys,
            CASE
              WHEN COUNT(*) = 0 THEN 'BARRIER_CLEAR'
              ELSE 'UNPUBLISHED_PRECEDING_CHAPTERS'
            END AS reason
          FROM importer_chapter_mappings
          WHERE work_id = $1::uuid
            AND chapter_sort_key < $2::numeric
            AND status != 'COMPLETED'
            AND is_gap IS NOT TRUE
        `, [args.p_work_id, args.p_target_sort_key]);

        return { data: res.rows || [], error: null };
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
          source: args.p_source,
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
        await failBatchDirect([{
          jobId: args.p_job_id,
          status: args.p_status || 'RETRY',
          error: args.p_error,
          retryDelaySeconds: args.p_retry_delay_seconds,
        }]);
        return { data: true, error: null };
      }

      if (fn === 'importer_recover_stalled_leases') {
        const res = await this.pool.query(`
          UPDATE importer_queue
          SET status = 'RETRY',
              locked_by = NULL,
              locked_at = NULL,
              lease_expires_at = NULL,
              next_run_at = NOW(),
              updated_at = NOW()
          WHERE status = 'IMPORTING'
            AND lease_expires_at <= NOW()
          RETURNING id;
        `);
        return { data: { recovered_count: res.rowCount || 0 }, error: null };
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
