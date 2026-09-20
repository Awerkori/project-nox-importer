import { getYugabytePool, acquireJobsDirect, heartbeatDirect, failBatchDirect, recoverStalledLeasesDirect, } from './yugabyte-direct.js';
import { QueryBuilder, } from '../core/gateway-supabase.js';
import { Logger } from '../core/logger.js';
export class DirectSupabaseClient {
    logger = new Logger('DirectSupabase');
    pool;
    constructor(pool) {
        this.pool = pool || getYugabytePool();
    }
    from(table) {
        return new QueryBuilder(this, table);
    }
    async sql(query, params = []) {
        const res = await this.pool.query(query, params);
        return {
            rows: res.rows || [],
            rowCount: res.rowCount ?? (res.rows ? res.rows.length : 0),
        };
    }
    async rpc(fn, args = {}) {
        try {
            if (fn === 'importer_check_publication_barrier') {
                const barrierSetting = await this.pool.query("SELECT value FROM settings WHERE key = 'publication_safety_barrier' LIMIT 1");
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
                const res = await this.pool.query('SELECT importer_replace_pages($1::uuid, $2::jsonb)', [args.p_chapter_id, JSON.stringify(args.p_pages)]);
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
        }
        catch (err) {
            this.logger.error(`Error executing direct RPC ${fn}`, { error: err?.message });
            return { data: null, error: { message: err?.message || `RPC ${fn} failed` } };
        }
    }
}
