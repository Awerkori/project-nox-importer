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
                const state = (barrierSetting.rows[0]?.value || 'CLOSED').toUpperCase();
                if (state === 'EMERGENCY_HALT') {
                    return {
                        data: [{ can_publish: false, reason: 'EMERGENCY_HALT_ACTIVE', blocking_count: 1, blocking_sort_keys: [] }],
                        error: null,
                    };
                }
                const targetSortKey = Number(args.p_target_sort_key);
                const workId = args.p_work_id;
                // 1. Check for any preceding incomplete chapters in mappings for this work
                const precRes = await this.pool.query(`
          SELECT chapter_sort_key
          FROM importer_chapter_mappings
          WHERE work_id = $1::uuid
            AND chapter_sort_key < $2::numeric
            AND status != 'COMPLETED'
            AND is_gap IS NOT TRUE
          ORDER BY chapter_sort_key ASC
        `, [workId, targetSortKey]);
                if (precRes.rows.length > 0) {
                    const blockingKeys = precRes.rows.map((r) => parseFloat(r.chapter_sort_key));
                    return {
                        data: [{
                                can_publish: false,
                                reason: 'UNPUBLISHED_PRECEDING_CHAPTERS',
                                blocking_count: precRes.rows.length,
                                blocking_sort_keys: blockingKeys,
                            }],
                        error: null,
                    };
                }
                // 2. Continuity & Gap Check against published chapters
                const pubRes = await this.pool.query(`
          SELECT COALESCE(MAX(number), -1) as max_published
          FROM chapters
          WHERE work_id = $1::uuid
            AND published_at IS NOT NULL
        `, [workId]);
                const maxPublished = parseFloat(pubRes.rows[0]?.max_published ?? '-1');
                if (maxPublished < 0) {
                    // No published chapters yet: only initial chapters (<= 1.5) or explicit gaps are allowed
                    if (targetSortKey > 1.5) {
                        const initCheck = await this.pool.query(`
              SELECT
                COUNT(CASE WHEN chapter_sort_key <= 1.5 AND is_gap IS NOT TRUE THEN 1 END) as init_count,
                COUNT(CASE WHEN chapter_sort_key < $2::numeric AND is_gap IS TRUE THEN 1 END) as gap_count
              FROM importer_chapter_mappings
              WHERE work_id = $1::uuid
            `, [workId, targetSortKey]);
                        const initCount = parseInt(initCheck.rows[0]?.init_count || '0', 10);
                        const gapCount = parseInt(initCheck.rows[0]?.gap_count || '0', 10);
                        if (initCount > 0 || gapCount === 0) {
                            return {
                                data: [{
                                        can_publish: false,
                                        reason: initCount > 0 ? 'WAITING_FOR_INITIAL_CHAPTERS' : 'UNRESOLVED_GAP',
                                        blocking_count: 1,
                                        blocking_sort_keys: [1.0],
                                    }],
                                error: null,
                            };
                        }
                    }
                }
                else if (targetSortKey > maxPublished) {
                    const step = targetSortKey - maxPublished;
                    if (step > 1.5) {
                        // Step anomaly: check if intermediate chapters exist and if gaps are registered
                        const gapCheck = await this.pool.query(`
              SELECT
                COUNT(CASE WHEN is_gap IS NOT TRUE AND status != 'COMPLETED' THEN 1 END) as pending_intermediate,
                COUNT(CASE WHEN is_gap IS TRUE THEN 1 END) as gap_count
              FROM importer_chapter_mappings
              WHERE work_id = $1::uuid
                AND chapter_sort_key > $2::numeric
                AND chapter_sort_key < $3::numeric
            `, [workId, maxPublished, targetSortKey]);
                        const pendingIntermediate = parseInt(gapCheck.rows[0]?.pending_intermediate || '0', 10);
                        const gapCount = parseInt(gapCheck.rows[0]?.gap_count || '0', 10);
                        if (pendingIntermediate > 0 || gapCount === 0) {
                            return {
                                data: [{
                                        can_publish: false,
                                        reason: pendingIntermediate > 0 ? 'UNPUBLISHED_PRECEDING_CHAPTERS' : 'UNRESOLVED_GAP',
                                        blocking_count: pendingIntermediate > 0 ? pendingIntermediate : Math.max(1, Math.floor(step) - 1),
                                        blocking_sort_keys: [],
                                    }],
                                error: null,
                            };
                        }
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
