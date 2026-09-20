export class QueryBuilder {
    client;
    table;
    op = 'SELECT';
    selectedCols = '*';
    insertData = null;
    updateData = null;
    onConflict = null;
    filters = [];
    orderCol = null;
    orderAsc = true;
    limitCount = null;
    isSingle = false;
    isMaybeSingle = false;
    constructor(client, table) {
        this.client = client;
        this.table = table;
    }
    select(columns = '*') {
        this.op = 'SELECT';
        this.selectedCols = columns;
        return this;
    }
    insert(values) {
        this.op = 'INSERT';
        this.insertData = values;
        return this;
    }
    update(values) {
        this.op = 'UPDATE';
        this.updateData = values;
        return this;
    }
    upsert(values, options) {
        this.op = 'UPSERT';
        this.insertData = values;
        this.onConflict = options?.onConflict || null;
        return this;
    }
    delete() {
        this.op = 'DELETE';
        return this;
    }
    eq(col, val) {
        this.filters.push({ col, op: '=', val });
        return this;
    }
    neq(col, val) {
        this.filters.push({ col, op: '!=', val });
        return this;
    }
    gt(col, val) {
        this.filters.push({ col, op: '>', val });
        return this;
    }
    gte(col, val) {
        this.filters.push({ col, op: '>=', val });
        return this;
    }
    lt(col, val) {
        this.filters.push({ col, op: '<', val });
        return this;
    }
    lte(col, val) {
        this.filters.push({ col, op: '<=', val });
        return this;
    }
    in(col, vals) {
        this.filters.push({ col, op: 'IN', val: vals });
        return this;
    }
    is(col, val) {
        this.filters.push({ col, op: val === null ? 'IS NULL' : 'IS NOT NULL', val });
        return this;
    }
    ilike(col, pattern) {
        this.filters.push({ col, op: 'ILIKE', val: pattern });
        return this;
    }
    like(col, pattern) {
        this.filters.push({ col, op: 'LIKE', val: pattern });
        return this;
    }
    overlaps(col, vals) {
        this.filters.push({ col, op: '&&', val: vals });
        return this;
    }
    order(col, options) {
        this.orderCol = col;
        this.orderAsc = options?.ascending ?? true;
        return this;
    }
    not(col, op, val) {
        if (op === 'is' && val === null) {
            this.filters.push({ col, op: 'IS NOT NULL', val });
        }
        else if (op === 'in') {
            this.filters.push({ col, op: 'NOT IN', val });
        }
        else {
            this.filters.push({ col, op: '!=', val });
        }
        return this;
    }
    limit(count) {
        this.limitCount = count;
        return this;
    }
    single() {
        this.isSingle = true;
        this.limitCount = 1;
        return this;
    }
    maybeSingle() {
        this.isMaybeSingle = true;
        this.limitCount = 1;
        return this;
    }
    isJsonCol(c) {
        return c === 'metadata' || c === 'payload' || c === 'metadata_provenance' || c === 'blocked_details' || c === 'config' || c === 'progress_details' || c === 'stages';
    }
    serializeVal(c, val) {
        if (this.isJsonCol(c) && typeof val === 'object' && val !== null) {
            return JSON.stringify(val);
        }
        return val;
    }
    buildSql() {
        const params = [];
        let pIdx = 1;
        const buildWhere = (prefix = '') => {
            if (this.filters.length === 0)
                return '';
            const clauses = [];
            for (const f of this.filters) {
                const colRef = prefix ? `${prefix}."${f.col}"` : `"${f.col}"`;
                if (f.op === 'IS NULL' || f.op === 'IS NOT NULL') {
                    clauses.push(`${colRef} ${f.op}`);
                }
                else if (f.op === 'IN') {
                    if (Array.isArray(f.val) && f.val.length === 0) {
                        clauses.push('FALSE');
                    }
                    else {
                        params.push(f.val);
                        clauses.push(`${colRef} = ANY($${pIdx++})`);
                    }
                }
                else if (f.op === 'NOT IN') {
                    if (Array.isArray(f.val) && f.val.length === 0) {
                        clauses.push('TRUE');
                    }
                    else {
                        params.push(f.val);
                        clauses.push(`NOT (${colRef} = ANY($${pIdx++}))`);
                    }
                }
                else if (f.op === '&&') {
                    params.push(f.val);
                    clauses.push(`${colRef} && $${pIdx++}`);
                }
                else {
                    params.push(f.val);
                    clauses.push(`${colRef} ${f.op} $${pIdx++}`);
                }
            }
            return ` WHERE ${clauses.join(' AND ')}`;
        };
        if (this.op === 'SELECT') {
            if (this.table === 'importer_work_mappings' && this.selectedCols.includes('works!inner')) {
                let query = `SELECT m.id, m.work_id, m.source, m.source_work_id, m.updated_at, json_build_object('id', w.id, 'title', w.title, 'slug', w.slug, 'published', w.published, 'updated_at', w.updated_at) AS works FROM "importer_work_mappings" m INNER JOIN "works" w ON m.work_id = w.id${buildWhere('m')}`;
                if (this.orderCol) {
                    query += ` ORDER BY m."${this.orderCol}" ${this.orderAsc ? 'ASC' : 'DESC'}`;
                }
                if (this.limitCount !== null) {
                    query += ` LIMIT ${this.limitCount}`;
                }
                return { sql: query, params };
            }
            let query = `SELECT ${this.selectedCols} FROM "${this.table}"${buildWhere()}`;
            if (this.orderCol) {
                query += ` ORDER BY "${this.orderCol}" ${this.orderAsc ? 'ASC' : 'DESC'}`;
            }
            if (this.limitCount !== null) {
                query += ` LIMIT ${this.limitCount}`;
            }
            return { sql: query, params };
        }
        if (this.op === 'INSERT') {
            const rows = Array.isArray(this.insertData) ? this.insertData : [this.insertData];
            if (rows.length === 0)
                return { sql: 'SELECT 1 WHERE FALSE', params: [] };
            const cols = Object.keys(rows[0]);
            const valueTuples = [];
            for (const row of rows) {
                const tupleCols = [];
                for (const c of cols) {
                    const val = this.serializeVal(c, row[c]);
                    params.push(val);
                    if (this.isJsonCol(c)) {
                        tupleCols.push(`$${pIdx++}::jsonb`);
                    }
                    else if (c === 'aliases') {
                        tupleCols.push(`$${pIdx++}::text[]`);
                    }
                    else {
                        tupleCols.push(`$${pIdx++}`);
                    }
                }
                valueTuples.push(`(${tupleCols.join(', ')})`);
            }
            let conflictClause = '';
            if (this.onConflict) {
                conflictClause = ` ON CONFLICT (${this.onConflict.split(',').map(c => `"${c.trim()}"`).join(', ')}) DO NOTHING`;
            }
            const query = `INSERT INTO "${this.table}" (${cols.map(c => `"${c}"`).join(', ')}) VALUES ${valueTuples.join(', ')}${conflictClause} RETURNING *`;
            return { sql: query, params };
        }
        if (this.op === 'UPSERT') {
            const rows = Array.isArray(this.insertData) ? this.insertData : [this.insertData];
            if (rows.length === 0)
                return { sql: 'SELECT 1 WHERE FALSE', params: [] };
            const cols = Object.keys(rows[0]);
            const valueTuples = [];
            for (const row of rows) {
                const tupleCols = [];
                for (const c of cols) {
                    const val = this.serializeVal(c, row[c]);
                    params.push(val);
                    if (this.isJsonCol(c)) {
                        tupleCols.push(`$${pIdx++}::jsonb`);
                    }
                    else if (c === 'aliases') {
                        tupleCols.push(`$${pIdx++}::text[]`);
                    }
                    else {
                        tupleCols.push(`$${pIdx++}`);
                    }
                }
                valueTuples.push(`(${tupleCols.join(', ')})`);
            }
            let conflictClause = '';
            if (this.onConflict) {
                const conflictCols = this.onConflict.split(',').map(c => `"${c.trim()}"`).join(', ');
                const updateCols = cols
                    .filter(c => !this.onConflict.split(',').map(s => s.trim()).includes(c))
                    .map(c => `"${c}" = EXCLUDED."${c}"`)
                    .join(', ');
                conflictClause = ` ON CONFLICT (${conflictCols}) DO UPDATE SET ${updateCols || 'updated_at = NOW()'}`;
            }
            const query = `INSERT INTO "${this.table}" (${cols.map(c => `"${c}"`).join(', ')}) VALUES ${valueTuples.join(', ')}${conflictClause} RETURNING *`;
            return { sql: query, params };
        }
        if (this.op === 'UPDATE') {
            const cols = Object.keys(this.updateData);
            const setClauses = [];
            for (const c of cols) {
                const val = this.serializeVal(c, this.updateData[c]);
                params.push(val);
                if (this.isJsonCol(c)) {
                    setClauses.push(`"${c}" = $${pIdx++}::jsonb`);
                }
                else if (c === 'aliases') {
                    setClauses.push(`"${c}" = $${pIdx++}::text[]`);
                }
                else {
                    setClauses.push(`"${c}" = $${pIdx++}`);
                }
            }
            const where = buildWhere();
            const query = `UPDATE "${this.table}" SET ${setClauses.join(', ')}${where} RETURNING *`;
            return { sql: query, params };
        }
        if (this.op === 'DELETE') {
            const where = buildWhere();
            const query = `DELETE FROM "${this.table}"${where} RETURNING *`;
            return { sql: query, params };
        }
        throw new Error(`Unsupported operation: ${this.op}`);
    }
    async execute() {
        try {
            const { sql, params } = this.buildSql();
            const res = await this.client.sql(sql, params);
            const rows = res.rows || [];
            if (this.isSingle) {
                if (rows.length === 0) {
                    return { data: null, error: { message: 'Row not found', code: 'PGRST116' }, count: 0 };
                }
                return { data: rows[0], error: null, count: 1 };
            }
            if (this.isMaybeSingle) {
                return { data: (rows[0] ?? null), error: null, count: rows.length };
            }
            return { data: rows, error: null, count: rows.length };
        }
        catch (err) {
            const isDup = err?.code === '23505' || err?.message?.includes('duplicate key');
            return { data: null, error: { message: err?.message || 'Database execution error', code: isDup ? '23505' : err?.code }, count: 0 };
        }
    }
    then(onfulfilled, onrejected) {
        return this.execute().then(onfulfilled, onrejected);
    }
}
export class GatewaySupabaseClient {
    gateway;
    constructor(gateway) {
        this.gateway = gateway;
    }
    from(table) {
        return new QueryBuilder(this.gateway, table);
    }
    async rpc(fn, args = {}) {
        try {
            // 1. Check for specific RPCs
            if (fn === 'importer_check_publication_barrier') {
                const state = await this.gateway.getSafetyBarrier();
                if (state === 'OPEN') {
                    return { data: [{ can_publish: true, reason: 'SAFETY_BARRIER_OPEN', blocking_count: 0, blocking_sort_keys: [] }], error: null };
                }
                // Query YugabyteDB Aeon directly for publication barrier
                const res = await this.gateway.sql(`
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
                const jobs = await this.gateway.acquireJobs({
                    workerId: args.p_worker_id,
                    source: args.p_source,
                    taskType: args.p_task_type,
                    batchSize: args.p_batch_size || 1,
                });
                return { data: jobs, error: null };
            }
            if (fn === 'importer_renew_lease') {
                const updates = await this.gateway.heartbeat(args.p_worker_id, [{
                        jobId: args.p_job_id,
                        leaseDurationMinutes: args.p_lease_minutes || 5,
                    }]);
                return { data: updates.length > 0 ? updates[0].renewed : false, error: null };
            }
            if (fn === 'importer_release_job') {
                await this.gateway.failBatch(args.p_worker_id, [{
                        jobId: args.p_job_id,
                        status: args.p_status || 'RETRY',
                        error: args.p_error,
                    }]);
                return { data: true, error: null };
            }
            if (fn === 'importer_recover_stalled_leases') {
                const count = await this.gateway.recoverStalled();
                return { data: { recovered_count: count }, error: null };
            }
            // Default: invoke RPC as SQL function
            const paramKeys = Object.keys(args);
            const placeholders = paramKeys.map((_, i) => `$${i + 1}`).join(', ');
            const sqlQuery = `SELECT * FROM ${fn}(${placeholders})`;
            const res = await this.gateway.sql(sqlQuery, paramKeys.map(k => args[k]));
            return { data: res.rows, error: null };
        }
        catch (err) {
            return { data: null, error: { message: err?.message || `RPC ${fn} failed` } };
        }
    }
}
