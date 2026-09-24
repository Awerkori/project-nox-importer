import { ImporterGatewayClient } from './gateway-client.js';

export interface PostgrestResponse<T = any> {
  data: T | null;
  error: { message: string; code?: string; details?: string } | null;
  count?: number | null;
}

export interface SqlClient {
  sql<T = any>(query: string, params?: any[]): Promise<{ rows: T[]; rowCount?: number }>;
}

export class QueryBuilder<T = any> implements PromiseLike<PostgrestResponse<T>> {
  private op: 'SELECT' | 'INSERT' | 'UPDATE' | 'UPSERT' | 'DELETE' = 'SELECT';
  private selectedCols: string = '*';
  private insertData: any = null;
  private updateData: any = null;
  private onConflict: string | null = null;
  private filters: Array<{ col: string; op: string; val: any }> = [];
  private orderCol: string | null = null;
  private orderAsc: boolean = true;
  private limitCount: number | null = null;
  private isSingle: boolean = false;
  private isMaybeSingle: boolean = false;

  constructor(
    private client: SqlClient,
    private table: string
  ) {}

  select(columns: string = '*'): this {
    if (this.op !== 'INSERT' && this.op !== 'UPSERT' && this.op !== 'UPDATE' && this.op !== 'DELETE') {
      this.op = 'SELECT';
    }
    this.selectedCols = columns;
    return this;
  }

  insert(values: any): this {
    this.op = 'INSERT';
    this.insertData = values;
    return this;
  }

  update(values: any): this {
    this.op = 'UPDATE';
    this.updateData = values;
    return this;
  }

  upsert(values: any, options?: { onConflict?: string }): this {
    this.op = 'UPSERT';
    this.insertData = values;
    this.onConflict = options?.onConflict || null;
    return this;
  }

  delete(): this {
    this.op = 'DELETE';
    return this;
  }

  eq(col: string, val: any): this {
    this.filters.push({ col, op: '=', val });
    return this;
  }

  neq(col: string, val: any): this {
    this.filters.push({ col, op: '!=', val });
    return this;
  }

  gt(col: string, val: any): this {
    this.filters.push({ col, op: '>', val });
    return this;
  }

  gte(col: string, val: any): this {
    this.filters.push({ col, op: '>=', val });
    return this;
  }

  lt(col: string, val: any): this {
    this.filters.push({ col, op: '<', val });
    return this;
  }

  lte(col: string, val: any): this {
    this.filters.push({ col, op: '<=', val });
    return this;
  }

  in(col: string, vals: any[]): this {
    this.filters.push({ col, op: 'IN', val: vals });
    return this;
  }

  is(col: string, val: any): this {
    this.filters.push({ col, op: val === null ? 'IS NULL' : 'IS NOT NULL', val });
    return this;
  }

  ilike(col: string, pattern: string): this {
    this.filters.push({ col, op: 'ILIKE', val: pattern });
    return this;
  }

  like(col: string, pattern: string): this {
    this.filters.push({ col, op: 'LIKE', val: pattern });
    return this;
  }

  overlaps(col: string, vals: any[]): this {
    this.filters.push({ col, op: '&&', val: vals });
    return this;
  }

  order(col: string, options?: { ascending?: boolean }): this {
    this.orderCol = col;
    this.orderAsc = options?.ascending ?? true;
    return this;
  }

  not(col: string, op: string, val: any): this {
    if (op === 'is' && val === null) {
      this.filters.push({ col, op: 'IS NOT NULL', val });
    } else if (op === 'in') {
      this.filters.push({ col, op: 'NOT IN', val });
    } else {
      this.filters.push({ col, op: '!=', val });
    }
    return this;
  }

  limit(count: number): this {
    this.limitCount = count;
    return this;
  }

  single(): this {
    this.isSingle = true;
    this.limitCount = 1;
    return this;
  }

  maybeSingle(): this {
    this.isMaybeSingle = true;
    this.limitCount = 1;
    return this;
  }

  private isJsonCol(c: string): boolean {
    return c === 'metadata' || c === 'payload' || c === 'metadata_provenance' || c === 'blocked_details' || c === 'config' || c === 'progress_details' || c === 'stages';
  }

  private serializeVal(c: string, val: any): any {
    if (this.isJsonCol(c) && typeof val === 'object' && val !== null) {
      return JSON.stringify(val);
    }
    return val;
  }

  private buildSql(): { sql: string; params: any[] } {
    const params: any[] = [];
    let pIdx = 1;

    const buildWhere = (prefix = ''): string => {
      if (this.filters.length === 0) return '';
      const clauses: string[] = [];
      for (const f of this.filters) {
        const colRef = prefix ? `${prefix}."${f.col}"` : `"${f.col}"`;
        if (f.op === 'IS NULL' || f.op === 'IS NOT NULL') {
          clauses.push(`${colRef} ${f.op}`);
        } else if (f.op === 'IN') {
          if (Array.isArray(f.val) && f.val.length === 0) {
            clauses.push('FALSE');
          } else {
            params.push(f.val);
            clauses.push(`${colRef} = ANY($${pIdx++})`);
          }
        } else if (f.op === 'NOT IN') {
          if (Array.isArray(f.val) && f.val.length === 0) {
            clauses.push('TRUE');
          } else {
            params.push(f.val);
            clauses.push(`NOT (${colRef} = ANY($${pIdx++}))`);
          }
        } else if (f.op === '&&') {
          params.push(f.val);
          clauses.push(`${colRef} && $${pIdx++}`);
        } else {
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
      if (rows.length === 0) return { sql: 'SELECT 1 WHERE FALSE', params: [] };

      const cols = Object.keys(rows[0]);
      const valueTuples: string[] = [];

      for (const row of rows) {
        const tupleCols: string[] = [];
        for (const c of cols) {
          const val = this.serializeVal(c, row[c]);
          params.push(val);
          if (this.isJsonCol(c)) {
            tupleCols.push(`$${pIdx++}::jsonb`);
          } else if (c === 'aliases') {
            tupleCols.push(`$${pIdx++}::text[]`);
          } else {
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
      if (rows.length === 0) return { sql: 'SELECT 1 WHERE FALSE', params: [] };

      const cols = Object.keys(rows[0]);
      const valueTuples: string[] = [];

      for (const row of rows) {
        const tupleCols: string[] = [];
        for (const c of cols) {
          const val = this.serializeVal(c, row[c]);
          params.push(val);
          if (this.isJsonCol(c)) {
            tupleCols.push(`$${pIdx++}::jsonb`);
          } else if (c === 'aliases') {
            tupleCols.push(`$${pIdx++}::text[]`);
          } else {
            tupleCols.push(`$${pIdx++}`);
          }
        }
        valueTuples.push(`(${tupleCols.join(', ')})`);
      }

      let conflictClause = '';
      if (this.onConflict) {
        const conflictCols = this.onConflict.split(',').map(c => `"${c.trim()}"`).join(', ');
        const updateCols = cols
          .filter(c => !this.onConflict!.split(',').map(s => s.trim()).includes(c))
          .map(c => `"${c}" = EXCLUDED."${c}"`)
          .join(', ');
        conflictClause = ` ON CONFLICT (${conflictCols}) DO UPDATE SET ${updateCols || 'updated_at = NOW()'}`;
      }

      const query = `INSERT INTO "${this.table}" (${cols.map(c => `"${c}"`).join(', ')}) VALUES ${valueTuples.join(', ')}${conflictClause} RETURNING *`;
      return { sql: query, params };
    }

    if (this.op === 'UPDATE') {
      const cols = Object.keys(this.updateData);
      const setClauses: string[] = [];
      for (const c of cols) {
        const val = this.serializeVal(c, this.updateData[c]);
        params.push(val);
        if (this.isJsonCol(c)) {
          setClauses.push(`"${c}" = $${pIdx++}::jsonb`);
        } else if (c === 'aliases') {
          setClauses.push(`"${c}" = $${pIdx++}::text[]`);
        } else {
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

  async execute(): Promise<PostgrestResponse<T>> {
    try {
      const { sql, params } = this.buildSql();
      const res = await this.client.sql<T>(sql, params);
      const rows = res.rows || [];

      if (this.isSingle) {
        if (rows.length === 0) {
          return { data: null, error: { message: 'Row not found', code: 'PGRST116' }, count: 0 };
        }
        return { data: rows[0] as any, error: null, count: 1 };
      }

      if (this.isMaybeSingle) {
        return { data: (rows[0] ?? null) as any, error: null, count: rows.length };
      }

      return { data: rows as any, error: null, count: rows.length };
    } catch (err: any) {
      const isDup = err?.code === '23505' || err?.message?.includes('duplicate key');
      return { data: null, error: { message: err?.message || 'Database execution error', code: isDup ? '23505' : err?.code }, count: 0 };
    }
  }

  then<TResult1 = PostgrestResponse<T>, TResult2 = never>(
    onfulfilled?: ((value: PostgrestResponse<T>) => TResult1 | PromiseLike<TResult1>) | undefined | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null
  ): Promise<TResult1 | TResult2> {
    return this.execute().then(onfulfilled, onrejected);
  }
}

export class GatewaySupabaseClient {
  constructor(public gateway: ImporterGatewayClient) {}

  from<T = any>(table: string): QueryBuilder<T> {
    return new QueryBuilder<T>(this.gateway, table);
  }

  async rpc(fn: string, args: Record<string, any> = {}): Promise<PostgrestResponse<any>> {
    try {
      // 1. Check for specific RPCs
      if (fn === 'importer_check_publication_barrier') {
        const rawState = await this.gateway.getSafetyBarrier();
        const state = (rawState || 'CLOSED').toUpperCase();
        if (state === 'EMERGENCY_HALT') {
          return { data: [{ can_publish: false, reason: 'EMERGENCY_HALT_ACTIVE', blocking_count: 1, blocking_sort_keys: [] }], error: null };
        }

        const targetSortKey = Number(args.p_target_sort_key);
        const workId = args.p_work_id;

        // 1. Get current highest published chapter in the canonical chapters table
        const pubRes = await this.gateway.sql(`
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
            const initCheck = await this.gateway.sql(`
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
        const gapCheck = await this.gateway.sql(`
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
          const explicitGapCheck = await this.gateway.sql(`
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
      const params = paramKeys.map(k => {
        const val = args[k];
        if (typeof val === 'object' && val !== null) {
          return JSON.stringify(val);
        }
        return val;
      });
      const res = await this.gateway.sql(sqlQuery, params);
      return { data: res.rows, error: null };
    } catch (err: any) {
      return { data: null, error: { message: err?.message || `RPC ${fn} failed` } };
    }
  }
}
