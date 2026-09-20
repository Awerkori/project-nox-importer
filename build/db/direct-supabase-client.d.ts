import type pg from 'pg';
import { QueryBuilder, type PostgrestResponse, type SqlClient } from '../core/gateway-supabase.js';
export declare class DirectSupabaseClient implements SqlClient {
    private logger;
    private pool;
    constructor(pool?: pg.Pool);
    from<T = any>(table: string): QueryBuilder<T>;
    sql<T = any>(query: string, params?: any[]): Promise<{
        rows: T[];
        rowCount: number;
    }>;
    rpc(fn: string, args?: Record<string, any>): Promise<PostgrestResponse<any>>;
}
