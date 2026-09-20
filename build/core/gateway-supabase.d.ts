import { ImporterGatewayClient } from './gateway-client.js';
export interface PostgrestResponse<T = any> {
    data: T | null;
    error: {
        message: string;
        code?: string;
        details?: string;
    } | null;
    count?: number | null;
}
export interface SqlClient {
    sql<T = any>(query: string, params?: any[]): Promise<{
        rows: T[];
        rowCount?: number;
    }>;
}
export declare class QueryBuilder<T = any> implements PromiseLike<PostgrestResponse<T>> {
    private client;
    private table;
    private op;
    private selectedCols;
    private insertData;
    private updateData;
    private onConflict;
    private filters;
    private orderCol;
    private orderAsc;
    private limitCount;
    private isSingle;
    private isMaybeSingle;
    constructor(client: SqlClient, table: string);
    select(columns?: string): this;
    insert(values: any): this;
    update(values: any): this;
    upsert(values: any, options?: {
        onConflict?: string;
    }): this;
    delete(): this;
    eq(col: string, val: any): this;
    neq(col: string, val: any): this;
    gt(col: string, val: any): this;
    gte(col: string, val: any): this;
    lt(col: string, val: any): this;
    lte(col: string, val: any): this;
    in(col: string, vals: any[]): this;
    is(col: string, val: any): this;
    ilike(col: string, pattern: string): this;
    like(col: string, pattern: string): this;
    overlaps(col: string, vals: any[]): this;
    order(col: string, options?: {
        ascending?: boolean;
    }): this;
    not(col: string, op: string, val: any): this;
    limit(count: number): this;
    single(): this;
    maybeSingle(): this;
    private isJsonCol;
    private serializeVal;
    private buildSql;
    execute(): Promise<PostgrestResponse<T>>;
    then<TResult1 = PostgrestResponse<T>, TResult2 = never>(onfulfilled?: ((value: PostgrestResponse<T>) => TResult1 | PromiseLike<TResult1>) | undefined | null, onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null): Promise<TResult1 | TResult2>;
}
export declare class GatewaySupabaseClient {
    gateway: ImporterGatewayClient;
    constructor(gateway: ImporterGatewayClient);
    from<T = any>(table: string): QueryBuilder<T>;
    rpc(fn: string, args?: Record<string, any>): Promise<PostgrestResponse<any>>;
}
