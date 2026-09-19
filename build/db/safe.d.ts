export declare function safeQuery<T>(promise: Promise<T>): Promise<{
    data: Awaited<T>;
    error: null;
} | {
    data: null;
    error: unknown;
}>;
export declare function safeQuerySingle<T>(promise: Promise<T[]>): Promise<{
    data: NonNullable<T> | null;
    error: null;
} | {
    data: null;
    error: unknown;
}>;
