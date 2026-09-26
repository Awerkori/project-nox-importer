/**
 * WriteBudget: Deprecated and neutralized.
 * YugabyteDB does not impose Turso SQLite write limits.
 * All operations are non-blocking no-ops.
 */
export declare class WriteBudget {
    static requestBudget(_estimatedWrites: number): Promise<boolean>;
    static reportWrites(_writes: number): void;
}
