/**
 * WriteBudget: Deprecated and neutralized.
 * YugabyteDB does not impose Turso SQLite write limits.
 * All operations are non-blocking no-ops.
 */
export class WriteBudget {
    static async requestBudget(_estimatedWrites) {
        return true;
    }
    static reportWrites(_writes) {
        // No-op in Yugabyte era
    }
}
