/**
 * WriteBudget: Deprecated and neutralized.
 * YugabyteDB does not impose Turso SQLite write limits.
 * All operations are non-blocking no-ops.
 */
export class WriteBudget {
  public static async requestBudget(_estimatedWrites: number): Promise<boolean> {
    return true;
  }

  public static reportWrites(_writes: number): void {
    // No-op in Yugabyte era
  }
}
