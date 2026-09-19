export declare class WriteBudget {
    private static MAX_WRITES_PER_DAY;
    private static MAX_WRITES_PER_MINUTE;
    private static writesThisMinute;
    private static currentMinute;
    static requestBudget(estimatedWrites: number): Promise<boolean>;
    static reportWrites(writes: number): void;
}
