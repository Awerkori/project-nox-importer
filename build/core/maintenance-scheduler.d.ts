/** Serial, completion-scheduled noncritical maintenance. Queries retain their DB timeout;
 * never release the single-flight guard on a Promise.race while SQL is still running. */
export declare class MaintenanceScheduler {
    private tasks;
    private timer;
    private running;
    private stopped;
    register(name: string, intervalMs: number, initialDelayMs: number, run: () => Promise<number | void>): void;
    private schedule;
    private tick;
    snapshot(): {
        [k: string]: {
            lastStarted: number | null;
            lastFinished: number | null;
            durationMs: number;
            rowsTouched: number | null;
            nextRun: number;
            currentlyRunning: boolean;
            failures: number;
        };
    };
    stop(): void;
}
export declare const maintenanceScheduler: MaintenanceScheduler;
