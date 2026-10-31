import type { LiteTask } from './types.js';
export interface MonitorFinish {
    task: LiteTask;
    workerId: string;
    nextRunAt: Date;
    checkpoint: Record<string, unknown>;
}
/** Coalesce unchanged monitors without making leases or successes speculative. */
export declare class MonitorFinishBatcher {
    private readonly finish;
    private pending;
    private timer;
    constructor(finish: (batch: MonitorFinish[]) => Promise<Set<string>>);
    submit(value: MonitorFinish): Promise<boolean>;
    private flush;
}
