import type { LiteTask } from './types.js';

export interface MonitorFinish {
  task: LiteTask;
  workerId: string;
  nextRunAt: Date;
  checkpoint: Record<string, unknown>;
}

type Pending = { value: MonitorFinish; resolve: (finished: boolean) => void; reject: (error: unknown) => void };

/** Coalesce unchanged monitors without making leases or successes speculative. */
export class MonitorFinishBatcher {
  private pending: Pending[] = [];
  private timer: ReturnType<typeof setTimeout> | null = null;
  constructor(private readonly finish: (batch: MonitorFinish[]) => Promise<Set<string>>) {}

  submit(value: MonitorFinish): Promise<boolean> {
    return new Promise((resolve, reject) => {
      this.pending.push({ value, resolve, reject });
      if (this.pending.length >= 12) void this.flush();
      else if (!this.timer) {
        this.timer = setTimeout(() => void this.flush(), 25);
        this.timer.unref();
      }
    });
  }

  private async flush(): Promise<void> {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    const batch = this.pending.splice(0, 12);
    if (!batch.length) return;
    try {
      const finished = await this.finish(batch.map((entry) => entry.value));
      for (const entry of batch) entry.resolve(finished.has(entry.value.task.id));
    } catch (error) {
      for (const entry of batch) entry.reject(error);
    }
  }
}
