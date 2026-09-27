/** Serial, completion-scheduled noncritical maintenance. Queries retain their DB timeout;
 * never release the single-flight guard on a Promise.race while SQL is still running. */
export class MaintenanceScheduler {
    tasks = new Map();
    timer = null;
    running = false;
    stopped = false;
    register(name, intervalMs, initialDelayMs, run) {
        if (this.tasks.has(name))
            return; // Soft restarts must not multiply loops.
        this.stopped = false;
        this.tasks.set(name, { intervalMs, run, nextRun: Date.now() + initialDelayMs,
            lastStarted: null, lastFinished: null, durationMs: 0, rowsTouched: null, running: false, failures: 0 });
        this.schedule();
    }
    schedule() {
        if (this.stopped || this.running || !this.tasks.size)
            return;
        if (this.timer)
            clearTimeout(this.timer);
        const next = Math.min(...[...this.tasks.values()].map(t => t.nextRun));
        this.timer = setTimeout(() => { this.timer = null; void this.tick(); }, Math.max(0, next - Date.now()));
        this.timer.unref();
    }
    async tick() {
        if (this.stopped || this.running)
            return;
        const task = [...this.tasks.values()].filter(t => t.nextRun <= Date.now()).sort((a, b) => a.nextRun - b.nextRun)[0];
        if (!task) {
            this.schedule();
            return;
        }
        this.running = task.running = true;
        task.lastStarted = Date.now();
        try {
            task.rowsTouched = (await task.run()) ?? null;
        }
        catch {
            task.failures++;
        } // Task owns its structured error log; scheduler exposes failures.
        finally {
            task.lastFinished = Date.now();
            task.durationMs = task.lastFinished - task.lastStarted;
            task.nextRun = Date.now() + task.intervalMs + Math.floor(Math.random() * Math.min(2000, task.intervalMs * .1));
            this.running = task.running = false;
            this.schedule();
        }
    }
    snapshot() {
        return Object.fromEntries([...this.tasks].map(([name, t]) => [name, {
                lastStarted: t.lastStarted, lastFinished: t.lastFinished, durationMs: t.durationMs,
                rowsTouched: t.rowsTouched, nextRun: t.nextRun, currentlyRunning: t.running, failures: t.failures,
            }]));
    }
    stop() {
        this.stopped = true;
        if (this.timer)
            clearTimeout(this.timer);
        this.timer = null;
        this.tasks.clear();
    }
}
export const maintenanceScheduler = new MaintenanceScheduler();
