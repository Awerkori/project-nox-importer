export class WriteBudget {
    static MAX_WRITES_PER_DAY = 230000;
    static MAX_WRITES_PER_MINUTE = 160;
    static writesThisMinute = 0;
    static currentMinute = Math.floor(Date.now() / 60000);
    static async requestBudget(estimatedWrites) {
        const now = Math.floor(Date.now() / 60000);
        if (now > this.currentMinute) {
            this.currentMinute = now;
            this.writesThisMinute = 0;
        }
        if (this.writesThisMinute + estimatedWrites <= this.MAX_WRITES_PER_MINUTE) {
            this.writesThisMinute += estimatedWrites;
            return true;
        }
        return false;
    }
    static reportWrites(writes) {
        this.writesThisMinute += writes;
    }
}
