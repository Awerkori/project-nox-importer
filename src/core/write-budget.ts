export class WriteBudget {
  private static MAX_WRITES_PER_DAY = 230000;
  private static MAX_WRITES_PER_MINUTE = 160;
  private static writesThisMinute = 0;
  private static currentMinute = Math.floor(Date.now() / 60000);

  public static async requestBudget(estimatedWrites: number): Promise<boolean> {
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

  public static reportWrites(writes: number) {
    this.writesThisMinute += writes;
  }
}
