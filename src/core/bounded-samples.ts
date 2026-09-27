/** Fixed storage; sample ordering is intentionally unspecified (statistics only). */
export class BoundedSamples<T> extends Array<T> {
  private cursor = 0;
  static get [Symbol.species]() { return Array; }
  constructor(private readonly capacity = 2048) { super(); }
  override push(...items: T[]): number {
    for (const item of items) {
      if (this.length < this.capacity) super.push(item);
      else { this[this.cursor] = item; this.cursor = (this.cursor + 1) % this.capacity; }
    }
    return this.length;
  }
}
