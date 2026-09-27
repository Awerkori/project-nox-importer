/** Fixed storage; sample ordering is intentionally unspecified (statistics only). */
export class BoundedSamples extends Array {
    capacity;
    cursor = 0;
    static get [Symbol.species]() { return Array; }
    constructor(capacity = 2048) {
        super();
        this.capacity = capacity;
    }
    push(...items) {
        for (const item of items) {
            if (this.length < this.capacity)
                super.push(item);
            else {
                this[this.cursor] = item;
                this.cursor = (this.cursor + 1) % this.capacity;
            }
        }
        return this.length;
    }
}
