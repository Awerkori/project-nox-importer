/** Fixed storage; sample ordering is intentionally unspecified (statistics only). */
export declare class BoundedSamples<T> extends Array<T> {
    private readonly capacity;
    private cursor;
    static get [Symbol.species](): ArrayConstructor;
    constructor(capacity?: number);
    push(...items: T[]): number;
}
