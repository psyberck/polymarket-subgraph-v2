/**
 * Map with a hard entry cap, for memoizing re-derivable facts.
 *
 * Eviction is a wholesale `clear()` rather than an LRU: callers here memoize
 * values that can always be fetched again, so forgetting one only ever costs a
 * lookup, and the flush keeps `set` O(1) with no recency bookkeeping on a hot
 * path. Pick a cap comfortably above the working set — a cap below it degrades
 * to a flush per miss rather than to a slow cache.
 *
 * The cap is a constructor argument, not an env read: this is a generic
 * container, and each caller decides where its own number comes from.
 */
export class SizedCache<V> extends Map<string, V> {
  private readonly maxSize: number;

  constructor(maxSize: number) {
    super();
    if (!Number.isInteger(maxSize) || maxSize < 1) {
      throw new Error(`maxSize must be a positive integer, got ${JSON.stringify(maxSize)}`);
    }
    this.maxSize = maxSize;
  }

  set(key: string, value: V): this {
    if (!this.has(key) && this.size >= this.maxSize) this.clear();
    return super.set(key, value);
  }
}
