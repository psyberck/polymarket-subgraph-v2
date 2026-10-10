/**
 * Format the time since `start`, for step timings.
 *
 * @param start - A `Date.now()` value.
 * @returns Seconds, e.g. `12.3s`.
 */
export const since = (start: number): string => `${((Date.now() - start) / 1000).toFixed(1)}s`;
