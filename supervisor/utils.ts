/** Seconds since `start` (a Date.now() value), for step timings. */
export const since = (start: number): string => `${((Date.now() - start) / 1000).toFixed(1)}s`;
