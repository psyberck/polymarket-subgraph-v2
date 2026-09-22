/**
 * Where the corpus and its progress live.
 *
 * SHARED so fetch and init cannot disagree. They were out of step once -- the
 * loader looked in logs/seed while the writer used logs/markets -- and the only
 * symptom was every file reported missing, which reads like a failed build
 * rather than a path mismatch.
 */
export const OUT_DIR = 'logs/markets';
export const META_FILE = `${OUT_DIR}/meta.json`;
export const RAW = `${OUT_DIR}/markets.jsonl`;
