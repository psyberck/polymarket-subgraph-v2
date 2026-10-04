/**
 * Rebuild the Gamma market corpus (markets.jsonl) from `/events`, every run.
 */
import { appendFileSync, renameSync, writeFileSync } from 'node:fs';
import { META_FILE, RAW } from './utils/literals';

const GAMMA_EVENTS = 'https://gamma-api.polymarket.com/events';

const RPS = 50;
const PAGE_SIZE = 100;
const WORKERS = 10;
const MAXIMUM_RETRY = 10;
const EMPTY_WAVES_TO_TERMINATE = 10;

const TMP = `${RAW}.tmp`;

interface GammaEvent {
  id: string;
  markets?: unknown[];
}

/** One record per line: escapes U+2028/U+2029, which readline splits on. */
const jsonLine = (v: unknown): string =>
  JSON.stringify(v)
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029');

/**
 * Highest event id in a batch; 0 when it is empty.
 *
 * @param evs - events from one or more requests.
 */
const getMaxEventId = (evs: GammaEvent[]): number =>
  evs.reduce((hi, e) => {
    const n = Number(e.id);
    return Number.isInteger(n) && n > hi ? n : hi;
  }, 0);

let slot = 0;

/**
 * One request's events, retried with backoff.
 *
 * @param start - first event id of the batch; omitted for the first page by
 *   id ascending, which is how the walk learns where events begin.
 * @returns The raw events.
 * @throws After MAXIMUM_RETRY attempts; the run then fails and is rerun.
 */
const getEvents = async (start?: number): Promise<GammaEvent[]> => {
  const url = new URL(GAMMA_EVENTS);
  url.searchParams.set('limit', String(PAGE_SIZE));
  url.searchParams.set('order', 'id');
  url.searchParams.set('ascending', 'true');
  if (start !== undefined) {
    for (let id = start; id < start + PAGE_SIZE; id++) url.searchParams.append('id', String(id));
  }

  let reason = '';
  for (let attempt = 1; attempt <= MAXIMUM_RETRY; attempt++) {
    const now = Date.now();
    const at = Math.max(now, slot);
    slot = at + 1000 / RPS;
    if (at > now) await new Promise((r) => setTimeout(r, at - now));
    try {
      const res = await fetch(url);
      if (res.ok) return (await res.json()) as GammaEvent[];
      reason = `HTTP ${res.status}`;
    } catch (e) {
      reason = e instanceof Error ? e.message : String(e);
    }
    console.log(`  ids ${start ?? 'first page'} retry ${attempt}: ${reason}`);
    if (attempt < MAXIMUM_RETRY) {
      await new Promise((r) => setTimeout(r, Math.min(250 * 2 ** (attempt - 1), 5_000)));
    }
  }
  throw new Error(`ids ${start ?? 'first page'}: ${MAXIMUM_RETRY} attempts reached: ${reason}`);
};

/**
 * Walk every event by id, WORKERS batches of PAGE_SIZE ids at a time, and
 * write their markets to TMP; then rename TMP over RAW.
 */
async function fetchMarkets(): Promise<void> {
  const t0 = Date.now();
  writeFileSync(TMP, '');
  let events = 0;
  let markets = 0;
  let waves = 0;
  let empty = 0;

  const append = (evs: GammaEvent[]) => {
    const ms = evs.flatMap((e) => e.markets ?? []);
    if (ms.length) appendFileSync(TMP, ms.map((m) => jsonLine(m)).join('\n') + '\n');
    events += evs.length;
    markets += ms.length;
  };

  const first = await getEvents();
  append(first);
  let next = getMaxEventId(first) + 1;
  if (next === 1) throw new Error('first page returned no events — cannot find a start id');

  for (;;) {
    const starts = Array.from({ length: WORKERS }, (_, k) => next + k * PAGE_SIZE);
    const evs = (await Promise.all(starts.map((s) => getEvents(s)))).flat();
    append(evs);
    next += WORKERS * PAGE_SIZE;
    empty = evs.length ? 0 : empty + 1;
    if (empty >= EMPTY_WAVES_TO_TERMINATE) break;
    if (++waves % 50 === 0) {
      console.log(
        `  id ${(next - 1).toLocaleString()}  events=${events.toLocaleString()} ` +
          `markets=${markets.toLocaleString()}`,
      );
    }
  }

  renameSync(TMP, RAW);
  const seconds = Math.round((Date.now() - t0) / 1000);
  writeFileSync(
    META_FILE,
    JSON.stringify({ completedAt: new Date().toISOString(), events, markets, seconds }, null, 2),
  );
  console.log(
    `\n  DONE  events=${events.toLocaleString()} markets=${markets.toLocaleString()} in ${seconds}s`,
  );
}

await fetchMarkets();
