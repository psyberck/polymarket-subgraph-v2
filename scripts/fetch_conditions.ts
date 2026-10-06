/**
 * Gamma market corpus (markets.jsonl) from `/events`, walking event ids.
 */
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { META_FILE, OUT_DIR, RAW } from '#scripts/utils/literals.ts';

const GAMMA_EVENTS = 'https://gamma-api.polymarket.com/events';

const RPS = 50;
const PAGE_SIZE = 100;
const WORKERS = 10;
const MAXIMUM_RETRY = 10;
const EMPTY_WAVES_TO_TERMINATE = 10;

/** Rebuild from the first event instead of resuming (program option). */
const REBUILD = process.argv.includes('--rebuild');

interface Progress {
  eventId: number | null;
  pages: number;
  events: number;
  markets: number;
  updatedAt: string;
  completed: boolean;
}

interface GammaEvent {
  id: string;
  markets?: unknown[];
}

interface MarketQueryParams {
  startBlockId?: number;
  endBlockId?: number;
  pageSize: number;
}

/**
 * A walk that has not started.
 *
 * @returns Progress with a null eventId, which is what makes the first pass ask
 *   for the first page by id instead of resuming.
 */
const initialProgress = (): Progress => ({
  eventId: null,
  pages: 0,
  events: 0,
  markets: 0,
  updatedAt: new Date().toISOString(),
  completed: false,
});

/**
 * Progress from disk, or a fresh one.
 *
 * @returns The saved walk state, or `initialProgress()` when META_FILE is absent.
 */
const getProgress = (): Progress =>
  existsSync(META_FILE)
    ? (JSON.parse(readFileSync(META_FILE, 'utf8')) as Progress)
    : initialProgress();
/**
 * Write progress. Called after appending, so a crash re-fetches a wave rather
 * than skipping it; `init_conditions` keeps the last copy of a repeat.
 *
 * @param p - the progress to persist; `updatedAt` is set here.
 */
const saveProgress = (p: Progress) => {
  p.updatedAt = new Date().toISOString();
  writeFileSync(META_FILE, JSON.stringify(p, null, 2));
};

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

/**
 * Walk events by id from the saved position, WORKERS batches of PAGE_SIZE ids
 * at a time, appending their markets and saving progress after each wave.
 *
 * Ids are SPARSE, so only EMPTY_WAVES_TO_TERMINATE empty waves in a row end
 * the walk; the saved position then moves back to just past the last event
 * seen, where the next run starts.
 */
async function fetchMarkets(): Promise<void> {
  let slot = 0;

  /**
   * One request's events, retried with backoff.
   *
   * @param params - ids to ask for. A bare `pageSize` with no range means the
   *   first page by id ascending, which is how a fresh walk learns where events
   *   begin instead of assuming a floor.
   * @returns The raw events.
   * @throws After MAXIMUM_RETRY attempts; the run then fails and is rerun.
   */
  const getEventsByIds = async (params: MarketQueryParams): Promise<GammaEvent[]> => {
    let reason = '';

    const url = new URL(GAMMA_EVENTS);
    url.searchParams.set('limit', String(params.pageSize));
    url.searchParams.set('order', 'id');
    url.searchParams.set('ascending', 'true');
    if (params.startBlockId !== undefined && params.endBlockId !== undefined) {
      for (let id = params.startBlockId; id <= params.endBlockId; id++)
        url.searchParams.append('id', String(id));
    }

    for (let attempt = 1; attempt <= MAXIMUM_RETRY; attempt++) {
      const now = Date.now();
      const at = Math.max(now, slot);
      slot = at + 1000 / RPS;
      if (at > now) await new Promise((r) => setTimeout(r, at - now));
      try {
        const res = await fetch(url);
        if (res.ok) return (await res.json()) as GammaEvent[];
        reason = `HTTP ${res.status}: ${await res.text()}`;
      } catch (e) {
        reason = e instanceof Error ? e.message : String(e);
      }
      if (attempt < MAXIMUM_RETRY) {
        await new Promise((r) => setTimeout(r, Math.min(250 * 2 ** (attempt - 1), 5_000)));
      }
    }
    throw new Error(
      `ids ${params?.startBlockId}-${params.endBlockId}: ${MAXIMUM_RETRY} attempts reached: ${reason}`,
    );
  };

  const t0 = Date.now();
  mkdirSync(OUT_DIR, { recursive: true });

  if (REBUILD) {
    rmSync(RAW, { force: true });
    rmSync(META_FILE, { force: true });
  }

  const p = getProgress();
  p.completed = false;
  const start = p.eventId;
  let waves = 0;

  let nextId = 0;
  let none = 0;

  for (;;) {
    let evs: GammaEvent[];
    let currMaxEventId: number;

    if (!p.eventId) {
      evs = await getEventsByIds({ pageSize: PAGE_SIZE });
      p.pages += 1;
      currMaxEventId = getMaxEventId(evs);
      if (!currMaxEventId)
        throw new Error('first page returned no events — cannot find a start id');
      console.log(`  first page ends at id ${currMaxEventId.toLocaleString()}`);
    } else {
      const items: MarketQueryParams[] = [];
      for (let i = p.eventId; items.length < WORKERS; i += PAGE_SIZE) {
        items.push({ startBlockId: i, endBlockId: i + PAGE_SIZE - 1, pageSize: PAGE_SIZE });
      }
      evs = (await Promise.all(items.map((item) => getEventsByIds(item)))).flat();
      p.pages += items.length;
      currMaxEventId = p.eventId + items.length * PAGE_SIZE - 1;
    }

    const markets = evs.flatMap((e) => e.markets ?? []);
    if (markets.length) {
      appendFileSync(RAW, markets.map((m) => jsonLine(m)).join('\n') + '\n');
    }

    nextId = Math.max(nextId, getMaxEventId(evs));
    p.eventId = currMaxEventId + 1;
    p.events += evs.length;
    p.markets += markets.length;
    saveProgress(p);

    if (evs.length === 0) {
      if (++none >= EMPTY_WAVES_TO_TERMINATE) {
        p.eventId = nextId ? nextId + 1 : start;
        saveProgress(p);
        break;
      }
    } else {
      none = 0;
    }

    if (++waves % 20 === 0) {
      console.log(
        `  id ${currMaxEventId.toLocaleString()}  events=${p.events.toLocaleString()} ` +
          `markets=${p.markets.toLocaleString()}`,
      );
    }
  }

  p.completed = true;
  saveProgress(p);
  const seconds = Math.round((Date.now() - t0) / 1000);
  console.log(
    `\n  DONE  events=${p.events.toLocaleString()} markets=${p.markets.toLocaleString()} ` +
      `next id ${p.eventId?.toLocaleString() ?? 'none'} in ${seconds}s`,
  );
}

await fetchMarkets();
