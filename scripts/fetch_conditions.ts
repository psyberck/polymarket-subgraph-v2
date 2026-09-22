/**
 * Append the Gamma event corpus to markets.jsonl.
 *
 * Writes only files: no database, no chain. Run it as often as you like; it
 * resumes from meta.json and appends, so the corpus is the union of every run.
 */
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { META_FILE, OUT_DIR, RAW } from './utils/literals';

const GAMMA = 'https://gamma-api.polymarket.com/events';

const RPS = 50;
const PAGE_SIZE = 100;
const WORKERS = 10;
const MAXIMUM_RETRY = 10;
const EMPTY_WAVES_TO_TERMINATE = 10;

interface Progress {
  eventId: number | null;
  pages: number;
  events: number;
  markets: number;
  updated_at: string;
  completed: boolean;
}

interface GammaEvent {
  id: string;
  markets: unknown[];
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
  updated_at: new Date().toISOString(),
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
 * Write progress, stamping the time.
 *
 * Called after markets are appended, never before: a crash between the two
 * re-fetches a wave rather than skipping it, which duplicates lines instead of
 * losing them. Duplicate lines are the safer failure -- but see the note on
 * insertHandler in init_conditions.ts, which has no ON CONFLICT to absorb them.
 *
 * @param p - the progress to persist; its `updated_at` is set here.
 */
const saveProgress = (p: Progress) => {
  p.updated_at = new Date().toISOString();
  writeFileSync(META_FILE, JSON.stringify(p, null, 2));
};

/**
 * One JSON record on exactly one line.
 *
 * MODULE SCOPE because both fetch paths append lines. JSON.stringify leaves
 * U+2028 and U+2029 RAW while readline SPLITS on them, so one market's question
 * text broke a record in two and JSON.parse got 85 truncated characters. Escaped
 * here, at the only place lines are written.
 */
const jsonLine = (v: unknown): string =>
  JSON.stringify(v)
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029');

/**
 * Highest event id in a batch.
 *
 * `reduce`, not `Math.max(...ids)`: on an empty batch Math.max() returns
 * -Infinity, which JSON.stringify writes as `null` -- the same value that means
 * "nothing saved", so the next run would restart from the first page.
 *
 * @param evs - events from one or more requests.
 * @returns The highest id, or 0 when the batch is empty.
 */
const getMaxEventId = (evs: GammaEvent[]): number =>
  evs.reduce((hi, e) => {
    const n = Number(e.id);
    return Number.isInteger(n) && n > hi ? n : hi;
  }, 0);

/**
 * The Gamma event corpus, appended to markets.jsonl.
 *
 * Walks BY EVENT ID, not by cursor. `/events/keyset` serves some id regions at
 * a fixed ~2.2 s regardless of page size, which is its timeout: limit=1 lands
 * under it, limit>=2 always 500s, and retrying never gets through. Ids have no
 * such region, need no cursor, and parallelise -- a wave is WORKERS batches of
 * PAGE_SIZE ids issued together, and the id is itself the resume point.
 *
 * Ids are SPARSE: 96.2% density over ids 2,800-260,000, with one run of 3,400
 * absent in a row. So a short response is normal and says nothing about the
 * end; only EMPTY_WAVES_TO_TERMINATE consecutive empty waves does.
 *
 * @returns Nothing. Markets are appended to RAW, progress written to META_FILE
 *   after each wave, and `completed` set once the walk passes the last event.
 */
async function fetchMarkets(): Promise<void> {
  // Shared pacer: every request, retries included, takes the next 1000/RPS slot.
  let slot = 0;

  /**
   * One request's events, retried with a backoff ladder.
   *
   * @param params - ids to ask for. A bare `pageSize` with no range means the
   *   first page by id ascending, which is how a fresh walk learns where events
   *   begin instead of assuming a floor.
   * @returns The raw events -- NOT their markets. The caller flattens; it needs
   *   the ids to know how far the walk actually got.
   * @throws After MAXIMUM_RETRY attempts. Aborts the run: progress is saved per
   *   wave, so a restart resumes, but a permanently bad range cannot be passed.
   */
  const getEventsByIds = async (params: MarketQueryParams): Promise<GammaEvent[]> => {
    let reason = '';
    const url = new URL(GAMMA);
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
      `ids ${params?.startBlockId}-${params?.endBlockId}: ${MAXIMUM_RETRY} attempts reached: ${reason}`,
    );
  };

  mkdirSync(OUT_DIR, { recursive: true });
  const p = getProgress();
  p.completed = false;
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
        p.eventId = nextId + 1;
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
  console.log(
    `\n  DONE  events=${p.events.toLocaleString()} markets=${p.markets.toLocaleString()}`,
  );
}

await fetchMarkets();
