/** Append Gamma markets to markets.jsonl */
import { appendFileSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { META_FILE, RAW } from './utils/literals';

const GAMMA_MARKETS_KEYSET = 'https://gamma-api.polymarket.com/markets/keyset';

const RPS = 50;
const PAGE_SIZE = 100;
const MAXIMUM_RETRY = 10;
const RETRY_STRIDE = 10;

const CLOSED = ['true', 'false'] as const;
type Closed = (typeof CLOSED)[number];

interface WalkState {
  /** Fetches the page as cursor; none before the first full page. */
  cursorId?: string;
  /** Highest market id saved from that page; lower ids are skipped on re-read. */
  marketId: number;
}

interface Progress {
  cursors: Partial<Record<Closed, WalkState>>;
  markets: number;
  updatedAt: string;
  completed: boolean;
}

interface Page {
  markets?: GammaMarket[];
  next_cursor?: string;
}

interface GammaMarket {
  id: string;
}

/**
 * Progress from disk, or a fresh one.
 *
 * @returns The saved walk state.
 */
const getProgress = (): Progress => {
  const p = existsSync(META_FILE)
    ? (JSON.parse(readFileSync(META_FILE, 'utf8')) as Partial<Progress>)
    : {};
  return {
    ...p,
    cursors: p.cursors ?? {},
    markets: p.markets ?? 0,
    updatedAt: p.updatedAt ?? new Date().toISOString(),
    completed: false,
  };
};

/**
 * Write progress. Called after appending, so a crash re-fetches a page rather
 * than skipping it.
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
 * Walk markets oldest first, one walk per `closed` value in parallel, from each
 * walk's saved page to the newest. The last page keeps its cursor, so the next
 * run re-reads it, skips ids up to `marketId`, and carries on. A rejected
 * cursor (422) restarts that walk from the oldest.
 *
 * Cloudflare blocks some request URLs outright (403); the same cursor with a
 * smaller page size goes through, so a blocked page is retried one size smaller
 * at a time. The walks fail independently.
 *
 * @throws When a walk fails; the saved state resumes it.
 */
async function fetchMarkets(): Promise<void> {
  let slot = 0;
  const p = getProgress();

  /**
   * One page, retried with backoff. A 403 is Cloudflare blocking the URL, which
   * a smaller page size gets past, so each 403 retries RETRY_STRIDE smaller.
   *
   * @returns The page.
   * @throws After MAXIMUM_RETRY attempts.
   */
  const getPage = async (closed: Closed, cursor?: string): Promise<Page> => {
    let limit = PAGE_SIZE;
    const url = new URL(GAMMA_MARKETS_KEYSET);
    url.searchParams.set('order', 'id');
    url.searchParams.set('ascending', 'true');
    url.searchParams.set('closed', closed);
    if (cursor) url.searchParams.set('after_cursor', cursor);

    let reason = '';
    for (let attempt = 1; attempt <= MAXIMUM_RETRY; attempt++) {
      url.searchParams.set('limit', String(limit));
      const now = Date.now();
      const at = Math.max(now, slot);
      slot = at + 1000 / RPS;
      if (at > now) await new Promise((r) => setTimeout(r, at - now));

      try {
        const res = await fetch(url);
        if (res.ok) return (await res.json()) as Page;
        reason = `HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`;
        if (res.status === 403) limit = Math.max(RETRY_STRIDE, limit - RETRY_STRIDE);
      } catch (e) {
        reason = e instanceof Error ? e.message : String(e);
      }
      console.log(`  closed=${closed} retry ${attempt}: ${reason}`);
      if (attempt < MAXIMUM_RETRY) {
        await new Promise((r) => setTimeout(r, Math.min(250 * 2 ** (attempt - 1), 5_000)));
      }
    }
    throw new Error(`closed=${closed}: ${MAXIMUM_RETRY} attempts reached: ${reason}`);
  };

  /** One `closed` walk, from its saved page to the newest. */
  const walk = async (closed: Closed): Promise<void> => {
    const state = (p.cursors[closed] ??= { marketId: 0 });
    let pages = 0;

    for (;;) {
      const page = await getPage(closed, state.cursorId);
      const ms = page.markets ?? [];
      const markets = ms.filter((m) => Number(m.id) > state.marketId);
      if (markets.length) appendFileSync(RAW, markets.map((m) => jsonLine(m)).join('\n') + '\n');
      p.markets += markets.length;
      for (const m of ms) state.marketId = Math.max(state.marketId, Number(m.id));
      if (page.next_cursor) state.cursorId = page.next_cursor;
      saveProgress(p);
      if (++pages % 200 === 0) {
        console.log(
          `  closed=${closed}: ${pages} pages, at id ${state.marketId.toLocaleString()}, ` +
            `markets=${p.markets.toLocaleString()}`,
        );
      }
      if (!page.next_cursor) return;
    }
  };

  console.log(`  starting from ${JSON.stringify(p.cursors)}...`);
  const results = await Promise.allSettled(CLOSED.map((c) => walk(c)));
  const failed = results.flatMap((r, i) =>
    r.status === 'rejected' ? [[CLOSED[i], r.reason]] : [],
  );
  for (const [c, e] of failed)
    console.log(`  closed=${c} FAILED: ${e instanceof Error ? e.message : e}`);
  if (failed.length) throw new Error(`${failed.length} walk(s) failed; rerun to resume`);

  p.completed = true;
  saveProgress(p);
  console.log(
    `\n  DONE  markets=${p.markets.toLocaleString()} at ` +
      CLOSED.map((c) => `closed=${c} id ${p.cursors[c]?.marketId.toLocaleString()}`).join(', '),
  );
}

await fetchMarkets();
