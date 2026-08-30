/**
 * Serializing, 429-absorbing RPC proxy.
 *
 * Why this exists
 * ---------------
 * The public Polygon gateway serves this workload fine sequentially — measured
 * 5/5 eth_getLogs over 500-block spans at ~2s each, 20k-48k logs apiece, about
 * 250 blk/s — but it rejects JSON-RPC batches of 5+ and starts 429ing under
 * modest concurrency.
 *
 * Envio fires up to `Env.maxChainConcurrency` in-flight queries per chain,
 * which is the hardcoded literal 100 in src/Env.res.mjs with no env override.
 * That alone would be survivable; the fatal part is what happens next. Envio
 * treats a 429 as "the range was too big", halves `initial_block_interval`,
 * and retries. Under sustained throttling the interval collapses toward 1 block
 * and never recovers, so throughput goes to zero — 48 blocks in ~4 minutes,
 * against a window of 110,000. config.yaml already documents this exact
 * failure for drpc; the same thing happens here.
 *
 * So the throttling must be absorbed BELOW envio, where it looks like latency
 * rather than like an oversized range. This proxy:
 *
 *   - caps concurrency to what the endpoint actually tolerates,
 *   - queues everything else rather than rejecting it,
 *   - retries 429s itself, with backoff, and never passes one upstream,
 *   - forwards each request as-is, without splitting oversized batches.
 *
 * CAVEAT on that last point: the endpoint rejects JSON-RPC batches of 5+, and
 * this proxy used to split them. That path never once ran — `splits` stayed 0
 * across ~900k live requests, envio never batching above 4 — so it was removed
 * rather than carried untested. The limit itself did not go away. If envio ever
 * does batch above it, the rejection now reaches envio directly, and envio
 * misreads any failure as an oversized range and halves its block interval:
 * exactly the collapse this file exists to prevent. A batch above 4 arriving
 * here is therefore a silent cliff, not a graceful degradation. If that becomes
 * a risk, restore the split (see git history) or add a loud warning on
 * `Array.isArray(payload) && payload.length > 4`.
 *
 * Response bodies are passed through as raw bytes. The proxy parses each
 * REQUEST (small, and only for the per-method counters) but never reads a
 * response field, and an eth_getLogs page runs past 10MB — parsing one measured
 * ~68ms of blocked event loop, which on this single-threaded process stalls
 * every other queued request.
 *
 * From envio's side every request simply succeeds, slowly. Its block interval
 * stays where config.yaml put it and throughput becomes the endpoint's real
 * sequential rate instead of zero.
 *
 * Usage:
 *   node scripts/rpc_proxy.mjs                    # :8546, concurrency 3
 *   PROXY_CONCURRENCY=2 node scripts/rpc_proxy.mjs
 *
 * Then point the data source at it — in config.yaml:
 *   rpc:
 *     url: http://127.0.0.1:8546
 * and the payout probes with PAYOUT_RPC_URL=http://127.0.0.1:8546
 *
 * This is a local dev/backfill aid, not a production component. With an RPC
 * endpoint that has real headroom, drop it and point envio straight at that.
 */

import http from 'node:http';

const PORT = Number(process.env.PROXY_PORT ?? 8546);
const UPSTREAM = process.env.PROXY_UPSTREAM ?? 'https://polygon.gateway.tenderly.co';
const CONCURRENCY = Number(process.env.PROXY_CONCURRENCY ?? 3);
const MAX_RETRIES = Number(process.env.PROXY_MAX_RETRIES ?? 12);
const BASE_DELAY_MS = Number(process.env.PROXY_BASE_DELAY_MS ?? 250);
const MAX_DELAY_MS = Number(process.env.PROXY_MAX_DELAY_MS ?? 15_000);

const stats = {
  requests: 0,
  upstream: 0,
  throttled: 0,
  failed: 0,
  maxQueueLength: 0,
  methodCounter: {},
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── Concurrency gate ───────────────────────────────────────────────────
//
// One FIFO queue: requests reach the endpoint in arrival order.
//
// This was two queues — eth_call demoted, everything else promoted — to stop
// the payout probes starving log fetching. Measured against ~900k live
// requests, that split did not describe the traffic: eth_getBlockByNumber is
// 71% of all calls and sat in the SAME high queue as eth_getLogs, outnumbering
// the critical path 5:1, while the demoted eth_call was only 10%. The queue the
// comment claimed to protect was already the crowded one.
//
// The probe starvation it was written for is handled at the source instead, in
// src/lib/payouts.ts: PAYOUT_TRUST_METADATA keeps most conditions from probing
// at all, the effect's rateLimit caps arrival at 5/s, and PAYOUT_RPC_URL can
// move probes off this endpoint entirely. Ordering was never the real fix —
// handlers await the probe effect, so the stall was inside the handler.
//
// If starvation returns (PAYOUT_TRUST_METADATA=0 against this proxy is the way
// to cause it), reserve a slot for non-eth_call traffic rather than restoring
// strict priority — strict priority can starve probes into the 30s viem
// timeout, which falls back to merge pricing and mis-signs realized PnL.
let active = 0;
const Queue = [];

async function acquire() {
  if (active < CONCURRENCY) {
    active++;
    return;
  }
  stats.maxQueueLength = Math.max(stats.maxQueueLength, Queue.length + 1);
  await new Promise((resolve) => Queue.push(resolve));
  active++;
}

function release() {
  active--;
  const next = Queue.shift();
  if (next) next();
}

const THROTTLE_RE = /rate limit|too many requests|exceeds defined limit/i;

// Only the head of the body is sniffed. Some gateways signal a rate limit with
// HTTP 200 and an error object, so the body still has to be looked at — but a
// throttle message is short and states itself immediately, while a successful
// eth_getLogs page runs past 10MB. Scanning all of it measured ~7.6ms per
// response, and risked matching log payload data and retrying a GOOD response
// to death; a 1KB window is ~7900x cheaper and cannot reach payload.
const THROTTLE_SNIFF = 1024;

const isThrottle = (status, text) => {
  if (status === 429 || status === 503) return true;
  return THROTTLE_RE.test(text.slice(0, THROTTLE_SNIFF));
};

// Cheap stand-in for the JSON.parse the pass-through no longer does: catches an
// HTML error page or a truncated body without walking megabytes.
const looksLikeJson = (text) => {
  const c = text.trimStart()[0];
  return c === '{' || c === '[';
};

/**
 * One upstream call, retried until it is not a throttle.
 *
 * Retrying here rather than surfacing the 429 is the entire point: envio reads
 * a 429 as an oversized range and halves its block interval, which it never
 * grows back under sustained throttling.
 */
async function callUpstream(payload) {
  for (const c of Array.isArray(payload) ? payload : [payload]) {
    if (c && c.method) stats.methodCounter[c.method] = (stats.methodCounter[c.method] ?? 0) + 1;
  }
  let delay = BASE_DELAY_MS;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    await acquire();
    let res, text;
    try {
      res = await fetch(UPSTREAM, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
      });
      text = await res.text();
      stats.upstream++;
    } catch (err) {
      release();
      if (attempt === MAX_RETRIES) throw err;
      await sleep(Math.min(delay, MAX_DELAY_MS));
      delay *= 2;
      continue;
    }
    release();

    if (!isThrottle(res.status, text)) {
      if (!looksLikeJson(text)) {
        throw new Error(`upstream returned non-JSON (HTTP ${res.status})`);
      }
      return text;
    }

    stats.throttled++;
    if (attempt === MAX_RETRIES) {
      stats.failed++;
      throw new Error(`upstream still throttling after ${MAX_RETRIES} retries`);
    }
    await sleep(Math.random() * Math.min(delay, MAX_DELAY_MS));
    delay *= 2;
  }
}

const server = http.createServer((req, res) => {
  if (req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify(
        {
          ...stats,
          active,
          queued: Queue.length,
        },
        null,
        2,
      ),
    );
    return;
  }

  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', async () => {
    stats.requests++;
    try {
      const result = await callUpstream(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(result);
    } catch (err) {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify({
          jsonrpc: '2.0',
          id: null,
          error: { code: -32603, message: `proxy: ${String(err.message ?? err)}` },
        }),
      );
    }
  });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(
    `rpc proxy on http://127.0.0.1:${PORT} -> ${UPSTREAM}\n` +
      `  concurrency ${CONCURRENCY}, up to ${MAX_RETRIES} retries\n` +
      `  GET http://127.0.0.1:${PORT} for stats`,
  );
});

setInterval(() => {
  console.log(
    `[proxy] requests=${stats.requests} upstream=${stats.upstream} ` +
      `throttled=${stats.throttled} failed=${stats.failed} ` +
      `currActive=${active} currQueued=${Queue.length} ` +
      `maxQueueLength=${stats.maxQueueLength} ` +
      `methods=${JSON.stringify(stats.methodCounter)}`,
  );
}, 30_000).unref();
