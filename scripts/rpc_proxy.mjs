/**
 * Serializing, 429-absorbing RPC proxy.
 *
 * Envio fires up to 100 concurrent queries per chain (hardcoded in
 * node_modules/envio/src/Env.res.mjs), reads any 429 as "range too big",
 * and halves its block interval until throughput hits zero.
 *
 * Usage:
 *   node scripts/rpc_proxy.mjs                    # :8546, concurrency 3
 *   PROXY_CONCURRENCY=2 node scripts/rpc_proxy.mjs
 * Then set rpc.url in config.yaml to http://127.0.0.1:8546.
 *
 */

import http from 'node:http';
import { text } from 'node:stream/consumers';

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
  errored: 0,
  maxQueueLength: 0,
  methodCounter: {},
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── Concurrency gate ───────────────────────────────────────────────────
//
// Single FIFO queue: requests reach the endpoint in arrival order.
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

/**
 * One upstream call, retried on 429, 5xx, or transport errors until the budget
 * runs out. Decided on status only: 176 of 259 observed 429s had empty bodies.
 */
async function callUpstream(payload) {
  for (const c of Array.isArray(payload) ? payload : [payload]) {
    if (c && c.method) stats.methodCounter[c.method] = (stats.methodCounter[c.method] ?? 0) + 1;
  }

  let delay = BASE_DELAY_MS;
  const backoff = async () => {
    await sleep(Math.random() * Math.min(delay, MAX_DELAY_MS));
    delay *= 2;
  };

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const isLast = attempt === MAX_RETRIES;
    let res, text, transportErr;

    await acquire();
    try {
      res = await fetch(UPSTREAM, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
      });
      text = await res.text();
      stats.upstream++;
    } catch (err) {
      transportErr = err;
    } finally {
      release();
    }

    if (transportErr) {
      if (isLast) throw transportErr;
      await backoff();
      continue;
    }

    if (res.status === 429 || res.status >= 500) {
      if (res.status === 429) stats.throttled++;
      else stats.errored++;
      if (isLast) {
        stats.failed++;
        throw new Error(`upstream still failing after ${MAX_RETRIES} retries (HTTP ${res.status})`);
      }
      await backoff();
      continue;
    }
    return text;
  }
}

const server = http.createServer(async (req, res) => {
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

  try {
    const body = await text(req);
    stats.requests++;
    const result = await callUpstream(JSON.parse(body));
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

server.listen(PORT, '127.0.0.1', () => {
  console.log(
    // Only the origin: the path of a provider URL might hold an API key.
    `rpc proxy on http://127.0.0.1:${PORT} -> ${new URL(UPSTREAM).origin}\n` +
      `  concurrency ${CONCURRENCY}, up to ${MAX_RETRIES} retries\n` +
      `  GET http://127.0.0.1:${PORT} for stats`,
  );
});

setInterval(() => {
  console.log(
    `[proxy] requests=${stats.requests} upstream=${stats.upstream} ` +
      `throttled=${stats.throttled} errored=${stats.errored} ` +
      `failed=${stats.failed} ` +
      `currActive=${active} currQueued=${Queue.length} ` +
      `maxQueueLength=${stats.maxQueueLength} ` +
      `methods=${JSON.stringify(stats.methodCounter)}`,
  );
}, 30_000).unref();
