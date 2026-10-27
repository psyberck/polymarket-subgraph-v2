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
 *   PROXY_UPSTREAM=https://a.example,https://b.example node scripts/rpc_proxy.mjs
 * Then set rpc.url in config.yaml to http://127.0.0.1:8546.
 *
 */

import http from 'node:http';
import { text } from 'node:stream/consumers';

const PORT = Number(process.env.PROXY_PORT ?? 8546);
const UPSTREAM_URLS = (
  process.env.PROXY_UPSTREAM ?? 'https://polygon.drpc.org,https://polygon.gateway.tenderly.co'
)
  .split(',')
  .map((u) => u.trim())
  .filter(Boolean);
const CONCURRENCY = Number(process.env.PROXY_CONCURRENCY ?? 3);
const MAX_RETRIES = Number(process.env.PROXY_MAX_RETRIES ?? 12);
const BASE_DELAY_MS = Number(process.env.PROXY_BASE_DELAY_MS ?? 250);
const MAX_DELAY_MS = Number(process.env.PROXY_MAX_DELAY_MS ?? 15_000);

const upstreams = UPSTREAM_URLS.map((url) => ({
  url,
  host: new URL(url).host,
  calls: 0,
  throttled: 0,
  errored: 0,
}));

const stats = {
  requests: 0,
  failed: 0,
  maxQueueLength: 0,
  methodCounter: {},
};

// ── Upstream RPC provider ───────────────────────────────────────────────
let current = 0;

function markFailed(u) {
  if (upstreams[current] === u) current = (current + 1) % upstreams.length;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sum = (key) => upstreams.reduce((n, u) => n + u[key], 0);
const unavailable = (status) => status === 401 || status === 403 || status === 429 || status >= 500;

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
    const upstream = upstreams[current];
    try {
      upstream.calls++;
      res = await fetch(upstream.url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
      });
      text = await res.text();
    } catch (err) {
      transportErr = err;
    } finally {
      release();
    }

    if (transportErr || unavailable(res.status)) {
      if (!transportErr && res.status === 429) upstream.throttled++;
      else upstream.errored++;
      markFailed(upstream);
      if (isLast) {
        stats.failed++;
        throw (
          transportErr ??
          new Error(`upstream still failing after ${MAX_RETRIES} retries (HTTP ${res.status})`)
        );
      }
      await backoff();
      continue;
    }
    return { status: res.status, text };
  }
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(
      JSON.stringify(
        {
          ...stats,
          upstream: sum('calls'),
          throttled: sum('throttled'),
          errored: sum('errored'),
          active,
          queued: Queue.length,
          upstreams: upstreams.map(({ host, calls, throttled, errored }) => ({
            host,
            calls,
            throttled,
            errored,
          })),
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
    const { status, text: result } = await callUpstream(JSON.parse(body));
    res.writeHead(status, { 'content-type': 'application/json' });
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
    `rpc proxy on http://127.0.0.1:${PORT} -> ${upstreams.map((u) => u.host).join(', ')}\n` +
      `  concurrency ${CONCURRENCY}, up to ${MAX_RETRIES} retries\n` +
      `  GET http://127.0.0.1:${PORT} for stats`,
  );
});

setInterval(() => {
  console.log(
    `[proxy] requests=${stats.requests} upstream=${sum('calls')} ` +
      `throttled=${sum('throttled')} errored=${sum('errored')} ` +
      `failed=${stats.failed} ` +
      `currActive=${active} currQueued=${Queue.length} ` +
      `maxQueueLength=${stats.maxQueueLength} ` +
      `methods=${JSON.stringify(stats.methodCounter)} ` +
      `upstreams=${upstreams.map((u) => `${u.host}:${u.calls}/${u.throttled}/${u.errored}`).join(',')}`,
  );
}, 30_000).unref();
