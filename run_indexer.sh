#!/usr/bin/env bash
# Launch the supervisor (and the indexer), optionally with the RPC proxy.

set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

readonly USAGE="\
usage: run_indexer.sh [--proxy] --output-dir <path> [--schedule monthly|weekly|stride [--stride <blocks>]]
                      [--prune [--debug]]

flags:
  --proxy                   also start the RPC proxy (scripts/rpc_proxy.mjs), unless one is already listening on
                            PROXY_PORT. The proxy runs on its own, logs to logs/proxy.log and outlives the supervisor;
                            stop it manually with the kill command this script prints.
  -h, --help                show this usage

  --output-dir <path>       required; snapshot folders are written here, created if missing
  --schedule <schedule>     monthly (1,296,000 blocks), weekly (302,400 blocks) or stride; without it, the indexer runs to
                            the head and no snapshots are taken
  --stride <blocks>         required with '--schedule stride', invalid otherwise
  --prune                   after each snapshot, delete the rows it archived
  --debug                   only with --prune: log what would be deleted, delete nothing

env: DUCKDB_MEMORY_LIMIT (default 2GB), DUCKDB_THREADS (default 2); see README.md#running"

readonly PROXY_START_TIMEOUT=10

log() { echo "=== $(date -Is) $*"; }
error() {
  log "FATAL: $*" >&2
  exit 1
}
listening() { : 2>/dev/null < "/dev/tcp/127.0.0.1/$1"; }

# Start the proxy in its own session (setsid): it has a different lifecycle than the indexer.
start_proxy() {
  local port=${PROXY_PORT:-8546}
  if listening "$port"; then
    log "process already listening on port $port; not starting proxy"
    return
  fi

  mkdir -p logs
  log "proxy server starting"
  setsid node scripts/rpc_proxy.mjs >> logs/proxy.log 2>&1 < /dev/null &
  local pid=$! deadline=$((SECONDS + PROXY_START_TIMEOUT))
  until listening "$port"; do
    kill -0 "$pid" 2>/dev/null || error "proxy exited before listening on port $port"
    if ((SECONDS >= deadline)); then
      kill "$pid"
      error "proxy not listening on port $port after ${PROXY_START_TIMEOUT}s; terminated it"
    fi
    sleep 0.2
  done
  log "proxy runs at pid $pid on port $port; terminate it manually with: kill $pid"
}

proxy=false
while (($#)); do
  case $1 in
    --proxy) proxy=true; shift ;;
    -h | --help) echo "$USAGE"; exit 0 ;;
    *) break ;;
  esac
done

[[ -f .env ]] || error ".env not found in $PWD"
set -a
# shellcheck source=/dev/null
. ./.env
set +a

log "node $(node --version) start_block ${ENVIO_START_BLOCK:-<config.yaml default>}" \
  "p2p_transfers ${INDEX_INCLUDE_P2P_TRANSFERS:-false} proxy $proxy supervisor $*"

if $proxy; then start_proxy; fi

log "supervisor runs at pid $$; terminate it with: kill $$"
exec node supervisor/main.ts "$@"
