# Polymarket Indexer

[![Node.js 24](https://img.shields.io/badge/node-24-339933?logo=node.js&logoColor=white)](https://nodejs.org)
[![TypeScript 5.9](https://img.shields.io/badge/typescript-5.9-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![Ubuntu 22.04+](https://img.shields.io/badge/ubuntu-22.04%2B-E95420?logo=ubuntu&logoColor=white)](https://ubuntu.com)

A lightweight [Envio](https://envio.dev) indexer that rebuilds Polymarket's per-wallet positions (**`UserPosition`**)
and trades (**`OrderFilled`**) from Polygon, following the conventions of
[Polymarket's Data API](https://data-api.polymarket.com/v2/positions). It's highly accurate: **97.76%** of positions
match the Data API exactly, and **98.60%** match or differ only by dust or a convention
([Accuracy](#accuracy-against-the-data-api)). A supervisor takes Parquet checkpoints at fixed blocks and can prune
finalized rows to keep the indexer's Postgres relatively small.

Project setup, prerequisites and development guides are in [CONTRIBUTING.md](CONTRIBUTING.md).

If this project is useful to you, please consider giving it a ⭐ on
[GitHub](https://github.com/psyberck/polymarket-subgraph-v2). It helps others find it, and it's much appreciated.

## Features

- **UserPosition and OrderFilled**: per-wallet `UserPosition` and `OrderFilled` from Polygon ([Data model](#data-model))
- **Accurate**: exact match **97.76%**, nearly identical **98.60%**, optimal **99.68%** with Polymarket's Data API
  ([Accuracy](#accuracy-against-the-data-api))
- **Verified checkpoints** at fixed block intervals ([Checkpoint supervisor](#checkpoint-supervisor))
- **Restorable** from any saved checkpoint ([Restoring a checkpoint](#restoring-a-checkpoint))
- **Continuous pruning** constantly keeps Postgres small ([Pruning](#pruning))
- **Optional RPC proxy** that absorbs rate limits with a free-tier RPC provider ([Running](#running))

## Contents

- [Data model](#data-model)
- [Running](#running)
- [Checkpoint supervisor](#checkpoint-supervisor)
- [Restoring a checkpoint](#restoring-a-checkpoint)
- [Accuracy against the Data API](#accuracy-against-the-data-api)
- [Tests](#tests)

## Data model

| Entity         | Storage              | Contents                                                                                 |
| -------------- | -------------------- | ---------------------------------------------------------------------------------------- |
| `UserPosition` | Postgres, ClickHouse | One row per `{user}-{tokenId}`: balance, average price, fees, realized PnL, total bought |
| `OrderFilled`  | Postgres, ClickHouse | One row per exchange fill, `{blockNumber}-{logIndex}`                                    |
| `Condition`    | Postgres             | Outcome count, neg-risk membership, `payoutPrices` once resolved                         |
| `OutcomeToken` | Postgres             | tokenId → condition, outcome index, collateral                                           |
| `NegRiskEvent` | Postgres             | Question count per neg-risk market                                                       |

The contracts indexed are `CTFExchangeV2`, `ConditionalTokens`, `NegRiskAdapter`, `NegRiskCtfCollateralAdapter`,
`CtfCollateralAdapter` and `AutoRedeemer` (`config.yaml`).

`Condition`, `OutcomeToken` and `NegRiskEvent` are mostly preloaded by `scripts/init_conditions.ts`.

## Running

```sh
nohup ./run_indexer.sh --proxy --output-dir ./checkpoints --schedule monthly --prune >> logs/run.log 2>&1 & disown
```

1. **Proxy**: `--proxy` starts the RPC proxy (`scripts/rpc_proxy.mjs`, logging to `logs/proxy.log`) unless one is
   already listening on `PROXY_PORT`. It outlives the indexer; stop it with the `kill` command the script prints.
2. **Indexer**: the script then runs the supervisor, which runs Envio, and prints its pid. Keep the `disown` if you
   want to keep it running in the background: Node resets SIGHUP, so `nohup` alone doesn't survive a logout.

### Flags

| Flag                                 | Meaning                                                                                                                                                          |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--output-dir <path>`                | Required. Checkpoint folders are written here; it is created if missing.                                                                                         |
| `--schedule monthly\|weekly\|stride` | Optional. `weekly` = 302,400 blocks, `monthly` = 1,296,000 blocks (fixed strides at 2 s/block). Without it, Envio runs to the head and no checkpoints are taken. |
| `--stride <blocks>`                  | Required with `--schedule stride`, invalid otherwise.                                                                                                            |
| `--prune`                            | After each checkpoint, delete the rows it archived (see [Pruning](#pruning)).                                                                                    |
| `--debug`                            | Only with `--prune`: log what would be deleted, delete nothing.                                                                                                  |

| Environment variable  | Default | Meaning                                    |
| --------------------- | ------- | ------------------------------------------ |
| `DUCKDB_MEMORY_LIMIT` | `2GB`   | DuckDB memory cap for exports and restores |
| `DUCKDB_THREADS`      | `2`     | DuckDB worker threads                      |

Invalid flags exit with code 64 and print the usage.

### Stopping

`kill <supervisor pid>` (SIGTERM), Ctrl-C (SIGINT) or a hang-up (SIGHUP, e.g. closing the terminal of a run started
without `disown`) stops Envio's whole process group, then the supervisor exits 0.

| Exit code | Meaning                                                                                                                       |
| --------- | ----------------------------------------------------------------------------------------------------------------------------- |
| 0         | Stopped by SIGTERM, SIGINT or SIGHUP                                                                                          |
| 1         | Envio exited non-zero (the supervisor does not restart it), another supervisor holds the lock, or a fatal error               |
| 2         | A stored block hash doesn't match the chain: no checkpoint was taken and Envio is left stopped; restore an earlier checkpoint |
| 64        | Invalid flags or environment                                                                                                  |

## Checkpoint supervisor

### Features

- **Single owner**: takes the supervisor lock at start and stops any orphaned indexer.
- **Runs in steps**: sets `end_block` to the next checkpoint block and resumes the indexer until it gets there.
- **Idempotent**: skips a checkpoint whose manifest already exists.
- **Optional pruning**: with `--prune`, deletes the rows each checkpoint archived.
- **Post-processing hook**: runs once per checkpoint after it is written (and pruned); see
  [Post-processing](#post-processing).

### Checkpoint layout

Each checkpoint is a folder named after its block:

```
<output-dir>/<endBlockId>/
  OrderFilled.parquet              trades in (startBlockId, endBlockId]
  UserPosition.finalized.parquet   finalized positions (market already resolved and prune deletes these)
  UserPosition.parquet             restore point: every other position (subject to change due to trade or market events)
  NegRiskEvent.parquet             restore point
  Condition.parquet                restore point
  OutcomeToken.parquet             restore point
  manifest.json                    metadata, written last; a folder without it is incomplete
```

I'll open-source my checkpoints in the near future, so you can [restore](#restoring-a-checkpoint) any of them and
resume indexing from there.

**Finalized position**: `amount = 0` on a resolved market (`Condition.payoutPrices IS NOT NULL`). Positions with a
non-zero balance don't qualify. A closed position on an open market can be re-entered, so it doesn't qualify either.
The two position files are exact complements: the restore point selects `(finalized) IS NOT TRUE`, so a position with
a null `conditionId` is never finalized.

### Pruning

With `--prune`, after each checkpoint is written:

1. Delete the ids in `UserPosition.finalized.parquet`, then `OrderFilled.parquet`, from Postgres in committed batches
   of 10,000.
2. Fail if any row still matches a file's filter: the archive missed it, so it is kept.
3. Delete all `SplitMarker` rows: each is read only within its own transaction, so none is needed at a block boundary.

### Post-processing

`processAt` in `supervisor/process.ts` is a placeholder for whatever each checkpoint should feed: an upload, an
analysis, or a notification for you to customize.

## Restoring a checkpoint

```sh
# 1. a fresh schema whose start block is the checkpoint block + 1
ENVIO_PG_SCHEMA=<schema> ENVIO_START_BLOCK=<endBlockId + 1> \
  pnpm envio local db-migrate up
# 2. load the restore point
ENVIO_PG_SCHEMA=<schema> tsx scripts/restore.ts <output-dir>/<endBlockId>
# 3. resume (with the same ENVIO_START_BLOCK)
./run_indexer.sh --output-dir <new output dir> --schedule ... [--prune]
```

## Accuracy against the Data API

Every indexed position was compared with the Data API, which is the reference for what Polymarket shows its users.

- **Indexer settings**: wallet-to-wallet transfers (`INDEX_INCLUDE_P2P_TRANSFERS=false`, the default).

### Results

| Measure                                                          | Exact match rate |
| ---------------------------------------------------------------- | ---------------- |
| Positions listed by the API, _INDEX_INCLUDE_P2P_TRANSFERS=False_ | **97.76%**       |
| Positions listed by the API, _INDEX_INCLUDE_P2P_TRANSFERS=True_  | **98.83%**       |

A breakdown of all positions by outcome (rates rounded to 2 decimal places):

| Outcome                                                                                                  | Rate       |
| -------------------------------------------------------------------------------------------------------- | ---------- |
| Exact match                                                                                              | **97.76%** |
| Wallet-to-wallet transfers not indexed (_INDEX_INCLUDE_P2P_TRANSFERS=True_ solves)                       | **1.08%**  |
| Balance differs by less than 0.1 token                                                                   | 0.50%      |
| PnL split differently across a condition's outcomes (yes/no tokens), but the total condition PnL matches | 0.33%      |
| Rounding issues: exactly 0.1 token left; `avgFeePrice` rounded down on every buy                         | 0.26%      |
| Neg-risk conversion pricing                                                                              | 0.06%      |
| PnL differs by less than 0.05 USDC                                                                       | 0.01%      |

1. **Wallet-to-wallet transfers.** Direct transfers between wallets aren't booked, so the sender keeps the tokens in
   the indexer while the API closes them. `INDEX_INCLUDE_P2P_TRANSFERS=true` books them at the sender's cost and
   improves the match rate to **98.83%**, **but adds about 60% more RPC load, so be cautious with a paid RPC
   provider.**
2. **Neg-risk conversion pricing.** YES tokens from converting NO tokens get a different cost than in the API; over 90%
   of these differences involve a `PositionsConverted` event.
3. **`avgFeePrice` precision.** The per-token fee is rounded down on every buy, so small buys on a large position drop
   their fee: 0.000001 × 20,379 tokens = 0.02 USDC instead of 0.33. `feesPaid` still matches the API exactly.

**Conclusion**: with the default settings, **98.60%** of positions match exactly or differ only by a convention or by
dust (below 0.1 token or 0.05 USDC). Wallet-to-wallet transfers cover another 1.08% and are handled by
`INDEX_INCLUDE_P2P_TRANSFERS=true`, which adds up to **99.68%**. The remaining 0.32% are known indexer issues: the
0.1-token rounding, `avgFeePrice` precision and neg-risk conversion pricing.

## Tests

```sh
node --test supervisor/tests/*.test.ts   # 51 unit tests: config, chain, typemap, snapshot, prune, process
pnpm typecheck && pnpm lint
```
