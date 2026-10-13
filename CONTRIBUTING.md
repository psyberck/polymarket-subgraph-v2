# Contributing

Thanks for helping out. This is a lightweight indexer built on top of [Envio](https://envio.dev) that builds
Polymarket `UserPosition` and `OrderFilled` data from Polygon and runs locally, with a supervisor that continuously
archives and prunes it. The [README](README.md) describes how the indexer and supervisor work together and what
has been validated.

## Prerequisites

- [pnpm](https://pnpm.io/installation)
- Node 24 (`.nvmrc`; `nvm use`). The supervisor runs its TypeScript directly under plain `node` and uses
  `using` / `await using`, which Node 22 rejects with a `SyntaxError`.
- Postgres: the local Envio container on `localhost:5433`, or
  a self-hosted instance configured through the `ENVIO_PG_*` variables.
- ClickHouse: the local Envio container on `localhost:8123` and `localhost:9000`. Required while
  `storage.clickhouse` is `true` in `config.yaml`, which it is by default.
- A Polygon RPC endpoint (`PROXY_UPSTREAM`), reached through the optional local proxy (`POLYGON_RPC_URL`). The
  supervisor uses it to check finality and block hashes and the indexer uses it to fetch raw chain events.

## Setup

```sh
pnpm install
cp .env.example .env   # then fill in the ClickHouse and RPC values
pnpm codegen           # regenerates .envio/ from config.yaml and schema.graphql
```

Rerun `pnpm codegen` after any change to `config.yaml` or `schema.graphql`.

## Initialization

Handlers look up every `tokenId` in `OutcomeToken` to find its condition. They read settled prices from `Condition`,
and neg-risk question counts from `NegRiskEvent`. Markets created before `start_block` never emit the events that
would fill these tables, so they are preloaded from Polymarket's Gamma API before indexing starts. Seed a new database
before the indexer first starts on it, and seed again whenever `start_block` moves.

The scripts read `.env` from the shell and run under tsx:

```sh
set -a; . ./.env; set +a
```

### 1. Fetch the market corpus

```sh
tsx scripts/fetch_conditions.ts --rebuild
```

- **What it does**: walks Gamma's `/events` and appends each event's markets to a local file
  `logs/markets/markets.jsonl`.
- **Why events**: `/markets` leaves out neg-risk placeholder markets ("Person X"), which can hold positions.
- **Resumable**: progress is saved in `logs/markets/meta.json`; a plain run fetches only newer events, `--rebuild`
  starts over. Use `--rebuild` before a full reseed (new markets can be added to events already fetched).

### 2. Load the seed

Create the schemas first; the seed reads `start_block` from them, so seed and indexer use the same block:

```sh
pnpm envio local db-migrate up
tsx scripts/init_conditions.ts
```

- **Guard**: refuses to run once `UserPosition` has rows; otherwise reloads `Condition`, `OutcomeToken` and
  `NegRiskEvent` from scratch.
- **Payouts**: Gamma prices count only for markets resolved before `start_block`; otherwise the on-chain
  `ConditionResolution` log is used.
- **RPC**: needs `eth_getLogs` for archive reads. Parsing runs in `WORKER_COUNT` threads (default: CPUs,
  max 8).
- **Verify**: `scripts/verify_conditions.ts` checks the tables and exits 0 when it is safe to launch the
  indexer; it exits 1 on any failure.

### 3. Start the indexer

Start it as described in [Running](#running). Markets created after the corpus was fetched are filled in by the
handlers as their events arrive.

## Running

```sh
nohup ./run_indexer.sh --proxy --output-dir ./checkpoints --schedule monthly --prune >> logs/run.log 2>&1 & disown
```

`run_indexer.sh` starts the supervisor, which runs the indexer internally; exit codes are in the
[README](README.md#stopping). Stop it with `kill <pid>` using the pid it prints.

Changing `INDEX_INCLUDE_P2P_TRANSFERS` requires a reindex.

## Code layout

| Path                | Contents                                                                               |
| ------------------- | -------------------------------------------------------------------------------------- |
| `config.yaml`       | Contracts, events, start block, RPC settings                                           |
| `schema.graphql`    | Entities; `@storage(clickhouse: true)` mirrors one to ClickHouse                       |
| `src/handlers/`     | One mapping file per contract                                                          |
| `src/utils/`        | Shared position math and ID helpers                                                    |
| `supervisor/`       | Checkpoint supervisor: schedule, archive, verify, prune, restore                       |
| `supervisor/tests/` | Supervisor unit tests (`node:test`)                                                    |
| `scripts/`          | RPC proxy, condition loaders, seeding, restore and verification tools                  |
| `tools/`            | Code shared by scripts and the supervisor: the Postgres pool, the DuckDB setup         |
| `run_indexer.sh`    | Launches the supervisor (and through it the indexer), and the RPC proxy with `--proxy` |

## Checks

Run these before opening a pull request:

```sh
pnpm lint
pnpm format:check   # or `pnpm format` to fix
pnpm typecheck
node --test supervisor/tests/*.test.ts   # supervisor unit tests, no database needed
```

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org) with a scope:

```
feat(negrisk-utils): add neg-risk utils for the neg-risk adapter
fix(avgPrice): split avgPrice into avgPrice and avgFeePrice
docs(comments): refine comments in the rpc proxy
```

Common types are `feat`, `fix`, `refactor`, `perf`, `test`, `docs` and `build`.

## Pull requests

Good pull requests (fixes, improvements, new features) are a big help. Keep each one focused on a single change, and
leave out unrelated commits.

**Please ask first** before starting a significant pull request: a change to position accounting, to `schema.graphql`,
or to what the supervisor archives or prunes. These can force a reindex or make old checkpoints unrestorable, so agree
on the approach before spending time on it.

Open every pull request against `main`. Topic branches are created from `main` and merged back into it.

Following this process is the best way to get your work merged:

1. Clone the repository, or update your clone:

   ```sh
   git clone git@github.com:psyberck/polymarket-subgraph-v2.git
   cd polymarket-subgraph-v2
   ```

2. Get the latest changes:

   ```sh
   git checkout main
   git pull origin main
   ```

3. Create a topic branch from `main`. Name it `<type>/<topic>`, with the same types as [commits](#commits), for
   example `feat/supervisor` or `fix/avg-fee-price`:

   ```sh
   git checkout -b <type>/<topic>
   ```

4. Make your change. Put code where [Code layout](#code-layout) says it belongs, and run `pnpm codegen` again if
   you touch `config.yaml` or `schema.graphql`.

5. Run the [checks](#checks): lint, format, typecheck and the supervisor unit tests. **TODO**: for a change to position
   accounting, we would also run an end-to-end test and reconcile the result with the Data API, and report the match
   rate before and after in the CI.

6. Commit your changes in logical chunks, following the [commit conventions](#commits). Tidy up your history before
   pushing, for example by squashing fixup commits.

7. Rebase your branch on the latest `main`:

   ```sh
   git pull --rebase origin main
   ```

8. Push your topic branch:

   ```sh
   git push -u origin <type>/<topic>
   ```

9. [Open a pull request][open-pr] against `main`, with a clear title in the commit format and a description that
   covers:
   - **What** changes, and **why**.
   - **How it was tested**: the checks run, and the block range of any throwaway run.
   - **Measurements**, when the change affects speed: timings, before and after.
   - **Operational impact**: whether a reindex, a new seed (`scripts/init_conditions.ts`) or a `.env` change is
     needed, and whether existing checkpoints can still be restored.

   Update the **README** in the same pull request when the change alters behaviour, flags, validation results or known
   limitations.

[open-pr]: https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
