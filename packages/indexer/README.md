# Indexer

For a detailed structure of how this indexer works look at this [data flow model](https://whimsical.com/secret-network-indexer-data-flow-LRX17PwCNqsaNFP9ezHDa1)

## Running the indexer

We have 4 cases where we would want to run the indexer using docker:

1. Production
2. Local Development
3. Testing

### Production

We have the base `docker-compose.yaml` file where the image is set to `indexer-prod` and a few other basic settings that all of the following docker compose files either use or inherit

### Local Development

We have a `docker-compose.local.yaml` file where we change and add the following:

- Image from `indexer-prod` -> `indexer-dev` (adds dev dependencies)
- Add root services `.env` file and indexer root `.env` for local environment variables
- We mount the local filesystem as a volume so you don't have to rebuild the image
- Set the network to have access to local postgres from root docker compose file

Run with the following command:

```bash
docker compose -f docker-compose.yaml -f docker-compose.local.yaml run indexer sh
```

### Testing

We have a `docker-compose.tests.yaml` that controls the configuration for testing.

- We add a new postgres service just for testing and we make our indexer depend on this service
- We don't use environment variables and use ones set inside of the docker compose file for consistency
- Change startup command to the pytest command

```bash
docker compose -f docker-compose.tests.yaml run indexer
```

This is the same setup for testing locally and on github actions

If you want to run the indexer for development, you can use the following command:

This launches the container with a bash shell. You can then start and stop the python scripts manually.

```bash
docker compose run --build --rm indexer sh
```

If you want to run the indexer in the background to use it's data, you can use the following command:

```bash
docker compose up -d
```

## Environment Variables

Look at .env.example for our preferred defaults, but for more specific explanations look below:

`BLOCKS_ENDPOINT` -  (optional) (requires one formatting place)  Endpoint to query a block information from. Defaults to:

`/cosmos/base/tendermint/v1beta1/blocks/{}`

`TXS_ENDPOINT` - (optional) (requires one formatting place) Endpoint to query a blocks transactions from. Defaults to:

`/cosmos/tx/v1beta1/txs?events=tx.height={}`

`CHAIN_REGISTRY_NAME` - (required) chain registry name from <https://github.com/cosmos/chain-registry>:

This is required so we load the correct chain_id

`LOAD_CHAIN_REGISTRY_APIS` - (optional) load apis from the chain registry. Defaults to: `true`

`INTERNAL_APIS` - (optional) load apis that aren't on the chain registry in a comma separate format. No default:

Example: `https://api.jackalprotocol.com,https://api.jackalprotocol.com`

`BATCH_SIZE` - (optional) batch size to backfill blocks in. Defaults to:`10`

`DROP_TABLES_ON_STARTUP` - (optional) Do we drop tables on indexer startup. Defaults to: `false`

`INDEXER_SCHEMA` (optional) - default schema to store data in. Defaults to: `public`

`USE_LOG_FILE` (optional) - whether logs get written to stdout or a file named `logger.log`. Defaults to `true`

Postgres Config from root environment variable:

```shell
POSTGRES_HOST=postgres
POSTGRES_PORT=5431
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```
