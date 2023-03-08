# Indexer

For a detailed structure of how this indexer works look at this [data flow model](https://whimsical.com/secret-network-indexer-data-flow-LRX17PwCNqsaNFP9ezHDa1])

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
- We don't use environment variables and use ones set inside of the file
- Change command to the pytest command

Run

```bash
docker compose -f docker-compose.yaml -f docker-compose.tests.yaml run indexer
```

This is the same setup for testing locally and on github actions

We have a doc

If you want to run the indexer for development, you can use the following command:

```bash
docker compose run --build --rm indexer sh
```

If you want to run the indexer in the background to use it's data, you can use the following command:

```bash
docker compose up -d
```

To run tests:
