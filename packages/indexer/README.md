# Indexer

For a detailed structure of how this indexer works look at this [data flow model](https://whimsical.com/secret-network-indexer-data-flow-LRX17PwCNqsaNFP9ezHDa1])

## Running the indexer

If you want to run the indexer for development, you can use the following command:

```bash
docker compose run --build --rm indexer sh
```

If you want to run the indexer in the background to use it's data, you can use the following command:

```bash
docker compose up -d
```
