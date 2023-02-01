import aiohttp
from indexer.chain_mapper import CosmosAPI


async def get_block(
    session: aiohttp.ClientSession,
    api: CosmosAPI,
    height: str = "latest",
    max_retries=5,
) -> dict | None:
    retries = 0
    while retries < max_retries:
        try:
            async with session.get(
                f"{api.url}/cosmos/base/tendermint/v1beta1/blocks/{height}"
            ) as resp:
                return await resp.json()
        except Exception as e:
            print(f"failed to get block {height} from {api.url}")
            # save error to db
        retries += 1
    return None


async def get_txs(
    session: aiohttp.ClientSession,
    api: CosmosAPI,
    height: str,
    max_retries=5,
) -> dict | None:
    retries = 0
    while retries < max_retries:
        try:
            async with session.get(
                f"{api.url}/cosmos/tx/v1beta1/txs?events=tx.height={height}"
            ) as resp:
                return await resp.json()
        except Exception as e:
            print(f"failed to get block {height} from {api.url}")
            # save error to db
        retries += 1
    return None
