import aiohttp
from indexer.chain_mapper import CosmosAPI


async def get_block(
    session: aiohttp.ClientSession, api: CosmosAPI, height: str = "latest"
):
    async with session.get(
        f"{api.url}/cosmos/base/tendermint/v1beta1/blocks/{height}"
    ) as resp:
        return await resp.json()


async def get_txs(session: aiohttp.ClientSession, api: CosmosAPI, height: str):
    async with session.get(
        f"{api.url}/cosmos/tx/v1beta1/txs?events=tx.height={height}"
    ) as resp:
        return await resp.json()
