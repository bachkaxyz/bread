import aiohttp
from indexer.chain_mapper import CosmosAPI


async def get_block(
    session: aiohttp.ClientSession, api: CosmosAPI, height: str = "latest"
):
    async with session.get(
        f"{api.url}/cosmos/base/tendermint/v1beta1/blocks/{height}"
    ) as resp:
        return await resp.json()
