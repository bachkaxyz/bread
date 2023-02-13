import asyncio
from dataclasses import dataclass, field
from typing import Dict, List

import aiohttp


@dataclass
class CosmosChain:
    min_block_height: int
    blocks_endpoint: str
    txs_endpoint: str
    apis: List[str] = field(default_factory=list)
    current_api_index: int = 0

    async def get_block(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        height: str = "latest",
        max_retries=5,
    ) -> dict | None:
        retries = 0
        while retries < max_retries:
            try:
                async with sem:
                    async with session.get(
                        f"{self.apis[self.current_api_index]}{self.blocks_endpoint.format(height)}"
                    ) as resp:
                        return await resp.json()
            except Exception as e:
                # print(e.__traceback__.tb_lineno, e)
                print(
                    f"failed to get block {height} from {self.apis[self.current_api_index]}"
                )
                # save error to db
            retries += 1
        return None

    async def get_txs(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        height: str,
        max_retries=5,
    ) -> dict | None:
        retries = 0
        while retries < max_retries:
            try:
                async with sem:
                    async with session.get(
                        f"{self.apis[self.current_api_index]}{self.txs_endpoint.format(height)}"
                    ) as resp:
                        return await resp.json()
            except Exception as e:
                print(
                    f"failed to get block {height} from {self.apis[self.current_api_index]}"
                )
                # save error to db
            retries += 1
        return None


# chain_mapping: List[CosmosChain] = [
#     CosmosChain(
#         chain_id="secret-4",
#         min_block_height=7284419,
#         apis=[
#             "https://secret-4.api.trivium.network:1317",
#         ],
#     ),
# CosmosChain(
#     chain_id="jackal-1",
#     min_block_height=1,
#     apis=[
#         CosmosAPI(url="https://api.jackalprotocol.com"),
#         CosmosAPI(url="https://jackal-api.polkachu.com"),
#         CosmosAPI(url=" https://api.jackal.nodestake.top"),
#     ],
# ),
# CosmosChain(
#     chain_id="akashnet-2",
#     min_block_height=9262196,
#     apis=[CosmosAPI(url="https://akash-api.polkachu.com")],
# ),
# ]
