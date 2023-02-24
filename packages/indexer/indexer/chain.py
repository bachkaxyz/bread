import asyncio, json, traceback, aiohttp
from dataclasses import dataclass, field
from typing import List


@dataclass
class CosmosChain:
    min_block_height: int
    chain_id: str
    blocks_endpoint: str
    txs_endpoint: str
    txs_batch_endpoint: str
    apis: List[str] = field(default_factory=list)
    apis_hit: List[int] = field(default_factory=list)
    apis_miss: List[int] = field(default_factory=list)
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
                        self.apis_hit[self.current_api_index] += 1
                        return await resp.json()
            except Exception as e:
                # print(e.__traceback__.tb_lineno, e)
                print(
                    f"failed to get block {height} from {self.apis[self.current_api_index]}"
                )
                self.apis_miss[self.current_api_index] += 1
                self.current_api_index = (self.current_api_index + 1) % len(self.apis)
                # save error to db
            retries += 1
        return None

    async def get_block_txs(
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
                        self.apis_hit[self.current_api_index] += 1
                        res = json.loads(await resp.read())
                        if list(res.keys()) != ["code", "message", "details"]:
                            return res
            except Exception as e:
                self.apis_miss[self.current_api_index] += 1
                self.current_api_index = (self.current_api_index + 1) % len(self.apis)
                # save error to db
            retries += 1
        print(f"failed to get block {height} from {self.apis[self.current_api_index]}")
        return None

    async def get_batch_txs(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        min_height: str,
        max_height: str,
        max_retries=5,
    ) -> dict | None:
        retries = 0
        while retries < max_retries:
            try:
                async with sem:
                    url = f"{self.apis[self.current_api_index]}{self.txs_batch_endpoint.format(min_height, max_height)}"
                    resp = await session.get(url)
                    if resp.status == 200:
                        self.apis_hit[self.current_api_index] += 1
                        return json.loads(await resp.read())
                    print(
                        f"{resp.status} {self.apis[self.current_api_index]}{self.txs_batch_endpoint.format(min_height, max_height)} {(await resp.read())}"
                    )
            except Exception as e:
                self.apis_miss[self.current_api_index] += 1
                self.current_api_index = (self.current_api_index + 1) % len(self.apis)
                print(traceback.format_exc())
                # save error to db
            retries += 1

        print(f"failed to get batch txs from {self.apis[self.current_api_index]}")
        return None
