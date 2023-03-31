import asyncio, json, traceback, aiohttp
from dataclasses import dataclass, field
from typing import List

from indexer.exceptions import APIResponseError


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
    time_between_blocks: int = 1

    async def is_valid_response(self, resp: aiohttp.ClientResponse) -> bool:
        # could we return specific error messages here to save to db?
        try:
            return (
                list((await self.get_json(resp)).keys())
                != ["code", "message", "details"]
                and resp.status == 200
            )
        except Exception as e:
            return False

    async def get_json(self, resp: aiohttp.ClientResponse) -> dict:
        return json.loads(await resp.read())

    async def _get(
        self,
        endpoint: str,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        max_retries: int,
    ) -> dict | None:
        retries = 0
        while retries < max_retries:
            try:
                async with sem:
                    async with session.get(
                        f"{self.apis[self.current_api_index]}{endpoint}"
                    ) as resp:
                        if await self.is_valid_response(resp):
                            self.apis_hit[self.current_api_index] += 1
                            return await self.get_json(resp)
                        else:
                            raise APIResponseError("API Response Not Valid")
            except APIResponseError as e:
                print(f"failed to get {self.apis[self.current_api_index]}{endpoint}")
                self.apis_miss[self.current_api_index] += 1
                self.current_api_index = (self.current_api_index + 1) % len(self.apis)
                # save error to db
            retries += 1
        return None

    async def get_block(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        height: str = "latest",
        max_retries=5,
    ) -> dict | None:
        return await self._get(
            self.blocks_endpoint.format(height), session, sem, max_retries
        )

    async def get_block_txs(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        height: str,
        max_retries=5,
    ) -> dict | None:
        return await self._get(
            self.txs_endpoint.format(height), session, sem, max_retries
        )

    async def get_batch_txs(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        min_height: str,
        max_height: str,
        max_retries=5,
    ) -> dict | None:
        return await self._get(
            self.txs_batch_endpoint.format(min_height, max_height),
            session,
            sem,
            max_retries,
        )
