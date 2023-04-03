import asyncio, json, traceback, aiohttp
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from indexer.exceptions import APIResponseError

ChainApiResponse = Tuple[str | None, dict | None]


@dataclass
class CosmosChain:
    min_block_height: int
    chain_id: str
    blocks_endpoint: str
    txs_endpoint: str
    txs_batch_endpoint: str
    apis: Dict[str, Dict[str, int]]
    current_api_index: int = 0
    time_between_blocks: int = 1

    async def is_valid_response(self, resp: aiohttp.ClientResponse) -> bool:
        """Check if the response is in the correct format

        Args:
            resp (aiohttp.ClientResponse): raw response from the api call to check

        Returns:
            bool: True if the response is valid, False otherwise
        """
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

    def get_next_api(self):
        return list(self.apis.keys())[self.current_api_index]

    def add_api_miss(self, api: str):
        index = list(self.apis.keys()).index(api)
        list(self.apis.values())[index]["miss"] += 1

    def add_api_hit(self, api: str):
        index = list(self.apis.keys()).index(api)
        list(self.apis.values())[index]["hit"] += 1

    def iterate_api(self):
        self.current_api_index = (self.current_api_index + 1) % len(self.apis)

    def remove_api(self, api: str):
        """Remove an api from the list of apis

        Args:
            api (str): api to remove from the list of apis
        """
        try:
            api_index = self.apis.pop(api)
        except KeyError:
            # if api is not in list of apis, do nothing
            pass

    async def _get(
        self,
        endpoint: str,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        max_retries: int,
    ) -> ChainApiResponse:
        """Get data from an endpoint with retries

        Args:
            endpoint (str): endpoint to
            session (aiohttp.ClientSession): _description_
            sem (asyncio.Semaphore): _description_
            max_retries (int): _description_

        Raises:
            APIResponseError: _description_

        Returns:
            Tuple[str, dict] | None: str is the api that was hit, dict is the response if valid, otherwise None
        """
        retries = 0

        while retries < max_retries:
            cur_api = self.get_next_api()
            try:
                async with sem:
                    async with session.get(f"{cur_api}{endpoint}") as resp:
                        if await self.is_valid_response(resp):
                            self.add_api_hit(cur_api)
                            return cur_api, await self.get_json(resp)
                        else:
                            raise APIResponseError("API Response Not Valid")
            except APIResponseError as e:
                print(f"failed to get {cur_api}{endpoint}")

                self.add_api_miss(cur_api)
                self.iterate_api()

            retries += 1
        return None, None

    async def get_block(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        height: str = "latest",
        max_retries=5,
    ) -> dict | None:
        """Get block data from an api

        Args:
            session (aiohttp.ClientSession): ClientSession to use for the request
            sem (asyncio.Semaphore): Semaphore to use for the request
            height (str, optional): height of the block to get. Defaults to "latest".
            max_retries (int, optional): max number of retries to make. Defaults to 5.

        Returns:
            dict | None: block data if valid, otherwise None
        """
        api_res = await self._get(
            self.blocks_endpoint.format(height), session, sem, max_retries
        )
        return self.verify_and_return_block_data(api_res)

    def verify_and_return_block_data(self, api_res: ChainApiResponse) -> dict | None:
        """Verify that the block data is from the correct chain

        Args:
            api_res (ChainApiResponse): api that was hit and the response data

        Returns:
            dict | None: block data if valid, otherwise None
        """
        api, data = api_res
        if data is None or api is None:
            return None

        if self.chain_id == data["block"]["header"]["chain_id"]:
            return data

        print(api_res)
        self.remove_api(api)
        return None

    async def get_block_txs(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        height: str,
        max_retries=5,
    ) -> dict | None:
        """Get transactions from a block

        Args:
            session (aiohttp.ClientSession): ClientSession to use for the request
            sem (asyncio.Semaphore): Semaphore to use for the request
            height (str): height of the block to get transactions from
            max_retries (int, optional): max number of retries to make. Defaults to 5.

        Returns:
            dict | None: transactions if valid, otherwise None
        """
        api, data = await self._get(
            self.txs_endpoint.format(height), session, sem, max_retries
        )
        return data

    async def get_batch_txs(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        min_height: str,
        max_height: str,
        max_retries=5,
    ) -> dict | None:
        """Get transactions from a range of blocks

        Args:
            session (aiohttp.ClientSession): ClientSession to use for the request
            sem (asyncio.Semaphore): Semaphore to use for the request
            min_height (str): Lower bound on heights to get transactions from
            max_height (str): Upper bound on heights to get transactions from
            max_retries (int, optional): max number of retries to make. Defaults to 5.

        Returns:
            dict | None: transactions if valid, otherwise None
        """
        api, data = await self._get(
            self.txs_batch_endpoint.format(min_height, max_height),
            session,
            sem,
            max_retries,
        )
        return data
