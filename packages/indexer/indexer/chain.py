import json, traceback
from ssl import SSLError
import time
import os
from dataclasses import dataclass
from typing import Dict, List, Tuple, TypedDict
from aiohttp import ClientOSError, ClientResponse, ClientSession
import logging
from aiohttp.client_exceptions import ClientError
from aiohttp.client import _RequestContextManager
from indexer.exceptions import APIResponseError
from indexer.manager import Manager

ChainApiResponse = Tuple[str | None, dict | None]
LATEST = "latest"


class Api(TypedDict):
    hit: int
    miss: int
    times: List[float]


Apis = Dict[str, Api]


async def is_valid_response(r, resp: ClientResponse) -> bool:
    """Check if the response is in the correct format

    Args:
        resp (ClientResponse): raw response from the api call to check

    Returns:
        bool: True if the response is valid, False otherwise
    """
    # could we return specific error messages here to save to db?
    try:
        return (
            list(json.loads(r).keys()) != ["code", "message", "details"]
            and resp.status == 200
        )
    except Exception as e:
        return False


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


@dataclass
class CosmosChain(metaclass=Singleton):
    chain_id: str
    chain_registry_name: str
    blocks_endpoint: str
    txs_endpoint: str
    apis: Apis
    current_api_index: int = 0
    time_between_blocks: int = 1
    batch_size: int = 20
    step_size: int = 10

    def get_next_api(self) -> str:
        """Get the next api to hit"""
        if len(self.apis.values()) == 1:
            return list(self.apis.keys())[0]
        return list(self.apis.keys())[self.current_api_index]

    def add_api_miss(self, api: str, start_time: float, end_time: float):
        """Add a miss to the api"""
        index = list(self.apis.keys()).index(api)
        value = list(self.apis.values())[index]
        value["miss"] += 1
        value["times"].append(end_time - start_time)

    def add_api_hit(self, api: str, start_time: float, end_time: float):
        """Add a hit to the api"""
        index = list(self.apis.keys()).index(api)
        value = list(self.apis.values())[index]
        value["hit"] += 1
        value["times"].append(end_time - start_time)

    def iterate_api(self):
        """Iterate the current api index"""
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

    def get_api_usage(self):
        """Save the api usage to a file"""

        return [
            {
                "api": s_api,
                "hit": api["hit"],
                "miss": api["miss"],
                "average_time_per_call": sum(api["times"]) / len(api["times"]),
                "total_time": sum(api["times"]),
                "total_calls": api["hit"] + api["miss"],
                "hit_rate": api["hit"] / (api["hit"] + api["miss"]),
                "miss_rate": api["miss"] / (api["hit"] + api["miss"]),
            }
            for s_api, api in self.apis.items()
            if len(api["times"]) > 0
        ]

    async def _get(
        self,
        endpoint: str,
        manager: Manager,
        max_retries: int,
    ) -> ChainApiResponse:
        """Get data from an endpoint with retries

        Args:
            endpoint (str): endpoint to
            session (Manager): _description_
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
            start_time = time.time()
            try:
                async with manager.get(f"{cur_api}{endpoint}") as resp:
                    r = await resp.read()
                    if await is_valid_response(r, resp):
                        end_time = time.time()
                        self.add_api_hit(cur_api, start_time, end_time)
                        return cur_api, json.loads(r)
                    else:
                        raise APIResponseError("API Response Not Valid")
            except (
                BaseException,
                Exception,
                ClientError,
                ClientOSError,
                SSLError,
            ) as e:
                end_time = time.time()
                logger = logging.getLogger("indexer")
                logger.error(f"error {cur_api}{endpoint}\n{traceback.format_exc()}")

                self.add_api_miss(cur_api, start_time, end_time)
                self.iterate_api()
            retries += 1
        return None, None

    async def get_block(
        self,
        session: Manager,
        height: int | str = LATEST,
        max_retries=5,
    ) -> dict | None:
        """Get block data from an api

        Args:
            session (Manager): Manager to use for the request
            sem (asyncio.Semaphore): Semaphore to use for the request
            height (str, optional): height of the block to get. Defaults to "latest".
            max_retries (int, optional): max number of retries to make. Defaults to 5.

        Returns:
            dict | None: block data if valid, otherwise None
        """
        api_res = await self._get(
            self.blocks_endpoint.format(height), session, max_retries
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

        try:
            if self.chain_id == data["block"]["header"]["chain_id"]:
                return data
        except:
            return None
        return None

    async def get_block_txs(
        self,
        session: Manager,
        height: int | str,
        max_retries=5,
    ) -> dict | None:
        """Get transactions from a block

        Args:
            session (Manager): Manager to use for the request
            sem (asyncio.Semaphore): Semaphore to use for the request
            height (str): height of the block to get transactions from
            max_retries (int, optional): max number of retries to make. Defaults to 5.

        Returns:
            dict | None: transactions if valid, otherwise None
        """
        api, data = await self._get(
            self.txs_endpoint.format(height), session, max_retries
        )
        return data

    async def get_lowest_height(self, session: Manager):
        async with session.get(
            f"{self.get_next_api()}{self.blocks_endpoint.format(1)}"
        ) as block_res:
            block_res_json = json.loads(await block_res.read())
            lowest_height = (
                block_res_json["message"].split("height is ")[-1].split(":")[0].lstrip()
            )
            return int(lowest_height)


async def query_chain_registry(session: ClientSession, chain_registry_name: str):
    async with session.get(
        url=f"https://raw.githubusercontent.com/cosmos/chain-registry/master/{chain_registry_name}/chain.json"
    ) as raw:
        return json.loads(await raw.read())


async def get_chain_registry_info(
    session: ClientSession, chain_registry_name: str
) -> Tuple[str, List[str]]:
    """Pull Chain Info from Cosmos Chain Registry

    Args:
        session (Manager): Manager to use for the request
        chain_registry_name (str): Name of chain in Cosmos Chain Registry

    Returns:
        Tuple[str, List[str]]: Chain_id, list of apis
    """
    raw_chain = await query_chain_registry(session, chain_registry_name)
    rest_apis = raw_chain["apis"]["rest"]
    apis: List[str] = [api["address"] for api in rest_apis]
    chain_id: str = raw_chain["chain_id"]
    return chain_id, apis


async def get_chain_info(session: ClientSession) -> Tuple[str, str, Apis]:
    """Get chain info from environment variables

    Args:
        session (Manager): Aiohttp Session to query from

    Raises:
        EnvironmentError: If environment variables are not set

    Returns:
        Tuple[str, str, APIS]: Chain ID, Chain Registry Name, Apis
    """
    chain_registry_name = os.getenv("CHAIN_REGISTRY_NAME", None)
    if chain_registry_name is None and not chain_registry_name:
        raise EnvironmentError(
            "CHAIN_REGISTRY_NAME environment variable not provided. This is needed to load the correct chain_id"
        )
    load_external_apis = os.getenv("LOAD_CHAIN_REGISTRY_APIS", "True").upper() == "TRUE"
    logger = logging.getLogger("indexer")
    apis = set()  # don't add duplicate apis
    chain_id, chain_registry_apis = await get_chain_registry_info(
        session, chain_registry_name
    )
    logger.info(load_external_apis)
    if load_external_apis:
        logger.info("added external")
        [apis.add(api) for api in chain_registry_apis]

    env_apis = os.getenv("APIS", "")
    if env_apis != "":
        [apis.add(api) for api in env_apis.split(",")]

    if len(apis) == 0:
        raise EnvironmentError(
            "No APIS. Either provide your own apis through APIS or turn LOAD_CHAIN_REGISTRY_APIS to True"
        )

    # replace https with http to try and fix SSL errors
    http_apis = [api.replace("https://", "http://") for api in apis]
    formatted_apis = {api: Api({"hit": 0, "miss": 0, "times": []}) for api in http_apis}
    return chain_id, chain_registry_name, formatted_apis


async def remove_bad_apis(
    session: ClientSession, apis: Apis, blocks_endpoint: str
) -> Apis:
    api_heights: List[Tuple[str, Api, int]] = []
    for i, (api, hit_miss) in enumerate(apis.items()):
        try:
            async with session.get(f"{api}{blocks_endpoint.format('latest')}") as res:
                j = json.loads(await res.read())
                api_heights.append((api, hit_miss, int(j["block"]["header"]["height"])))
        except Exception as e:
            logger = logging.getLogger("indexer")
            logger.info(f"Error with {api}: {e}")

    max_height = max([height for _, _, height in api_heights])
    difs = [
        (api, hit_miss, (max_height - height)) for api, hit_miss, height in api_heights
    ]

    not_outliers = filter(lambda x: x[2] < 10, difs)

    logger = logging.getLogger("indexer")
    logger.info(f"{not_outliers=}")

    d = {api: hit_miss for api, hit_miss, _ in not_outliers}
    return d


async def get_chain_from_environment(session: ClientSession) -> CosmosChain:
    """Creates a CosmosChain based on environment config

    I did not override __init__ due to potential other use cases for using the CosmosChain class to query chain data

    Args:
        session (Manager): session to query external requests from

    Raises:
        EnvironmentError: Error if environment variables are not defined

    Returns:
        CosmosChain: CosmosChain object configured from current environment
    """
    chain_id, chain_registry_name, apis = await get_chain_info(session)
    time_between = os.getenv("TIME_BETWEEN_BLOCKS", "1")
    batch_size = os.getenv("BATCH_SIZE", "20")
    step_size = os.getenv("STEP_SIZE", "10")
    blocks_endpoint = os.getenv(
        "BLOCKS_ENDPOINT", "/cosmos/base/tendermint/v1beta1/blocks/{}"
    )

    txs_endpoint = os.getenv(
        "TXS_ENDPOINT", "/cosmos/tx/v1beta1/txs?events=tx.height={}"
    )
    apis = await remove_bad_apis(session, apis, blocks_endpoint)

    try:
        time_between = int(time_between)
        batch_size = int(batch_size)
        step_size = int(step_size)
    except BaseException as e:
        raise EnvironmentError(
            "Either TIME_BETWEEN_BLOCKS, BATCH_SIZE OR STEP_SIZE is not of type int"
        )
    chain = CosmosChain(
        chain_id=chain_id,
        chain_registry_name=chain_registry_name,
        blocks_endpoint=blocks_endpoint,
        txs_endpoint=txs_endpoint,
        apis=apis,
        time_between_blocks=time_between,
        batch_size=batch_size,
        step_size=step_size,
    )
    return chain
