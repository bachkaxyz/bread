import asyncio
import os
from typing import Any, Awaitable, Callable, Coroutine
from aiohttp import ClientSession

from asyncpg import Pool, create_pool

from indexer.chain import get_chain_from_environment
from indexer.chain import CosmosChain


async def main(f: Callable[[ClientSession, CosmosChain, Pool], Coroutine]):
    schema_name = os.getenv("INDEXER_SCHEMA", "public")
    async with create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        server_settings={"search_path": schema_name},
    ) as pool:
        async with ClientSession() as session:
            chain = await get_chain_from_environment(session)
            while True:
                await f(session, chain, pool)
                await asyncio.sleep(chain.time_between_blocks)
