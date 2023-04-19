import asyncio
import io
import json
import logging
import os
from typing import Callable, Coroutine
from aiohttp import ClientSession

from asyncpg import Pool, create_pool
from indexer.backfill import backfill

from indexer.chain import get_chain_from_environment, CosmosChain
from indexer.db import create_tables, drop_tables
from indexer.live import live

from logging import Logger


async def run(
    pool: Pool,
    session: ClientSession,
    f: Callable[[ClientSession, CosmosChain, Pool], Coroutine],
):
    """
    The entry point of each process (live and backfill). This function controls the while loop that runs each portion of the indexer.
    The while loop is necessary because the indexer needs to run indefinitely.
    The function passed in is ran on each iteration of the loop.

    Args:
        pool (Pool): The database connection pool
        f (Callable[[ClientSession, CosmosChain, Pool], Coroutine]): The function to run on each iteration of the loop
    """
    chain = await get_chain_from_environment(session)
    while True:
        await f(session, chain, pool)
        await asyncio.sleep(chain.time_between_blocks)


async def main():
    """
    This function is the entry point for the indexer. It creates the database connection pool and runs both the live and backfill tasks.
    """
    schema_name = os.getenv("INDEXER_SCHEMA", "public")
    async with create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        server_settings={"search_path": schema_name},
    ) as pool:
        async with ClientSession(trust_env=True) as session:
            # initialize logger
            USE_LOG_FILE = os.getenv("USE_LOG_FILE", "TRUE").upper() == "TRUE"
            if USE_LOG_FILE is False:
                logging.basicConfig(
                    handlers=(logging.StreamHandler(),),
                    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S",
                    level=logging.DEBUG,
                )
            else:
                logging.basicConfig(
                    filename="indexer.log",
                    filemode="a",
                    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S",
                    level=logging.DEBUG,
                )

            # manage tables on startup if needed
            async with pool.acquire() as conn:
                DROP_TABLES_ON_STARTUP = (
                    os.getenv("DROP_TABLES_ON_STARTUP", "True").upper() == "TRUE"
                )
                if DROP_TABLES_ON_STARTUP:
                    await drop_tables(conn, schema_name)
                await create_tables(conn, schema_name)

            # start indexer
            await asyncio.gather(
                run(pool, session, live),
                run(pool, session, backfill),
            )


if __name__ == "__main__":
    asyncio.run(main())
