import asyncio
import io
import json
import logging
import os
import traceback
from typing import Any, Callable, Coroutine, Dict
import aiohttp

from asyncpg import Connection, connect
from indexer.backfill import backfill_historical, backfill_wrong_count

from indexer.chain import Manager, get_chain_from_environment, CosmosChain
from indexer.db import create_tables, drop_tables, setup_dirs
from indexer.live import live
from indexer.config import Config
from gcloud.aio.storage import Bucket, Storage
import asyncio


async def run(
    db_kwargs: Dict[str, Any],
    session_kwargs: Dict[str, Any],
    chain: CosmosChain,
    bucket: Bucket,
    f: Callable[[Manager, CosmosChain, Bucket], Coroutine],
):
    """
    The entry point of each process (live and backfill). This function controls the while loop that runs each portion of the indexer.
    The while loop is necessary because the indexer needs to run indefinitely.
    The function passed in is ran on each iteration of the loop.

    Args:
        pool (Pool): The database connection pool
        f (Callable[[ClientSession, CosmosChain, Pool], Coroutine]): The function to run on each iteration of the loop
    """
    async with Manager(db_kwargs, session_kwargs) as manager:
        while True:
            try:
                await f(manager, chain, bucket)
            except Exception as e:
                logger = logging.getLogger("indexer")
                logger.error(f"function error Exception in {f.__name__}: {e}")
                logger.error(traceback.format_exc())
            await asyncio.sleep(chain.time_between_blocks)


async def main():
    """
    This function is the entry point for the indexer. It creates the database connection pool and runs both the live and backfill tasks.
    """

    # this calls the function to ignore the aiohttp ssl error on the current event loop
    # ignore_aiohttp_ssl_eror(asyncio.get_running_loop())

    config = Config()
    await config.configure()

    # initialize logger
    if config.USE_LOG_FILE is False:
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
    conn: Connection = await connect(**config.db_kwargs)

    if config.DROP_TABLES_ON_STARTUP:
        logging.info("Dropping tables")
        await drop_tables(conn, config.schema_name)
    if config.CREATE_TABLES_ON_STARTUP:
        logging.info("Creating tables")
        await create_tables(conn, config.schema_name)
    await conn.close()

    # start indexer
    storage_client = Storage()
    bucket = storage_client.get_bucket(config.BUCKET_NAME)  # your bucket name

    # create temp file structure
    setup_dirs(config.chain)
    try:
        results = await asyncio.gather(
            run(config.db_kwargs, config.session_kwargs, config.chain, bucket, live),
            run(
                config.db_kwargs,
                config.session_kwargs,
                config.chain,
                bucket,
                backfill_historical,
            ),
            run(
                config.db_kwargs,
                config.session_kwargs,
                config.chain,
                bucket,
                backfill_wrong_count,
            ),
        )
        for e in results:
            logging.error("Exception in main loop")
            logging.error(e)
            logging.error(traceback.format_exc())
    except Exception as e:
        logging.error("Exception in main loop (try)")
        logging.error(e)
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
