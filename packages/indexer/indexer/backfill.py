import asyncio
from asyncio import Future
from typing import Any, Coroutine, List, Generator, Awaitable
from aiohttp import ClientSession
from asyncpg import Pool, Connection
from indexer.chain import CosmosChain
from indexer.db import missing_blocks_cursor, wrong_tx_count_cursor, upsert_data
from indexer.parser import Raw, process_tx, process_block
import logging

min_block_height = 116001


async def run_and_upsert_tasks(
    raw_tasks: List[Coroutine[Any, Any, Raw | None]],
    pool: Pool,
):
    """Processing a list of coroutines and upserting the results  into the database.

    Args:
        `raw_tasks (List[Future  |  Generator  |  Awaitable])`: List of coroutines to run.
        `pool (Pool)`: Database connection pool.
    """
    results: List[Raw | None] = await asyncio.gather(*raw_tasks)
    upsert_tasks = []
    for res in results:
        if res:
            upsert_tasks.append(upsert_data(pool, res))
    if upsert_tasks:
        await asyncio.gather(*upsert_tasks)


async def backfill(session: ClientSession, chain: CosmosChain, pool: Pool):
    """Backfilling the database with historical data.

    Args:
        session (ClientSession): Client session for making HTTP requests.
        chain (CosmosChain): Chain object for making chain specific requests.
        pool (Pool): Database connection pool.
    """
    logger = logging.getLogger("indexer")
    logger.info("backfill - starting backfill")
    async with pool.acquire() as cursor_conn:
        cursor_conn: Connection

        # we are using transactions here since a cursor is used
        async with cursor_conn.transaction():
            raw_tasks = []
            # check for wrong tx counts (where the tx count in the block header does not match the number of txs in the block)
            async for (height, block_tx_count, chain_id) in wrong_tx_count_cursor(
                cursor_conn, chain
            ):
                logger.info(f"backfill - wrong tx count for {height}, {block_tx_count}")
                raw = Raw(
                    height=height,
                    block_tx_count=block_tx_count,
                    chain_id=chain_id,
                )
                # since the block has already been processed, we can just process the txs

                raw_tasks.append(process_tx(raw, session, chain))

                # we are batching the txs to be processed for performance
                if len(raw_tasks) > chain.batch_size:
                    await run_and_upsert_tasks(raw_tasks, pool)
                    raw_tasks = []

            # process the remaining txs
            await run_and_upsert_tasks(raw_tasks, pool)
            raw_tasks = []

            # check for missing blocks
            async for (height, dif) in missing_blocks_cursor(cursor_conn, chain):
                logger.info(f"backfill - missing block {height=} {dif=}")

                # if dif is -1, then the block is the lowest block in the database
                if dif == -1:
                    logger.info("backfill - min block in db reached")

                    # if the lowest block in the database is the min block height, then we are done
                    lowest_height = await chain.get_lowest_height(session)
                    logger.info(f"lowest height {lowest_height=}")

                    # if the lowest block in the database is not the min block height, then we need to backfill the missing blocks
                    # we are only backfilling 20 blocks at a time to avoid timeouts and to backfill more recent blocks first
                    if height - chain.batch_size > lowest_height:
                        dif = chain.batch_size
                    else:
                        dif = height - lowest_height
                logger.info(
                    f"backfill - after processing state of indexer {height=}, {dif=}"
                )

                # we are querying 10 blocks at a time to avoid timeouts
                max_height = height - 1
                min_height = (
                    height - dif
                )  # the min height to query during this iteration
                current_height = max_height
                # query in batches while the current height is greater than the min height
                while current_height > min_height:
                    # check if the next iteration will be less than the min height and set lower bound accordingly
                    if current_height - chain.step_size > min_height:
                        query_lower_bound = current_height - chain.step_size
                    else:
                        query_lower_bound = min_height
                    logger.info(
                        f"backfill - querying range {current_height} - {query_lower_bound}"
                    )

                    # query and process the blocks in the range
                    tasks: List[Coroutine] = [
                        get_data_historical(session, chain, h)
                        for h in range(current_height, query_lower_bound, -1)
                    ]
                    await run_and_upsert_tasks(tasks, pool)

                    logger.info("backfill - data upserted")
                    current_height = query_lower_bound
    logger.info("backfill - finish backfill task")


async def get_data_historical(
    session: ClientSession, chain: CosmosChain, height: int
) -> Raw | None:
    """Historical data processing for a single block.

    Args:
        session (ClientSession): Client session for making HTTP requests.
        chain (CosmosChain): Chain object for making chain specific requests.
        height (int): Block height to process.

    Returns:
        Raw | None: Raw data object or None if the block data is None.
    """
    block_res_json = await chain.get_block(session, height=height)
    logger = logging.getLogger("indexer")
    logger.info(f"block returned {height=}")
    if block_res_json is not None:
        return await process_block(block_res_json, session, chain)
    else:
        logger.info("block data is None")
        return None
