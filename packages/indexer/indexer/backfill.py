import asyncio
from asyncio import Future, Task
import json
import time
from typing import Any, Coroutine, List, Generator, Awaitable
from aiohttp import ClientSession
from asyncpg import Pool, Connection
from indexer.chain import CosmosChain
from indexer.db import (
    missing_blocks_cursor,
    wrong_tx_count_cursor,
    upsert_data,
    blob_upload_times,
    upsert_times,
)
from indexer.parser import Raw, process_tx, process_block
import logging
from google.cloud.storage import Bucket

min_block_height = 116001


async def run_and_upsert_tasks(
    raw_tasks: List[Task[Raw | None]], pool: Pool, bucket: Bucket
):
    """Processing a list of coroutines and upserting the results  into the database.

    Args:
        `raw_tasks (List[Task])`: List of coroutines to run.
        `pool (Pool)`: Database connection pool.
    """
    upsert_tasks = []
    for task in asyncio.as_completed(raw_tasks):
        raw = await task
        if raw:
            upsert_tasks.append(asyncio.create_task(upsert_data(pool, raw, bucket)))

    await asyncio.gather(*upsert_tasks)


while_times = []
hun_times = []


async def backfill(
    session: ClientSession, chain: CosmosChain, pool: Pool, bucket: Bucket
):
    global while_times, hun_times
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

                raw_tasks.append(asyncio.create_task(process_tx(raw, session, chain)))

            await run_and_upsert_tasks(raw_tasks, pool, bucket)

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
                hun_start_time = time.time()
                while current_height > min_height:
                    while_start_time = time.time()
                    # check if the next iteration will be less than the min height and set lower bound accordingly
                    if current_height - chain.step_size > min_height:
                        query_lower_bound = current_height - chain.step_size
                    else:
                        query_lower_bound = min_height
                    logger.info(
                        f"backfill - querying range {current_height} - {query_lower_bound}"
                    )

                    # query and process the blocks in the range
                    tasks: List[Task[Raw | None]] = [
                        asyncio.create_task(
                            get_data_historical(session, chain, h, pool)
                        )
                        for h in range(current_height, query_lower_bound, -1)
                    ]

                    await run_and_upsert_tasks(tasks, pool, bucket)

                    logger.info("backfill - data upserted")
                    while_end_time = time.time()
                    while_times.append(
                        (
                            while_end_time - while_start_time,
                            current_height - query_lower_bound,
                        )
                    )
                    logger.debug(
                        f"backfill - while loop took {while_end_time - while_start_time} seconds"
                    )
                    current_height = query_lower_bound
                    save_analytics(
                        hun_times, while_times, blob_upload_times, upsert_times, chain
                    )

                hun_end_time = time.time()
                hun_times.append((time.time() - hun_start_time, dif))
                save_analytics(
                    hun_times, while_times, blob_upload_times, upsert_times, chain
                )

    logger.info("backfill - finish backfill task")


def save_analytics(
    hun_times, while_times, blob_times, upsert_times, chain: CosmosChain
):
    api_usage = chain.get_api_usage()
    with open("usage.json", "w") as f:
        upsert_data, while_data, hun_data, blob_data = {}, {}, {}, {}
        just_times_hun = [t[0] for t in hun_times]
        just_times_while = [t[0] for t in while_times]
        if len(just_times_hun) > 0:
            hun_data = {
                "avg": sum(just_times_hun) / len(just_times_hun),
                "time_per_block": sum(just_times_hun) / sum([t[1] for t in hun_times]),
                "total": sum(just_times_hun),
                "number_times": len(just_times_hun),
            }

        if len(just_times_while) > 0:
            while_data = (
                {
                    "avg": sum(just_times_while) / len(just_times_while),
                    "time_per_block": sum(just_times_while)
                    / sum([t[1] for t in while_times]),
                    "total": sum(just_times_while),
                    "number_times": len(just_times_while),
                },
            )

        if len(blob_times) > 0:
            blob_data = (
                {
                    "avg": sum(blob_upload_times) / len(blob_upload_times),
                    "number_upload": len(blob_upload_times),
                    "total_time": sum(blob_upload_times),
                },
            )

        if len(upsert_times) > 0:
            upsert_data = {
                "avg": sum(upsert_times) / len(upsert_times),
                "number_upserts": len(upsert_times),
                "total_time": sum(upsert_times),
                "time_per_block": sum(upsert_times) / len(upsert_times),
            }

        data = {
            "hun_times": hun_data,
            "while_times": while_data,
            "blob_upload": blob_data,
            "upsert_times": upsert_data,
            "api_usage": api_usage,
        }
        json.dump(data, f)


async def get_data_historical(
    session: ClientSession, chain: CosmosChain, height: int, pool: Pool
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
