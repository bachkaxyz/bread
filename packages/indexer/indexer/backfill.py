import asyncio
from asyncio import Future
from typing import List, Generator, Awaitable
from aiohttp import ClientSession
from asyncpg import Pool, Connection
from indexer.chain import CosmosChain
from indexer.db import missing_blocks_cursor, wrong_tx_count_cursor, upsert_data
from indexer.parser import Raw, process_tx, process_block

min_block_height = 116001


async def run_and_upsert_tasks(raw_tasks: List[Future | Generator | Awaitable], pool: Pool):
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
    print("starting backfill")
    async with pool.acquire() as cursor_conn:
        cursor_conn: Connection

        # we are using transactions here since a cursor is used
        async with cursor_conn.transaction():
            raw_tasks = []
            # check for wrong tx counts (where the tx count in the block header does not match the number of txs in the block)
            async for (height, block_tx_count, chain_id) in wrong_tx_count_cursor(
                cursor_conn, chain
            ):
                print(f"{height}, {block_tx_count}")
                raw = Raw(
                    height=height,
                    block_tx_count=block_tx_count,
                    chain_id=chain_id,
                )
                # since the block has already been processed, we can just process the txs 
                
                raw_tasks.append(process_tx(raw, session, chain))

                # we are batching the txs to be processed for performance
                if len(raw_tasks) > 20:
                    await run_and_upsert_tasks(raw_tasks, pool)
                    raw_tasks = []

            # process the remaining txs
            await run_and_upsert_tasks(raw_tasks, pool)
            raw_tasks = []
            
            # check for missing blocks
            async for (height, dif) in missing_blocks_cursor(cursor_conn, chain):
                print(f"{height=} {dif=}")
                
                # if dif is -1, then the block is the lowest block in the database
                if dif == -1:
                    print("min block in db")

                    # if the lowest block in the database is the min block height, then we are done
                    lowest_height = await chain.get_lowest_height(session)

                    # if the lowest block in the database is not the min block height, then we need to backfill the missing blocks
                    # we are only backfilling 20 blocks at a time to avoid timeouts and to backfill more recent blocks first
                    if height - 20 > lowest_height:
                        dif = 20
                    else:
                        dif = height - lowest_height
                print(height, dif)
                
                # we are querying 10 blocks at a time to avoid timeouts
                step_size = 10 
                max_height = height - 1
                min_height = height - dif # the min height to query during this iteration
                current_height = max_height
                # query in batches while the current height is greater than the min height
                while current_height > min_height:
                    print(f"{current_height}")
                    
                    # check if the next iteration will be less than the min height and set lower bound accordingly
                    if current_height - step_size > min_height:
                        query_lower_bound = current_height - step_size
                    else:
                        query_lower_bound = min_height
                    print(f"querying range {current_height} - {query_lower_bound}")
                    
                    # query and process the blocks in the range
                    tasks = [
                        get_data_historical(session, chain, h)
                        for h in range(current_height, query_lower_bound, -1)
                    ]
                    await run_and_upsert_tasks(tasks, pool)

                    print("data upserted")
                    current_height = query_lower_bound
    print("finish backfill task")


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
    print(f"pulling new data {height}")
    block_res_json = await chain.get_block(session, height=height)
    print(f"block returned {height}")
    if block_res_json is not None:
        return await process_block(block_res_json, session, chain)
    else:
        print("block data is None")
        return None
