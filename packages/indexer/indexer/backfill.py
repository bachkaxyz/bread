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
    results: List[Raw | None] = await asyncio.gather(*raw_tasks)
    upsert_tasks = []
    for res in results:
        if res:
            upsert_tasks.append(upsert_data(pool, res))
    if upsert_tasks:
        await asyncio.gather(*upsert_tasks)


async def backfill(session: ClientSession, chain: CosmosChain, pool: Pool):
    print("starting backfill")
    async with pool.acquire() as cursor_conn:
        cursor_conn: Connection

        async with cursor_conn.transaction():
            raw_tasks = []
            async for (height, block_tx_count, chain_id) in wrong_tx_count_cursor(
                cursor_conn, chain
            ):
                print(f"{height}, {block_tx_count}")
                raw = Raw(
                    height=height,
                    block_tx_count=block_tx_count,
                    chain_id=chain_id,
                )
                raw_tasks.append(process_tx(raw, session, chain))

                if len(raw_tasks) > 20:
                    await run_and_upsert_tasks(raw_tasks, pool)
                    raw_tasks = []
            await run_and_upsert_tasks(raw_tasks, pool)
            raw_tasks = []

            async for record in missing_blocks_cursor(cursor_conn, chain):
                height, dif = record
                print(f"{height=} {dif=}")
                if dif == -1:
                    print("min block in db")

                    lowest_height = await chain.get_lowest_height(session)

                    if height - 20 > lowest_height:
                        dif = 20
                    else:
                        dif = height - lowest_height
                print(height, dif)
                step_size = 10
                max_height = height - 1
                min_height = height - dif
                current_height = max_height
                while current_height > min_height:
                    print(f"{current_height}")
                    if current_height - step_size > min_height:
                        query_lower_bound = current_height - step_size
                    else:
                        query_lower_bound = min_height
                    print(f"querying range {current_height} - {query_lower_bound}")
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
    print(f"pulling new data {height}")
    block_res_json = await chain.get_block(session, height=height)
    print(f"block returned {height}")
    if block_res_json is not None:
        return await process_block(block_res_json, session, chain)
    else:
        print("block data is None")
        return None
