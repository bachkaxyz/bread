import asyncio
import json
import os
import traceback
from typing import Dict, List, Tuple
import aiohttp

import asyncpg
from indexer.chain import CosmosChain
from indexer.parser import Log


async def drop_tables(pool: asyncpg.pool):
    await pool.execute(
        """
        DROP TABLE IF EXISTS raw_blocks CASCADE;
        DROP TABLE IF EXISTS raw_txs CASCADE;
        DROP TABLE IF EXISTS blocks CASCADE;
        DROP TABLE IF EXISTS txs CASCADE;
        DROP TABLE IF EXISTS logs CASCADE;
        DROP TABLE IF EXISTS log_columns CASCADE;
        DROP TABLE IF EXISTS messages CASCADE;
        """
    )


async def get_table_cols(pool: asyncpg.pool, table_name):
    async with pool.acquire() as conn:
        cols = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1;
            """,
            table_name,
        )
        return [col["column_name"] for col in cols]


async def create_tables(pool: asyncpg.pool):
    cur_dir = os.path.dirname(__file__)
    async with pool.acquire() as conn:

        file_path = os.path.join(cur_dir, "sql/create_tables.sql")
        with open(file_path, "r") as f:
            await conn.execute(f.read())

        # create the parse_raw function on insert trigger of raw
        file_path = os.path.join(cur_dir, "sql/parse_raw.sql")
        with open(file_path, "r") as f:
            await conn.execute(f.read())

        file_path = os.path.join(cur_dir, "sql/log_triggers.sql")
        with open(file_path, "r") as f:
            await conn.execute(f.read())


async def upsert_raw_blocks(pool: asyncpg.pool, block: dict):
    async with pool.acquire() as conn:
        chain_id, height = block["block"]["header"]["chain_id"], int(
            block["block"]["header"]["height"]
        )
        await conn.execute(
            """
            INSERT INTO raw_blocks (chain_id, height, block)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            chain_id,
            height,
            json.dumps(block),
        )


async def upsert_raw_txs(pool: asyncpg.pool, txs: Dict[str, List[dict]], chain_id: str):
    async with pool.acquire() as conn:
        async with conn.transaction():
            for height, tx in txs.items():
                await conn.execute(
                    """
                    INSERT INTO raw_txs (chain_id, height, txs)
                    VALUES ($1, $2, $3)
                    ON CONFLICT DO NOTHING
                    """,
                    chain_id,
                    height,
                    json.dumps(tx) if len(tx) > 0 else None,
                )


async def get_missing_blocks(
    pool: asyncpg.pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    chain: CosmosChain,
):
    while True:
        block_data = await chain.get_block(session, sem)
        current_height = int(block_data["block"]["header"]["height"])
        chain_id = block_data["block"]["header"]["chain_id"]
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select height, difference_per_block from (
                    select height, COALESCE(height - LAG(height) over (order by time), -1) as difference_per_block, chain_id
                    from blocks
                    where chain_id = $1
                ) as dif
                where difference_per_block <> 1 and difference_per_block <> -1
                """,
                chain_id,
            )
            if not rows:
                for i in range(chain.min_block_height, current_height):
                    yield i
            for height, dif in rows:
                if dif == -1:
                    yield chain.min_block_height
                else:
                    for i in range(height - dif, height):
                        yield i


async def add_current_log_columns(pool: asyncpg.pool, new_cols: List[tuple[str, str]]):
    async with pool.acquire() as conn:
        async with conn.transaction():
            for i, row in enumerate(new_cols):
                await conn.execute(
                    f"""
                    INSERT INTO log_columns (event, attribute)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    """,
                    row[0],
                    row[1],
                )


async def add_logs(pool: asyncpg.pool, logs: List[Log]):
    async with pool.acquire() as conn:
        async with conn.transaction():
            for log in logs:
                await conn.execute(
                    """
                    INSERT INTO logs (txhash, msg_index, parsed, failed, failed_msg)
                    VALUES (
                        $1, $2, $3, $4, $5
                    )
                    """,
                    log.txhash,
                    str(log.msg_index),
                    log.dump(),
                    log.failed,
                    str(log.failed_msg) if log.failed_msg else None,
                )


async def get_missing_txs(pool, chain: CosmosChain) -> List[Tuple[int, int]]:
    """
    Returns the MISSING blocks that we are missing txs for
    Returns a list of tuples of (start_height, end_height) INCLUSIVE
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
                select height, difference_per_block from (
                    select height, COALESCE(height - LAG(height) over (order by height), -1) as difference_per_block, chain_id
                    from raw_txs
                    where chain_id = $1
                ) as dif
                where difference_per_block <> 1
        """,
            chain.chain_id,
        )

        # the above query returns the difference between the two blocks that exists

        # so if we have height=7285179 and height=7285186 then the difference is 7
        # but we really are missing the blocks between 7285179 and 7285186

        # so we add 1 to the bottom and subtract 1 from the top to get the range of blocks we need to query

        # this query returns the difference between the current block and the previous block, if there is no previous block it returns -1
        # if dif == -1 and  if min_block_height == height and remove that row, otherwise return that number and min_block_height
        updated_rows = []
        for height, dif in rows:
            if dif != -1:
                updated_rows.append(((height - dif) + 1, height - 1))
            else:
                if height == chain.min_block_height:
                    continue
                else:
                    updated_rows.append((chain.min_block_height, height - 1))

        return updated_rows
