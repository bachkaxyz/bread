import json
import os
from typing import List
import aiohttp

import asyncpg
from indexer.chain import CosmosChain


async def drop_tables(pool: asyncpg.pool):
    await pool.execute(
        """
        DROP TABLE IF EXISTS raw CASCADE;
        DROP TABLE IF EXISTS blocks CASCADE;
        DROP TABLE IF EXISTS txs CASCADE;
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


async def upsert_block(pool: asyncpg.pool, block: dict, txs: dict):
    async with pool.acquire() as conn:
        chain_id, height = block["block"]["header"]["chain_id"], int(
            block["block"]["header"]["height"]
        )
        await conn.execute(
            """
            INSERT INTO raw (chain_id, height, block, txs)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
            """,
            chain_id,
            height,
            json.dumps(block),
            json.dumps(txs),
        )
        # for some reason this doesn't support params


async def get_missing_blocks(
    pool: asyncpg.pool, session: aiohttp.ClientSession, chain: CosmosChain
):

    while True:
        block_data = await chain.get_block(session)
        current_block = int(block_data["block"]["header"]["height"])
        chain_id = block_data["block"]["header"]["chain_id"]
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select height, difference_per_block from (
                    select height, COALESCE(height - LAG(height) over (order by time), -1) as difference_per_block, chain_id
                    from blocks
                    where chain_id = $1
                ) as dif
                where difference_per_block <> 1
                """,
                chain_id,
            )
            for height, dif in rows:
                if dif == -1:
                    yield chain.min_block_height
                else:
                    for i in range(height - dif, height):
                        yield i


async def add_columns(pool: asyncpg.pool, table_name: str, new_cols: List[str]):
    async with pool.acquire() as conn:
        alter_table = f"""
            ALTER TABLE {table_name}
            {
                ", ".join([f"ADD COLUMN IF NOT EXISTS {col} TEXT" for col in new_cols])
            }
        """
        await conn.execute(alter_table)
