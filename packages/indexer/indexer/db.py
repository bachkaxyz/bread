import json
import os
import aiohttp

import asyncpg
from indexer.chain_mapper import CosmosChain
from indexer.data import get_block


async def drop_tables(pool: asyncpg.pool):
    await pool.execute(
        """
        DROP TABLE IF EXISTS raw CASCADE;
        DROP TABLE IF EXISTS blocks CASCADE;
        DROP TABLE IF EXISTS txs CASCADE;
        """
    )


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
        await conn.execute(
            f"""
            NOTIFY raw, '{chain_id} {height}';
            """
        )


async def get_missing_blocks(
    pool: asyncpg.pool, session: aiohttp.ClientSession, chain: CosmosChain
):

    while True:
        block_data = await get_block(session, chain.apis[0])
        current_block = int(block_data["block"]["header"]["height"])
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT height FROM blocks WHERE chain_id = $1
                """,
                chain.chain_id,
            )
            heights = [row[0] for row in rows]
            for height in range(chain.min_block_height, current_block):
                if height not in heights:
                    yield height
