import asyncio
import json
import os
import traceback
from typing import List
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


async def get_missing_blocks(
    pool: asyncpg.pool, session: aiohttp.ClientSession, sem: asyncio.Semaphore, chain: CosmosChain
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
                where difference_per_block <> 1
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


async def add_columns(pool: asyncpg.pool, table_name: str, new_cols: List[str]):
    async with pool.acquire() as conn:
        alter_table = f"""
            ALTER TABLE {table_name}
            {
                ", ".join([f"ADD COLUMN IF NOT EXISTS {col} TEXT" for col in new_cols])
            }
        """
        await conn.execute(alter_table)

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
                    row[0], row[1]
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
                    str(log.failed_msg), 
                )
    
        
        
async def insert_dict(pool: asyncpg.pool, table_name: str, data: List[dict]) -> bool:
    """
    Insert a list of dicts into a table with keys as columns and values as values
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for row in data:
                keys = ",".join(row.keys())
                values = "'" + "','".join(str(i) for i in row.values()) + "'"
                try:
                    await conn.execute(
                        f"""
                        INSERT INTO {table_name} ({keys})
                        VALUES ({values})
                        """
                    )
                except asyncpg.PostgresSyntaxError as e:
                    print(table_name, traceback.format_exc())
                    return False
    return True
