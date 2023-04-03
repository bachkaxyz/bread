import asyncio
from dataclasses import dataclass
import json
import os
from typing import Dict, List, Set, Tuple

from asyncpg import Connection, Pool
from indexer.chain import CosmosChain
from indexer.parser import Log


@dataclass
class Database:
    pool: Pool
    schema: str = "public"


async def drop_tables(db: Database):
    async with db.pool.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {db.schema} CASCADE")
        await conn.execute(f"CREATE SCHEMA {db.schema}")


async def get_table_cols(db: Database, table_name: str):
    async with db.pool.acquire() as conn:
        cols = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2;
            """,
            db.schema,
            table_name,
        )
        print(cols)
        return [col["column_name"] for col in cols]


async def create_tables(db: Database):
    cur_dir = os.path.dirname(__file__)
    async with db.pool.acquire() as conn:
        file_path = os.path.join(cur_dir, "sql/create_tables.sql")
        with open(file_path, "r") as f:
            await conn.execute(f.read().replace("$schema", db.schema))

        # create the parse_raw function on insert trigger of raw
        file_path = os.path.join(cur_dir, "sql/parse_raw.sql")
        with open(file_path, "r") as f:
            await conn.execute(f.read().replace("$schema", db.schema))

        file_path = os.path.join(cur_dir, "sql/log_triggers.sql")
        with open(file_path, "r") as f:
            await conn.execute(f.read().replace("$schema", db.schema))


async def upsert_raw_blocks(db: Database, block: dict):
    async with db.pool.acquire() as conn:
        chain_id, height = block["block"]["header"]["chain_id"], int(
            block["block"]["header"]["height"]
        )
        await conn.execute(
            f"""
            INSERT INTO {db.schema}.raw_blocks (chain_id, height, block)
            VALUES ($1, $2, $3)
            """,
            chain_id,
            int(height),
            json.dumps(block),
        )


async def upsert_raw_txs(db: Database, txs: Dict[str, List[dict]], chain_id: str):
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            for height, tx in txs.items():
                await conn.execute(
                    f"""
                    INSERT INTO {db.schema}.raw_txs (chain_id, height, txs)
                    VALUES ($1, $2, $3)
                    """,
                    chain_id,
                    int(height),
                    json.dumps(tx) if len(tx) > 0 else None,
                )


async def add_current_log_columns(db: Database, new_cols: Set[tuple[str, str]]):
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            for i, row in enumerate(new_cols):
                await conn.execute(
                    f"""
                    INSERT INTO {db.schema}.log_columns (event, attribute)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    """,
                    row[0],
                    row[1],
                )


async def add_logs(db: Database, logs: List[Log]):
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            for log in logs:
                await conn.execute(
                    f"""
                    INSERT INTO {db.schema}.logs (txhash, msg_index, parsed, failed, failed_msg)
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


async def get_missing_from_raw(db: Database, chain: CosmosChain, table_name: str):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
                select height, difference_per_block from (
                    select height, COALESCE(height - LAG(height) over (order by height), -1) as difference_per_block, chain_id
                    from {db.schema}.{table_name}
                    where chain_id = $1
                ) as dif
                where difference_per_block <> 1
            """,
            chain.chain_id,
        )
        print(rows)

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


async def get_missing_txs(db: Database, chain: CosmosChain) -> List[Tuple[int, int]]:
    return await get_missing_from_raw(db, chain, "raw_txs")


async def get_missing_blocks(db: Database, chain: CosmosChain) -> List[Tuple[int, int]]:
    return await get_missing_from_raw(db, chain, "raw_blocks")


async def get_tx_raw_log_and_tx(
    db: Database, chain_id: str, txhash: str
) -> Tuple[str, str]:
    async with db.pool.acquire() as conn:
        return await conn.fetchrow(
            f"SELECT raw_log, tx FROM {db.schema}.txs WHERE chain_id = $1 AND txhash = $2",
            chain_id,
            txhash,
        )
