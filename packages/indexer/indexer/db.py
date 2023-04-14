import asyncio
from dataclasses import dataclass
import json
import os
from typing import Dict, List, Set, Tuple

from asyncpg import Connection, Pool
from indexer.chain import CosmosChain
from indexer.exceptions import ChainDataIsNoneError
from indexer.parser import Raw


async def missing_blocks_cursor(conn: Connection, chain: CosmosChain):
    """
    Generator that yields missing blocks from the database

    limit of 100 is to prevent the generator from yielding too many results to keep live data more up to date
    """
    async for record in conn.cursor(
        """
        select height, difference_per_block from (
            select height, COALESCE(height - LAG(height) over (order by height), -1) as difference_per_block, chain_id
            from raw
            where chain_id = $1
        ) as dif
        where difference_per_block <> 1
        order by height desc
        limit 100
        """,
        chain.chain_id,
    ):
        yield record


async def wrong_tx_count_cursor(conn: Connection, chain: CosmosChain):
    """
    Generator that yields blocks with wrong tx counts from the database
    """
    async for record in conn.cursor(
        """
        select height, block_tx_count, chain_id
        from raw
        where tx_tx_count <> block_tx_count and chain_id = $1
        """,
        chain.chain_id,
    ):
        yield record


async def drop_tables(conn: Connection, schema: str):
    await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    await conn.execute(f"CREATE SCHEMA {schema}")


async def create_tables(conn: Connection, schema: str):
    # we use the path of your current directory to get the absolute path of the sql files depending on where the script is run from
    cur_dir = os.path.dirname(__file__)
    file_path = os.path.join(cur_dir, "sql/create_tables.sql")
    with open(file_path, "r") as f:
        # our schema in .sql files is defined as $schema so we replace it with the actual schema name
        await conn.execute(f.read().replace("$schema", schema))

    file_path = os.path.join(cur_dir, "sql/log_triggers.sql")
    with open(file_path, "r") as f:
        await conn.execute(f.read().replace("$schema", schema))


async def upsert_data(pool: Pool, raw: Raw) -> bool:
    """Upsert a blocks data into the database

    Args:
        pool (Pool): The database connection pool
        raw (Raw): The raw data to upsert

    Returns:
        bool: True if the data was upserted, False if the data was not upserted
    """

    # we check if the data is valid before upserting it
    if raw.height is not None and raw.chain_id is not None:
        async with pool.acquire() as conn:
            # we do all the upserts in a transaction so that if one fails, all of them fail
            async with conn.transaction():
                await insert_raw(conn, raw)

                # we are checking if the block is not None because we might only have the tx data and not the block data
                if raw.block is not None:
                    print("raw block height", raw.block.height)
                    await insert_block(conn, raw)

                await insert_many_txs(conn, raw)

                await insert_many_log_columns(conn, raw)

                await insert_many_logs(conn, raw)

        print(f"{raw.height=} inserted")
        return True

    else:
        print(f"{raw.height} {raw.chain_id} {raw.block}")
        return False


async def insert_raw(conn: Connection, raw: Raw):
    # the on conflict clause is used to update the tx_responses and tx_tx_count columns if the raw data already exists but the tx data is new
    await conn.execute(
        f"""
                INSERT INTO raw(chain_id, height, block, block_tx_count, tx_responses, tx_tx_count)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT ON CONSTRAINT raw_pkey
                DO UPDATE SET tx_responses = EXCLUDED.tx_responses, tx_tx_count = EXCLUDED.tx_tx_count;
                """,
        *raw.get_raw_db_params(),
    )


async def insert_block(conn: Connection, raw: Raw):
    if raw.block:
        await conn.execute(
            """
                    INSERT INTO blocks(chain_id, height, time, block_hash, proposer_address)
                    VALUES ($1, $2, $3, $4, $5);
                    """,
            *raw.block.get_db_params(),
        )
    else:
        raise ChainDataIsNoneError("Block does not exist")


async def insert_many_txs(conn: Connection, raw: Raw):
    await conn.executemany(
        """
                INSERT INTO txs(txhash, chain_id, height, code, data, info, logs, events, raw_log, gas_used, gas_wanted, codespace, timestamp)
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                )
                """,
        raw.get_txs_db_params(),
    )


async def insert_many_log_columns(conn: Connection, raw: Raw):
    await conn.executemany(
        f"""
                INSERT INTO log_columns (event, attribute)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
        raw.get_log_columns_db_params(),
    )


async def insert_many_logs(conn: Connection, raw: Raw):
    await conn.executemany(
        f"""
                INSERT INTO logs (txhash, msg_index, parsed, failed, failed_msg)
                VALUES (
                    $1, $2, $3, $4, $5
                )
                """,
        raw.get_logs_db_params(),
    )


async def get_max_height(conn: Connection, chain: CosmosChain) -> int:
    """Get the max height of the chain from the database"""
    res = await conn.fetchval(
        """
        select max(height)
        from raw
        where chain_id = $1
        """,
        chain.chain_id,
        column=0,
    )
    print(res)
    if res:
        return res
    else:
        # if max height doesn't exist
        return 0
