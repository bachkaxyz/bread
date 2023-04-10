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
    async for record in conn.cursor(
        f"""
        select height, difference_per_block from (
            select height, COALESCE(height - LAG(height) over (order by height), -1) as difference_per_block, chain_id
            from raw
            where chain_id = {chain.chain_id}
        ) as dif
        where difference_per_block <> 1
        order by height desc
        """
    ):
        yield record


async def drop_tables(conn: Connection, schema: str):
    await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    await conn.execute(f"CREATE SCHEMA {schema}")


async def create_tables(conn: Connection, schema: str):
    cur_dir = os.path.dirname(__file__)
    file_path = os.path.join(cur_dir, "sql/create_tables.sql")
    with open(file_path, "r") as f:
        await conn.execute(f.read().replace("$schema", schema))

    file_path = os.path.join(cur_dir, "sql/log_triggers.sql")
    with open(file_path, "r") as f:
        await conn.execute(f.read().replace("$schema", schema))


async def upsert_data(conn: Connection, raw: Raw):
    if not (raw.height is None or raw.chain_id is None or raw.block is None):
        await insert_raw(conn, raw)

        await insert_block(conn, raw)

        await insert_many_txs(conn, raw)

        await conn.executemany(
            f"""
                INSERT INTO logs (txhash, msg_index, parsed, failed, failed_msg)
                VALUES (
                    $1, $2, $3, $4, $5
                )
                """,
            raw.get_logs_db_params(),
        )

        print(f"{raw.height=} inserted")

    else:
        print(f"{raw.height} {raw.chain_id} {raw.block}")


async def insert_raw(conn: Connection, raw: Raw):
    await conn.execute(
        f"""
                INSERT INTO raw(chain_id, height, block, block_tx_count, tx_responses, tx_tx_count)
                VALUES ($1, $2, $3, $4, $5, $6);
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
