import asyncio
from dataclasses import dataclass
import json
import os
from typing import Dict, List, Set, Tuple

from asyncpg import Connection, Pool
from indexer.chain import CosmosChain
from indexer.parser import Log


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
