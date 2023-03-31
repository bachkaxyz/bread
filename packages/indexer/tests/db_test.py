import asyncio
from datetime import datetime
import json
import os
from typing import Dict
import aiohttp
from indexer.db import (
    Database,
    create_tables,
    drop_tables,
    get_missing_blocks,
    get_table_cols,
    upsert_raw_blocks,
)
import pytest
import asyncpg


@pytest.fixture
async def mock_pool():
    pool = await asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
    )
    return pool


@pytest.fixture
async def mock_db(mock_pool):
    db = Database(pool=mock_pool, schema=os.getenv("INDEXER_SCHEMA", "public"))
    await db.pool.execute(f"CREATE SCHEMA IF NOT EXISTS {db.schema}")
    await drop_tables(db)
    return db


@pytest.fixture
def raw_blocks():
    with open("tests/test_data/raw_blocks.json", "r") as f:
        return json.load(f)


@pytest.mark.asyncio
async def test_create_drop_tables(mock_db: Database):
    async def check_tables(mock_db: Database, table_names) -> int:
        async with mock_db.pool.acquire() as conn:
            results = await conn.fetch(
                f"""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = '{mock_db.schema}'
                AND table_name in {table_names}
                """
            )
        return len(results)

    await create_tables(mock_db)

    table_names = ("raw_blocks", "raw_txs", "blocks", "txs", "logs", "log_columns")

    assert await check_tables(mock_db, table_names) == len(table_names)

    assert await get_table_cols(mock_db, "blocks") == [
        "height",
        "chain_id",
        "time",
        "block_hash",
        "proposer_address",
    ]

    await drop_tables(mock_db)

    assert await check_tables(mock_db, table_names) == 0


@pytest.mark.asyncio
async def test_upsert_raw_blocks(
    mock_db: Database,
    raw_blocks,
):
    await create_tables(mock_db)
    for block in raw_blocks:
        await upsert_raw_blocks(mock_db, block)

    async with mock_db.pool.acquire() as conn:
        raw_results = await conn.fetch(
            f"""
            select * from {mock_db.schema}.raw_blocks
            """
        )

        results = await conn.fetch(f"select * from {mock_db.schema}.blocks")

    assert len(raw_results) == len(raw_blocks)
    assert len(results) == len(raw_blocks)

    for block, res in zip(raw_blocks, raw_results):
        assert int(res["height"]) == int(block["block"]["header"]["height"])
        assert res["chain_id"] == block["block"]["header"]["chain_id"]

    for block, res in zip(raw_blocks, results):
        print(list(res.keys()))
        assert int(res["height"]) == int(block["block"]["header"]["height"])
        assert res["chain_id"] == block["block"]["header"]["chain_id"]
        # assert res["time"] == datetime.strptime(
        #     block["block"]["header"]["time"], "%Y-%m-%dT%H:%M:%S.%fZ"
        # )
        assert res["block_hash"] == block["block_id"]["hash"]
        assert res["proposer_address"] == block["block"]["header"]["proposer_address"]

    await drop_tables(mock_db)
