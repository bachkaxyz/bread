import asyncio
from datetime import datetime
import json
import os
from typing import Dict
import aiohttp
from indexer.db import (
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
    # make sure its a clean db
    await drop_tables(pool)
    return pool


@pytest.fixture
def raw_blocks():
    with open("tests/test_data/raw_blocks.json", "r") as f:
        return json.load(f)


@pytest.mark.asyncio
async def test_create_drop_tables(mock_pool: asyncpg.pool):
    async def check_tables(pool, table_names, schema) -> int:
        async with pool.acquire() as conn:
            results = await conn.fetch(
                f"""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = $1
                AND table_name in {table_names}
                """,
                schema,
            )
        return len(results)

    await create_tables(mock_pool)

    table_names = ("raw_blocks", "raw_txs", "blocks", "txs", "logs", "log_columns")

    assert await check_tables(mock_pool, table_names, "public") == len(table_names)

    assert await get_table_cols(mock_pool, "blocks") == [
        "height",
        "chain_id",
        "time",
        "block_hash",
        "proposer_address",
    ]

    await drop_tables(mock_pool)

    assert await check_tables(mock_pool, table_names, "public") == 0


@pytest.mark.asyncio
async def test_upsert_raw_blocks(
    mock_pool,
    raw_blocks,
):
    await create_tables(mock_pool)
    for block in raw_blocks:
        await upsert_raw_blocks(mock_pool, block)

    async with mock_pool.acquire() as conn:
        raw_results = await conn.fetch(
            """
            select * from raw_blocks
            """
        )

        results = await conn.fetch("select * from blocks")

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

    # assert await get_missing_blocks(mock_pool, mock_session, mock_sem, mock_chain) == []

    await drop_tables(mock_pool)
