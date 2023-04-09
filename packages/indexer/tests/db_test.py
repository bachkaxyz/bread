import asyncio
from datetime import datetime
import json
import os
from typing import Dict
from aiohttp import ClientSession
from indexer.chain import CosmosChain
from indexer.db import (
    create_tables,
    drop_tables,
)


import pytest
from asyncpg import Connection, Pool, create_pool
import pandas as pd
from .chain_test import mock_chain, mock_client


@pytest.fixture
def mock_schema():
    return "public"


@pytest.fixture
async def mock_pool(mock_schema):
    pool = await create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        server_settings={"search_path": mock_schema},
    )
    return pool


@pytest.fixture
def raw_blocks():
    df = pd.read_csv("tests/test_data/raw_blocks.csv")
    df["block"] = df["block"].apply(json.loads)
    return df


@pytest.fixture
def raw_txs():
    df = pd.read_csv("tests/test_data/raw_txs.csv")
    df["txs"] = df["txs"].apply(lambda x: json.loads(x) if not pd.isna(x) else [])
    return df


@pytest.mark.asyncio
async def test_create_drop_tables(mock_pool: Pool, mock_schema: str):
    async def check_tables(table_names) -> int:
        async with mock_pool.acquire() as conn:
            results = await conn.fetch(
                f"""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{mock_schema}'
                AND table_name in {table_names}
                """
            )
        return len(results)

    async with mock_pool.acquire() as conn:
        await create_tables(conn, mock_schema)

    table_names = ("raw_blocks", "raw_txs", "blocks", "txs", "logs", "log_columns")

    assert await check_tables(table_names) == len(table_names)

    async with mock_pool.acquire() as conn:
        await drop_tables(conn, mock_schema)

    assert await check_tables(table_names) == 0


@pytest.mark.asyncio
async def test_upsert_raw_blocks(
    mock_pool: Pool,
    mock_schema: str,
    mock_chain: CosmosChain,
    mock_client: ClientSession,
    mock_semaphore: asyncio.Semaphore,
    raw_blocks: pd.DataFrame,
    mocker,
):
    async with mock_pool.acquire() as conn:
        await create_tables(conn, mock_schema)
    # for block in raw_blocks["block"][: len(raw_blocks) // 2]:
    #     await upsert_raw_blocks(mock_pool, block)

    # valid process block
    for height, _chain_id, block, _parsed_at in raw_blocks[
        len(raw_blocks) // 2 :
    ].itertuples(index=False):
        assert True == False  # should test parsing blocks

        # assert True == await process_block(
        #     height, mock_chain, mock_pool, mock_client, mock_semaphore
        # )

    #     results = await conn.fetch(f"select * from {mock_pool.schema}.blocks")

    # assert len(raw_results) == len(raw_blocks)
    # assert len(results) == len(raw_blocks)

    # for block, res in zip(raw_blocks["block"], raw_results):
    #     assert int(res["height"]) == int(block["block"]["header"]["height"])
    #     assert res["chain_id"] == block["block"]["header"]["chain_id"]

    # for block, res in zip(raw_blocks["block"], results):
    #     assert int(res["height"]) == int(block["block"]["header"]["height"])
    #     assert res["chain_id"] == block["block"]["header"]["chain_id"]
    #     # assert res["time"] == datetime.strptime(
    #     #     block["block"]["header"]["time"], "%Y-%m-%dT%H:%M:%S.%fZ"
    #     # )
    #     assert res["block_hash"] == block["block_id"]["hash"]
    #     assert res["proposer_address"] == block["block"]["header"]["proposer_address"]

    # await drop_tables(mock_pool)


@pytest.mark.asyncio
async def test_upsert_raw_txs(
    mock_pool: Pool,
    mock_schema: str,
    mock_chain: CosmosChain,
    mock_client: ClientSession,
    mock_semaphore: asyncio.Semaphore,
    raw_txs: pd.DataFrame,
    mocker,
):
    async with mock_pool.acquire() as conn:
        await create_tables(conn, mock_schema)

    flattened_raw_txs = []
    for _chain_id, height, txs, _parsed_at in raw_txs[:500].itertuples(index=False):
        flattened_raw_txs.extend(txs)
        # await upsert_raw_txs(mock_pool, {height: txs}, mock_chain.chain_id)

    # valid block data
    for _chain_id, height, txs, _parsed_at in raw_txs[500:].itertuples(index=False):
        flattened_raw_txs.extend(txs)
        mocker.patch(
            "indexer.process.CosmosChain.get_block_txs",
            return_value={"tx_responses": txs},
        )

        assert True == False

    # invalid tx data
    mocker.patch("indexer.process.CosmosChain.get_block_txs", return_value=None)
    assert False == True

    async with mock_pool.acquire() as conn:
        raw_results = await conn.fetch(
            f"""
            select * from {mock_schema}.raw_txs
            """
        )

        results = await conn.fetch(f"select * from {mock_schema}.txs")

    assert len(results) == len(flattened_raw_txs)

    async with mock_pool.acquire() as conn:
        await drop_tables(conn, mock_schema)


async def test_get_missing(mocker):
    # beginning
    # dif is of length 1
    # all caught up
    # not all caught up
    pass
