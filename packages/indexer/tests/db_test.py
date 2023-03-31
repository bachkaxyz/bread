import asyncio
from datetime import datetime
import json
import os
from typing import Dict
import aiohttp
import asyncpg_listen
from indexer.chain import CosmosChain
from indexer.db import (
    Database,
    create_tables,
    drop_tables,
    get_missing_blocks,
    get_table_cols,
    upsert_raw_blocks,
    upsert_raw_txs,
)
from indexer.process import (
    DbNotificationHandler,
    process_block,
    process_tx,
)
import pytest
import asyncpg
import pandas as pd
from .chain_test import mock_chain, mock_client, mock_semaphore


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
async def mock_listener():
    return asyncpg_listen.NotificationListener(
        asyncpg_listen.connect_func(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB"),
        )
    )


@pytest.fixture
async def mock_db(mock_pool: asyncpg.Pool):
    db = Database(pool=mock_pool, schema=os.getenv("INDEXER_SCHEMA", "public"))
    await db.pool.execute(f"CREATE SCHEMA IF NOT EXISTS {db.schema}")
    await drop_tables(db)
    return db


@pytest.fixture
async def mock_notification_handler(mock_db: Database):
    dbNotifHandler = DbNotificationHandler(mock_db)
    return dbNotifHandler


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
    mock_chain: CosmosChain,
    mock_client: aiohttp.ClientSession,
    mock_semaphore: asyncio.Semaphore,
    raw_blocks: pd.DataFrame,
    mocker,
):
    await create_tables(mock_db)
    for block in raw_blocks["block"][: len(raw_blocks) // 2]:
        await upsert_raw_blocks(mock_db, block)

    # valid process block
    for height, _chain_id, block, _parsed_at in raw_blocks[
        len(raw_blocks) // 2 :
    ].itertuples(index=False):
        mock_client.get.return_value.__aenter__.return_value.status = 200
        mock_client.get.return_value.__aenter__.return_value.read.return_value = (
            json.dumps(block)
        )

        assert True == await process_block(
            height, mock_chain, mock_db, mock_client, mock_semaphore
        )

    # invalid block data
    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = None

    assert False == await process_block(
        height, mock_chain, mock_db, mock_client, mock_semaphore
    )

    async with mock_db.pool.acquire() as conn:
        raw_results = await conn.fetch(
            f"""
            select * from {mock_db.schema}.raw_blocks
            """
        )

        results = await conn.fetch(f"select * from {mock_db.schema}.blocks")

    assert len(raw_results) == len(raw_blocks)
    assert len(results) == len(raw_blocks)

    for block, res in zip(raw_blocks["block"], raw_results):
        assert int(res["height"]) == int(block["block"]["header"]["height"])
        assert res["chain_id"] == block["block"]["header"]["chain_id"]

    for block, res in zip(raw_blocks["block"], results):
        assert int(res["height"]) == int(block["block"]["header"]["height"])
        assert res["chain_id"] == block["block"]["header"]["chain_id"]
        # assert res["time"] == datetime.strptime(
        #     block["block"]["header"]["time"], "%Y-%m-%dT%H:%M:%S.%fZ"
        # )
        assert res["block_hash"] == block["block_id"]["hash"]
        assert res["proposer_address"] == block["block"]["header"]["proposer_address"]

    await drop_tables(mock_db)


@pytest.mark.asyncio
async def test_upsert_raw_txs(
    mock_db: Database,
    mock_listener: asyncpg_listen.NotificationListener,
    mock_chain: CosmosChain,
    mock_client: aiohttp.ClientSession,
    mock_semaphore: asyncio.Semaphore,
    raw_txs: pd.DataFrame,
    mock_notification_handler: DbNotificationHandler,
    mocker,
):
    await create_tables(mock_db)

    listener_task = asyncio.create_task(
        mock_listener.run(
            {"txs_to_logs": mock_notification_handler.process_tx_notifications},
            policy=asyncpg_listen.ListenPolicy.ALL,
        )
    )

    flattened_raw_txs = []
    for _chain_id, height, txs, _parsed_at in raw_txs[:500].itertuples(index=False):
        flattened_raw_txs.extend(txs)
        await upsert_raw_txs(mock_db, {int(height): txs}, mock_chain.chain_id)

    # valid block data
    for _chain_id, height, txs, _parsed_at in raw_txs[500:].itertuples(index=False):
        flattened_raw_txs.extend(txs)
        mocker.patch(
            "indexer.process.CosmosChain.get_block_txs",
            return_value={"tx_responses": txs},
        )

        assert True == await process_tx(
            height, mock_chain, mock_db, mock_client, mock_semaphore
        )

    # invalid tx data
    mocker.patch("indexer.process.CosmosChain.get_block_txs", return_value=None)
    assert False == await process_tx(
        height, mock_chain, mock_db, mock_client, mock_semaphore
    )

    async with mock_db.pool.acquire() as conn:
        raw_results = await conn.fetch(
            f"""
            select * from {mock_db.schema}.raw_txs
            """
        )

        results = await conn.fetch(f"select * from {mock_db.schema}.txs")

    assert len(results) == len(flattened_raw_txs)

    while len(mock_notification_handler.notifications) < len(flattened_raw_txs):
        await asyncio.sleep(0.1)

    assert len(mock_notification_handler.notifications) == len(flattened_raw_txs)
    await DbNotificationHandler.cancel_and_wait(listener_task)

    await drop_tables(mock_db)


async def test_get_missing(mocker):
    # beginning
    # dif is of length 1
    # all caught up
    # not all caught up
    pass
