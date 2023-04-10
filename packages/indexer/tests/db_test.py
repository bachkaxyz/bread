import asyncio
from datetime import datetime
import json
import os
from typing import Dict, List
from aiohttp import ClientSession
from indexer.chain import CosmosChain
from indexer.db import (
    create_tables,
    drop_tables,
    upsert_data,
)
from indexer.parser import Block, Raw, Tx
from deepdiff import DeepDiff


import pytest
from asyncpg import Connection, Pool, create_pool

# fixtures
from tests.chain_test import mock_chain, mock_client
from tests.parser_test import raws, unparsed_raw_data


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

    table_names = ("raw", "blocks", "txs", "logs", "log_columns")

    assert await check_tables(table_names) == len(table_names)

    async with mock_pool.acquire() as conn:
        await drop_tables(conn, mock_schema)

    assert await check_tables(table_names) == 0


async def test_upsert_data(
    mock_pool: Pool,
    mock_schema: str,
    mock_chain: CosmosChain,
    mock_client: ClientSession,
    raws: List[Raw],
):
    async with mock_pool.acquire() as conn:
        conn: Connection
        await create_tables(conn, mock_schema)
        for data in raws:
            await upsert_data(conn, data)

        raw_results = await conn.fetch("select * from raw")

        block_results = await conn.fetch("select * from blocks")

        tx_results = await conn.fetch("select * from txs")

    for raw, res in zip(raws, raw_results):
        assert raw.chain_id == res["chain_id"]
        assert raw.height == res["height"]
        assert raw.block_tx_count == res["block_tx_count"]
        assert raw.tx_responses_tx_count == res["tx_tx_count"]

    for block, res_block in zip([raw.block for raw in raws], block_results):
        if block:
            res_block_parsed = Block(
                height=res_block["height"],
                chain_id=res_block["chain_id"],
                time=res_block["time"],
                proposer_address=res_block["proposer_address"],
                block_hash=res_block["block_hash"],
            )

            for b, r in zip(block.get_db_params(), res_block_parsed.get_db_params()):
                assert b == r

    txs: List[Tx] = []
    [txs.extend(raw.txs) for raw in raws]
    for tx, res_tx in zip(txs, tx_results):
        res_tx_parsed = Tx(
            txhash=res_tx["txhash"],
            height=res_tx["height"],
            chain_id=res_tx["chain_id"],
            code=res_tx["code"],
            data=res_tx["data"],
            info=res_tx["info"],
            logs=json.loads(res_tx["logs"]),
            events=json.loads(res_tx["events"]),
            raw_log=res_tx["raw_log"],
            gas_used=res_tx["gas_used"],
            gas_wanted=res_tx["gas_wanted"],
            codespace=res_tx["codespace"],
            timestamp=res_tx["timestamp"],
        )

        keys = "txhash, chain_id, height, code, data, info, logs, events, raw_log, gas_used, gas_wanted, codespace, timestamp".split(
            ", "
        )
        for k, b, r in zip(keys, tx.get_db_params(), res_tx_parsed.get_db_params()):
            try:
                actual = json.loads(str(b))
                expected = json.loads(str(r))

            except:
                assert b == r


async def test_get_missing(raws: List[Raw], mock_pool):
    # beginning
    # dif is of length 1
    # all caught up
    # not all caught up
    pass
