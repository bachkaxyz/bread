import json

import aiohttp
import asyncpg
from indexer.db import create_tables, drop_tables
from indexer.exceptions import ChainIdMismatchError
from indexer.process import process_block
import pytest
from .chain_test import mock_chain, mock_client, mock_semaphore
from .db_test import mock_db, mock_pool
from datetime import datetime


@pytest.fixture
def working_block_data(mock_chain):
    return {
        "block": {
            "header": {
                "chain_id": mock_chain.chain_id,
                "height": 1,
                "time": datetime.now().isoformat(),
                "proposer_address": "test_address",
            },
        },
        "block_id": {"hash": "test_hash"},
    }


@pytest.fixture
def wrong_chain_id_block_data(working_block_data):
    working_block_data["block"]["header"]["chain_id"] = "wrong_chain_id"
    return working_block_data


async def test_process_block_valid_query(
    working_block_data, mock_chain, mock_db, mock_client, mock_semaphore, mocker
):

    await create_tables(mock_db)

    mocker.patch("indexer.chain.CosmosChain.get_block", return_value=working_block_data)

    assert True == await process_block(
        1, mock_chain, mock_db, mock_client, mock_semaphore
    )

    async with mock_db.pool.acquire() as conn:
        raw_block_res = await conn.fetch(
            f"""
            select * from {mock_db.schema}.raw_blocks
            """
        )

        block_res = await conn.fetch(f"select * from {mock_db.schema}.blocks")

    assert len(raw_block_res) == 1
    assert len(block_res) == 1

    assert block_res[0]["height"] == 1
    assert block_res[0]["chain_id"] == mock_chain.chain_id
    assert block_res[0]["block_hash"] == "test_hash"
    assert block_res[0]["proposer_address"] == "test_address"

    assert raw_block_res[0]["height"] == 1
    assert raw_block_res[0]["chain_id"] == mock_chain.chain_id
    assert json.loads(raw_block_res[0]["block"]) == working_block_data

    ## should trigger a unique key error
    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await process_block(1, mock_chain, mock_db, mock_client, mock_semaphore)

    await drop_tables(mock_db)


async def test_process_block_valid_passed_in(
    working_block_data, mock_chain, mock_db, mock_client, mock_semaphore, mocker
):

    await create_tables(mock_db)

    assert True == await process_block(
        1, mock_chain, mock_db, mock_client, mock_semaphore, working_block_data
    )

    async with mock_db.pool.acquire() as conn:
        raw_block_res = await conn.fetch(
            f"""
            select * from {mock_db.schema}.raw_blocks
            """
        )

        block_res = await conn.fetch(f"select * from {mock_db.schema}.blocks")

    assert len(raw_block_res) == 1
    assert len(block_res) == 1

    assert block_res[0]["height"] == 1
    assert block_res[0]["chain_id"] == mock_chain.chain_id
    assert block_res[0]["block_hash"] == "test_hash"
    assert block_res[0]["proposer_address"] == "test_address"

    assert raw_block_res[0]["height"] == 1
    assert raw_block_res[0]["chain_id"] == mock_chain.chain_id
    assert json.loads(raw_block_res[0]["block"]) == working_block_data

    ## should trigger a unique key error
    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await process_block(
            1, mock_chain, mock_db, mock_client, mock_semaphore, working_block_data
        )

    await drop_tables(mock_db)


async def test_process_block_wrong_chain_id(
    wrong_chain_id_data, mock_chain, mock_db, mock_client, mock_semaphore, mocker
):

    await create_tables(mock_db)

    mocker.patch(
        "indexer.chain.CosmosChain.get_block", return_value=wrong_chain_id_data
    )

    with pytest.raises(ChainIdMismatchError):
        await process_block(1, mock_chain, mock_db, mock_client, mock_semaphore)

    await drop_tables(mock_db)


async def test_process_block_chain_data_is_none(
    mock_chain, mock_db, mock_client, mock_semaphore, mocker
):

    await create_tables(mock_db)

    mocker.patch("indexer.chain.CosmosChain.get_block", return_value=None)

    # error is caught and logged, so no exception is raised but function returns False
    assert False == await process_block(
        1, mock_chain, mock_db, mock_client, mock_semaphore
    )

    await drop_tables(mock_db)
