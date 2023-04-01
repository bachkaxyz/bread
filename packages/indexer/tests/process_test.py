import json

import aiohttp
import asyncpg
from indexer.db import create_tables, drop_tables
from indexer.exceptions import ChainIdMismatchError
from indexer.process import process_block, process_tx
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


@pytest.fixture
def working_tx_data():
    return {
        "tx_responses": [
            {
                "txhash": "test_hash",
                "tx": {
                    "test_tx_key": "test_tx_value",
                    "@type": "/cosmos.bank.v1beta1.MsgSend",
                },
                "logs": [
                    {
                        "log": "",
                        "events": [
                            {
                                "type": "message",
                                "attributes": [{"key": "action", "value": "test"}],
                            }
                        ],
                    }
                ],
                "gas_wanted": 1,
                "gas_used": 1,
                "raw_log": [
                    {
                        "events": [
                            {
                                "type": "message",
                                "attributes": [{"key": "action", "value": "test"}],
                            }
                        ]
                    }
                ],
                "timestamp": datetime.now().isoformat(),
                "codespace": "test_codespace",
                "code": 0,
                "events": ["event1", "event2"],
            },
            {
                "txhash": "test_hash_2",
                "tx": {
                    "test_tx_key": "test_tx_value",
                    "@type": "/cosmos.bank.v1beta1.MsgSend",
                },
                "logs": [
                    {
                        "log": "",
                        "events": [
                            {
                                "type": "message",
                                "attributes": [{"key": "action", "value": "test"}],
                            }
                        ],
                    }
                ],
                "gas_wanted": 1,
                "gas_used": 1,
                "raw_log": [
                    {
                        "events": [
                            {
                                "type": "message",
                                "attributes": [{"key": "action", "value": "test"}],
                            }
                        ]
                    }
                ],
                "timestamp": datetime.now().isoformat(),
                "codespace": "test_codespace",
                "code": 0,
                "events": ["event1", "event2"],
            },
        ]
    }


@pytest.fixture
def no_tx_response_tx_data():
    return {}


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
    wrong_chain_id_block_data, mock_chain, mock_db, mock_client, mock_semaphore, mocker
):

    await create_tables(mock_db)

    mocker.patch(
        "indexer.chain.CosmosChain.get_block", return_value=wrong_chain_id_block_data
    )

    with pytest.raises(ChainIdMismatchError):
        await process_block(1, mock_chain, mock_db, mock_client, mock_semaphore)

    with pytest.raises(ChainIdMismatchError):
        await process_block(
            2,
            mock_chain,
            mock_db,
            mock_client,
            mock_semaphore,
            wrong_chain_id_block_data,
        )

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


async def test_process_tx_valid(
    working_tx_data, mock_chain, mock_db, mock_client, mock_semaphore, mocker
):
    await create_tables(mock_db)

    mocker.patch(
        "indexer.chain.CosmosChain.get_block_txs", return_value=working_tx_data
    )

    assert True == await process_tx(1, mock_chain, mock_db, mock_client, mock_semaphore)

    async with mock_db.pool.acquire() as conn:
        raw_txs_res = await conn.fetch(
            f"""
            select * from {mock_db.schema}.raw_txs
            """
        )

        txs_res = await conn.fetch(f"select * from {mock_db.schema}.txs")

    assert len(raw_txs_res) == 1
    assert len(txs_res) == 2

    for tx_res, test_tx in zip(txs_res, working_tx_data["tx_responses"]):
        assert tx_res["txhash"] == test_tx["txhash"]
        assert tx_res["chain_id"] == mock_chain.chain_id
        assert tx_res["gas_wanted"] == test_tx["gas_wanted"]
        assert tx_res["gas_used"] == test_tx["gas_used"]
        assert tx_res["codespace"] == test_tx["codespace"]
        assert tx_res["tx_response_tx_type"] == test_tx["tx"]["@type"]
        assert json.loads(tx_res["logs"]) == test_tx["logs"]
        assert json.loads(tx_res["raw_log"]) == test_tx["raw_log"]
        assert json.loads(tx_res["tx"]) == test_tx["tx"]

    assert raw_txs_res[0]["height"] == 1
    assert raw_txs_res[0]["chain_id"] == mock_chain.chain_id
    assert json.loads(raw_txs_res[0]["txs"]) == working_tx_data["tx_responses"]

    ## should trigger a unique key error
    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await process_tx(1, mock_chain, mock_db, mock_client, mock_semaphore)

    await drop_tables(mock_db)


async def test_process_tx_chain_data_is_none(
    mock_chain, mock_db, mock_client, mock_semaphore, mocker
):

    await create_tables(mock_db)

    mocker.patch("indexer.chain.CosmosChain.get_block_txs", return_value=None)

    # error is caught and logged, so no exception is raised but function returns False
    assert False == await process_tx(
        1, mock_chain, mock_db, mock_client, mock_semaphore
    )

    await drop_tables(mock_db)


async def test_process_tx_chain_data_key_error(
    mock_chain, mock_db, mock_client, mock_semaphore, mocker
):

    await create_tables(mock_db)

    mocker.patch("indexer.chain.CosmosChain.get_block_txs", return_value={})

    # error is caught and logged, so no exception is raised but function returns False
    assert False == await process_tx(
        1, mock_chain, mock_db, mock_client, mock_semaphore
    )

    await drop_tables(mock_db)
