import json
from types import SimpleNamespace
import aiohttp, asyncio
import pytest
from indexer.chain import CosmosChain
from unittest.mock import AsyncMock, MagicMock, Mock, patch


@pytest.fixture
def mock_client():
    mock = aiohttp.ClientSession
    mock.get = MagicMock()
    return mock


@pytest.fixture
def mock_chain():
    apis = {
        "https://mock_api.com/": {"hit": 0, "miss": 0},
        "https://mock_api2.com/": {"hit": 0, "miss": 0},
    }
    chain = CosmosChain(
        chain_id="mock_chain_id",
        blocks_endpoint="cosmos/blocks/{}",
        txs_endpoint="cosmos/txs/{}",
        apis=apis,
    )
    return chain


async def test_api_get(
    mock_client: aiohttp.ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"block": {"mock_key": "mock_response"}}

    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_block_txs(session, "1")
        assert exp_res == result


async def test_api_get_wrong_status(
    mock_client: aiohttp.ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"block": {"mock_key": "mock_response"}}

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_block_txs(session, "1")
        assert None == result


async def test_api_get_invalid_keys(
    mock_client: aiohttp.ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"code": 500, "message": "mock_response", "details": "mock_details"}

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    assert None == await mock_chain.get_block(mock_client)


async def test_api_get_invalid_json(
    mock_client: aiohttp.ClientSession, mock_chain: CosmosChain
):
    exp_res = "invalid_json"

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_block(session)
        assert None == result


async def test_block_get_wrong_chain(
    mock_client: aiohttp.ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"block": {"header": {"height": 1, "chain_id": "wrong_chain_id"}}}
    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_block(session)
        assert None == result

    assert None == mock_chain.remove_api("doesn't matter")


async def test_valid_block_valid_chain(
    mock_client: aiohttp.ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"block": {"header": {"height": 1, "chain_id": "mock_chain_id"}}}
    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_block(session)
        assert exp_res == result
