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
def mock_semaphore():
    return AsyncMock(spec=asyncio.Semaphore)


@pytest.fixture
def mock_chain():
    apis = (["https://mock_api.com/", "https://mock_api2.com/"],)
    return CosmosChain(
        min_block_height=0,
        chain_id="mock_chain_id",
        blocks_endpoint="cosmos/blocks/{}",
        txs_endpoint="cosmos/txs/{}",
        txs_batch_endpoint="cosmos/txs?minHeight={}&maxHeight={}",
        apis=apis,
        apis_hit=[0 * len(apis)],
        apis_miss=[0 * len(apis)],
    )


@pytest.mark.asyncio
async def test_api_get(mock_semaphore, mock_client, mock_chain):

    exp_res = {"block": {"mock_key": "mock_response"}}

    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_block(session, mock_semaphore)
        assert exp_res == []


@pytest.mark.asyncio
async def test_api_get_wrong_status(mock_semaphore, mock_client, mock_chain):

    exp_res = {"block": {"mock_key": "mock_response"}}

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_block_txs(session, mock_semaphore, 1)
        assert None == result


@pytest.mark.asyncio
async def test_api_get_invalid_keys(mock_semaphore, mock_client, mock_chain):

    exp_res = {"code": 500, "message": "mock_response", "details": "mock_details"}

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_batch_txs(session, mock_semaphore, 0, 100)
        assert None == result


@pytest.mark.asyncio
async def test_api_get_invalid_json(mock_semaphore, mock_client, mock_chain):

    exp_res = "invalid_json"

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with aiohttp.ClientSession() as session:
        result = await mock_chain.get_block(session, mock_semaphore)
        assert None == result
