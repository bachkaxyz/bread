import json
import os
from aiohttp import ClientSession
import asyncio
import pytest
from indexer.chain import (
    CosmosChain,
    get_chain_from_environment,
    query_chain_registry,
    remove_bad_apis,
    Api,
)
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_client():
    mock = ClientSession
    mock.get = MagicMock()
    return mock


@pytest.fixture
def emptyApi():
    return Api({"hit": 0, "miss": 0, "times": []})


@pytest.fixture
def mock_chain(emptyApi: Api):
    apis = {
        "https://mock_api.com/": emptyApi,
        "https://mock_api2.com/": emptyApi,
    }
    chain = CosmosChain(
        chain_id="mock_chain_id",
        blocks_endpoint="cosmos/blocks/{}",
        txs_endpoint="cosmos/txs/{}",
        apis=apis,
    )
    return chain


async def test_api_get(
    mock_client: ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"block": {"mock_key": "mock_response"}}

    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with ClientSession() as session:
        result = await mock_chain.get_block_txs(session, "1")
        assert exp_res == result


async def test_api_get_wrong_status(
    mock_client: ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"block": {"mock_key": "mock_response"}}

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with ClientSession() as session:
        result = await mock_chain.get_block_txs(session, "1")
        assert None == result


async def test_api_get_invalid_keys(
    mock_client: ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"code": 500, "message": "mock_response", "details": "mock_details"}

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    assert None == await mock_chain.get_block(mock_client)


async def test_api_get_invalid_json(
    mock_client: ClientSession, mock_chain: CosmosChain
):
    exp_res = "invalid_json"

    mock_client.get.return_value.__aenter__.return_value.status = 500
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with ClientSession() as session:
        result = await mock_chain.get_block(session)
        assert None == result


async def test_block_get_wrong_chain(
    mock_client: ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"block": {"header": {"height": 1, "chain_id": "wrong_chain_id"}}}
    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with ClientSession() as session:
        result = await mock_chain.get_block(session)
        assert None == result

    assert None == mock_chain.remove_api("doesn't matter")


async def test_valid_block_valid_chain(
    mock_client: ClientSession,
    mock_chain: CosmosChain,
):
    exp_res = {"block": {"header": {"height": 1, "chain_id": "mock_chain_id"}}}
    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    async with ClientSession() as session:
        result = await mock_chain.get_block(session)
        assert exp_res == result


async def test_get_lowest_height_on_api(
    mock_client: ClientSession, mock_chain: CosmosChain
):
    exp_res = {
        "code": 3,
        "message": "height 1 is not available, lowest height is 2: invalid request",
        "details": [],
    }
    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    assert 2 == await mock_chain.get_lowest_height(mock_client)


async def test_chain_environment_fail_no_chain_registry(
    mock_client: ClientSession, mocker
):
    mocker.resetall()
    with pytest.raises(OSError):
        assert None == await get_chain_from_environment(mock_client)


async def test_chain_environment_time_between_blocks_not_int(mock_client):
    os.environ["TIME_BETWEEN_BLOCKS"] = "not a string"
    with pytest.raises(EnvironmentError):
        assert None == await get_chain_from_environment(mock_client)

    os.environ["TIME_BETWEEN_BLOCKS"] = "1"


async def test_chain_environment_pass(
    mock_client: ClientSession, mocker, emptyApi: Api
):
    chain_reg = {
        "chain_name": "jackal",
        "chain_id": "jackal-1",
        "apis": {
            "rest": [
                {
                    "address": "https://api.jackalprotocol.com",
                    "provider": "Jackal Labs",
                },
                {
                    "address": "https://jackal-api.lavenderfive.com:443",
                    "provider": "Lavender.Five Nodes 🐝",
                },
            ],
        },
    }

    mocker.patch("indexer.chain.query_chain_registry", return_value=chain_reg)

    exp_res = {
        "block": {
            "header": {
                "height": "1",
            }
        }
    }

    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        exp_res
    )

    # only external apis
    os.environ["CHAIN_REGISTRY_NAME"] = "jackal"
    async with ClientSession() as session:
        assert await get_chain_from_environment(session) == CosmosChain(
            chain_id="jackal-1",
            blocks_endpoint="/cosmos/base/tendermint/v1beta1/blocks/{}",
            txs_endpoint="/cosmos/tx/v1beta1/txs?events=tx.height={}",
            apis={
                "https://api.jackalprotocol.com": emptyApi,
                "https://jackal-api.lavenderfive.com:443": emptyApi,
            },
            current_api_index=0,
            time_between_blocks=1,
        )

    # no apis should error
    os.environ["LOAD_CHAIN_REGISTRY_APIS"] = "FALSE"
    os.environ["APIS"] = ""

    async with ClientSession() as session:
        with pytest.raises(EnvironmentError):
            await get_chain_from_environment(session)

    # only internal apis
    os.environ["LOAD_CHAIN_REGISTRY_APIS"] = "FALSE"
    os.environ[
        "APIS"
    ] = "https://api.jackalprotocol.com,https://jackal-api.lavenderfive.com:443"
    async with ClientSession() as session:
        assert await get_chain_from_environment(session) == CosmosChain(
            chain_id="jackal-1",
            blocks_endpoint="/cosmos/base/tendermint/v1beta1/blocks/{}",
            txs_endpoint="/cosmos/tx/v1beta1/txs?events=tx.height={}",
            apis={
                "https://api.jackalprotocol.com": emptyApi,
                "https://jackal-api.lavenderfive.com:443": emptyApi,
            },
            current_api_index=0,
            time_between_blocks=1,
        )

    # both external and internal apis
    os.environ["LOAD_CHAIN_REGISTRY_APIS"] = "TRUE"
    os.environ[
        "APIS"
    ] = "https://api2.jackalprotocol.com,https://jackal-api2.lavenderfive.com:443"
    async with ClientSession() as session:
        assert await get_chain_from_environment(session) == CosmosChain(
            chain_id="jackal-1",
            blocks_endpoint="/cosmos/base/tendermint/v1beta1/blocks/{}",
            txs_endpoint="/cosmos/tx/v1beta1/txs?events=tx.height={}",
            apis={
                "https://api.jackalprotocol.com": emptyApi,
                "https://jackal-api.lavenderfive.com:443": emptyApi,
                "https://api2.jackalprotocol.com": emptyApi,
                "https://jackal-api2.lavenderfive.com:443": emptyApi,
            },
            current_api_index=0,
            time_between_blocks=1,
        )

    # no duplicate apis:
    os.environ["LOAD_CHAIN_REGISTRY_APIS"] = "TRUE"
    os.environ[
        "APIS"
    ] = "https://api.jackalprotocol.com,https://jackal-api.lavenderfive.com:443"
    async with ClientSession() as session:
        assert await get_chain_from_environment(session) == CosmosChain(
            chain_id="jackal-1",
            blocks_endpoint="/cosmos/base/tendermint/v1beta1/blocks/{}",
            txs_endpoint="/cosmos/tx/v1beta1/txs?events=tx.height={}",
            apis={
                "https://api.jackalprotocol.com": emptyApi,
                "https://jackal-api.lavenderfive.com:443": emptyApi,
            },
            current_api_index=0,
            time_between_blocks=1,
        )


async def test_query_chain_registry(mock_client):
    chain_reg = {
        "chain_name": "jackal",
        "chain_id": "jackal-1",
        "apis": {
            "rest": [
                {
                    "address": "https://api.jackalprotocol.com",
                    "provider": "Jackal Labs",
                },
                {
                    "address": "https://jackal-api.lavenderfive.com:443",
                    "provider": "Lavender.Five Nodes 🐝",
                },
            ],
        },
    }

    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        chain_reg
    )

    assert await query_chain_registry(mock_client, "jackal") == chain_reg


async def test_remove_bad_api_exception(mock_client, emptyApi: Api):
    invalid_json = {
        "block": {
            "height": "1",
        }
    }

    mock_client.get.return_value.__aenter__.return_value.status = 200
    mock_client.get.return_value.__aenter__.return_value.read.return_value = json.dumps(
        invalid_json
    )

    with pytest.raises(BaseException):
        await remove_bad_apis(
            mock_client,
            {"https://api.jackalprotocol.com": emptyApi},
            "blocks/",
        )
