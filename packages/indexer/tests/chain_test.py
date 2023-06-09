import asyncio
import json
import os
import re
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from aiohttp import ClientResponse, ClientSession
from aioresponses import aioresponses
from indexer.chain import (
    Api,
    CosmosChain,
    get_chain_from_environment,
    is_valid_response,
    query_chain_registry,
    remove_bad_apis,
)
from indexer.manager import Manager
from pytest_mock import MockerFixture, MockFixture, mocker


async def test_api_get(chain: CosmosChain, manager: Manager):
    with aioresponses() as m:
        exp_res = {"block": {"mock_key": "mock_response"}}
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(exp_res),
        )

        result = await chain.get_block_txs(manager, "1")
        assert exp_res == result


async def test_api_get_wrong_status(
    chain: CosmosChain,
    manager: Manager,
):
    exp_res = {"block": {"mock_key": "mock_response"}}
    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=400,
            body=json.dumps(exp_res),
        )

    assert None == await chain.get_block_txs(manager, "1")


async def test_api_get_invalid_keys(chain: CosmosChain, manager: Manager):
    exp_res = {"code": 500, "message": "mock_response", "details": "mock_details"}
    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=500,
            body=json.dumps(exp_res),
        )

    assert None == await chain.get_block(manager)


async def test_api_get_invalid_json(chain: CosmosChain, manager: Manager):
    exp_res = "invalid_json"

    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(exp_res),
        )

    assert None == await chain.get_block(manager)


async def test_block_get_wrong_chain(
    chain: CosmosChain,
    manager: Manager,
):
    exp_res = {"block": {"header": {"height": 1, "chain_id": "wrong_chain_id"}}}
    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(exp_res),
        )

    result = await chain.get_block(manager)
    assert None == result


async def test_valid_block_valid_chain(
    chain: CosmosChain,
    manager: Manager,
):
    exp_res = {"block": {"header": {"height": 1, "chain_id": "chain_id"}}}
    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(exp_res),
        )

        assert exp_res == await chain.get_block(manager)


async def test_get_lowest_height_on_api(chain: CosmosChain, manager: Manager):
    exp_res = {
        "code": 3,
        "message": "height 1 is not available, lowest height is 2: invalid request",
        "details": [],
    }
    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(exp_res),
        )

        assert 2 == await chain.get_lowest_height(manager)


async def test_chain_environment_fail_no_chain_registry(
    mocker: MockerFixture, session: ClientSession
):
    mocker.resetall()
    with pytest.raises(OSError):
        assert None == await get_chain_from_environment(session)


async def test_chain_environment_time_between_blocks_not_int(session: ClientSession):
    os.environ["TIME_BETWEEN_BLOCKS"] = "not a string"
    with pytest.raises(EnvironmentError):
        await get_chain_from_environment(session)

    os.environ["TIME_BETWEEN_BLOCKS"] = "1"


async def test_chain_environment_pass(
    mocker: MockerFixture, emptyApi: Api, session: ClientSession
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

    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(exp_res),
        )

    # only external apis
    os.environ["CHAIN_REGISTRY_NAME"] = "jackal"
    assert await get_chain_from_environment(session) == CosmosChain(
        chain_id="jackal-1",
        chain_registry_name="jackal",
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

    with pytest.raises(EnvironmentError):
        await get_chain_from_environment(session)

    # only internal apis
    os.environ["LOAD_CHAIN_REGISTRY_APIS"] = "FALSE"
    os.environ[
        "APIS"
    ] = "https://api.jackalprotocol.com,https://jackal-api.lavenderfive.com:443"
    assert await get_chain_from_environment(session) == CosmosChain(
        chain_id="jackal-1",
        chain_registry_name="jackal",
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
    assert await get_chain_from_environment(session) == CosmosChain(
        chain_id="jackal-1",
        chain_registry_name="jackal",
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
    assert await get_chain_from_environment(session) == CosmosChain(
        chain_id="jackal-1",
        chain_registry_name="jackal",
        blocks_endpoint="/cosmos/base/tendermint/v1beta1/blocks/{}",
        txs_endpoint="/cosmos/tx/v1beta1/txs?events=tx.height={}",
        apis={
            "https://api.jackalprotocol.com": emptyApi,
            "https://jackal-api.lavenderfive.com:443": emptyApi,
        },
        current_api_index=0,
        time_between_blocks=1,
    )


async def test_query_chain_registry(session: ClientSession):
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

    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(chain_reg),
        )

        assert await query_chain_registry(session, "jackal") == chain_reg


async def test_remove_bad_api_exception(emptyApi: Api, session: ClientSession):
    invalid_json = {
        "block": {
            "height": "1",
        }
    }

    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(invalid_json),
        )

        with pytest.raises(BaseException):
            await remove_bad_apis(
                session,
                {"https://api.jackalprotocol.com": emptyApi},
                "blocks/",
            )


async def test_is_error_response():
    assert False == await is_valid_response(
        r={"code": "test", "message": "test", "details": "test"}, resp=Mock(status=200)
    )
    assert False == await is_valid_response({}, Mock(status=200))


def test_get_next_api_only_one(chain: CosmosChain):
    api = "https://api.jackalprotocol.com"
    chain.apis = {api: Api(hit=0, miss=0, times=[])}
    chain.current_api_index = 0
    assert len(chain.apis) == 1
    assert len(chain.apis.values()) == 1
    assert chain.get_next_api() == api
