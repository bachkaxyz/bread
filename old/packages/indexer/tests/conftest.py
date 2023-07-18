import os
from pathlib import Path
from typing import Any, Dict
from indexer.chain import Api, CosmosChain

import pytest
from aiohttp import ClientSession
from asyncpg import create_pool
from indexer.manager import Manager
from gcloud.aio.storage import Storage

# use parser fixtures
from parse.fixtures import *


@pytest.fixture
def schema() -> str:
    return "public"


@pytest.fixture
def db_kwargs(schema: str):
    return dict(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        server_settings={"search_path": schema},
    )


@pytest.fixture
def session_kwargs():
    return dict()


@pytest.fixture()
async def pool(db_kwargs: Dict[str, Any]):
    async with create_pool(**db_kwargs) as pool:
        yield pool


@pytest.fixture
def setup_temp_storage_structure(chain: CosmosChain):
    # create temp file structure
    os.makedirs(f"{chain.chain_registry_name}/{chain.chain_id}/blocks", exist_ok=True)
    os.makedirs(f"{chain.chain_registry_name}/{chain.chain_id}/txs", exist_ok=True)


@pytest.fixture
async def storage_config(setup_temp_storage_structure):
    session = ClientSession()
    storage = Storage(session=session)
    BUCKET_NAME = os.getenv("BUCKET_NAME", "sn-mono-indexer-test")
    bucket = storage.get_bucket(BUCKET_NAME)  # your bucket name
    yield session, storage, bucket

    await session.close()


@pytest.fixture
async def manager(db_kwargs, session_kwargs):
    async with Manager(db_kwargs=db_kwargs, session_kwargs=session_kwargs) as manager:
        yield manager


@pytest.fixture
def emptyApi():
    return Api({"hit": 0, "miss": 0, "times": []})


@pytest.fixture
async def session():
    async with ClientSession() as session:
        yield session


@pytest.fixture
def chain(emptyApi: Api):
    apis = {
        "https://mock_api.com/": emptyApi,
        "https://mock_api2.com/": emptyApi,
    }
    chain = CosmosChain(
        chain_id="chain_id",
        chain_registry_name="chain_registry_name",
        blocks_endpoint="cosmos/blocks/{}",
        txs_endpoint="cosmos/txs/{}",
        apis=apis,
    )
    return chain
