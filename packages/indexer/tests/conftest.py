import os
from typing import Any, Dict

import pytest
from aiohttp import ClientSession
from asyncpg import create_pool
from indexer.manager import Manager


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


@pytest.fixture()
async def pool(db_kwargs: Dict[str, Any]):
    return await create_pool(**db_kwargs)


@pytest.fixture
async def storage_config():
    session = ClientSession()
    storage = Storage(session=session)
    BUCKET_NAME = os.getenv("BUCKET_NAME", "sn-mono-indexer-test")
    bucket = storage.get_bucket(BUCKET_NAME)  # your bucket name
    yield session, storage, bucket

    await session.close()


@pytest.fixture
def manager(db_kwargs):
    return Manager(db_kwargs=db_kwargs)
