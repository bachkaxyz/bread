import asyncio
from typing import Any, Dict, Optional
from aiohttp import ClientSession
from aiohttp.client import _RequestContextManager
from asyncpg import Connection, Pool, create_pool


class Manager:
    """
    High Level Context Manager for postgres database and aiohttp session

    This class has to be used with async context manager since we need to wait for the pool to be created in a async function

    To use:
    ```python
    async with Manager(
        db_kwargs=dict(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB"),
            server_settings={"search_path": schema_name},
            command_timeout=60,
        ),
        session_kwargs=dict(),
    ) as manager:
        # To use the database connection pool:
        async with (await manager.getPool()).acquire() as conn:
            conn.execute(...)

        # To use the aiohttp session:
        async with manager.get(f"{cur_api}{endpoint}") as resp:
            await resp.json()
            ...
    ```
    """

    pool: Pool
    setup_task: asyncio.Task

    def __init__(
        self,
        db_kwargs: Optional[Dict[str, Any]] = None,
        session_kwargs: Optional[Dict[str, Any]] = None,
    ):
        # this class has to be used with async context manager since we need to wait for the pool to be created in a async function
        self.db_kwargs = db_kwargs
        self.session_kwargs = session_kwargs
        self.session = ClientSession(**(session_kwargs or {}))

        self.setup_task = asyncio.create_task(self.setup_pool(db_kwargs))

    async def setup_pool(
        self,
        db_kwargs: Optional[Dict[str, Any]] = None,
    ):
        if hasattr(self, "pool"):
            await self.pool.close()
        p = await create_pool(**(db_kwargs or {}))
        if p:
            self.pool = p
        else:
            raise Exception("Could not create pool")

    def get(self, url: str, **kwargs) -> _RequestContextManager:
        return self.getSession().get(url, **kwargs)

    def getSession(self) -> ClientSession:
        # creates a new session if the current session is closed
        if self.session.closed:
            self.session = ClientSession()
        return self.session

    async def getPool(self) -> Pool:
        if self.pool._closed:
            await self.setup_pool(self.db_kwargs)
        return self.pool

    async def __aexit__(self, *args, **kwargs):
        await self.session.close()
        await self.pool.close()

    async def __aenter__(self, *args, **kwargs):
        await self.setup_task
        return self
