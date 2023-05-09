import asyncio
from dagster import ConfigurableResource
import pandas as pd
from asyncpg import Connection, Pool, create_pool


class PostgresResource(ConfigurableResource):
    _pool: Pool
    _schema: str

    async def create_schema(self, schema: str):
        async with self._pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
