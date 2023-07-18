import asyncio
from dagster import ConfigurableResource
import pandas as pd
from asyncpg import Connection, Pool, create_pool, connect


class PostgresResource(ConfigurableResource):
    host: str
    port: str
    user: str
    password: str
    database: str
    s: str  # schema

    async def get_conn(self) -> Connection:
        return await connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    async def create_schema(self):
        conn = await self.get_conn()
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.s};")
