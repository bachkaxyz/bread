import os
from indexer.db import create_tables, drop_tables
import pytest
import asyncpg
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
async def pool():
    return await asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
    )



async def test_create_drop_tables(pool: asyncpg.pool):
    async def check_tables(pool, table_names, schema) -> int:
        async with pool.acquire() as conn:
            results = await conn.fetch(
                f"""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = $1
                AND table_name in {table_names}
                """,
                schema,
            )
        return len(results)
        
    await create_tables(pool)
    
    table_names = ('raw_blocks', 'raw_txs', 'blocks', 'txs', 'logs', 'log_columns')
   
    assert await check_tables(pool, table_names, "public") == len(table_names)
    
    await drop_tables(pool)
    
    assert await check_tables(pool, table_names, "public") == 0
    
    