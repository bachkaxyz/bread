import json


async def create_tables(pool):
    async with pool.acquire() as conn:

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS blocks (
                chain_id TEXT NOT NULL,
                height BIGINT NOT NULL,
                block JSONB,
                
                PRIMARY KEY (chain_id, height)
            );
        """
        )


async def upsert_block(pool, block: dict):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO blocks (chain_id, height, block)
            VALUES ($1, $2, $3)
            ON CONFLICT (chain_id, height) DO UPDATE SET block = $3;
            NOTIFY new_block;
        """,
            block["block"]["header"]["chain_id"],
            int(block["block"]["header"]["height"]),
            json.dumps(block),
        )
