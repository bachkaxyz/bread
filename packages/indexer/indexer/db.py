import json


async def create_tables(pool):
    async with pool.acquire() as conn:

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw (
                chain_id TEXT NOT NULL,
                height BIGINT NOT NULL,
                block JSONB,      
                txs JSONB,
                          
                PRIMARY KEY (chain_id, height)
            );
        """
        )

        await conn.execute(
            """
              CREATE TABLE IF NOT EXISTS blocks (
                height BIGINT NOT NULL,
                chain_id TEXT NOT NULL,
                time TIMESTAMP NOT NULL,
                block_hash TEXT NOT NULL,
                proposer_address TEXT NOT NULL
            );"""
        )

        await conn.execute(
            """ 
            CREATE OR REPLACE FUNCTION parse_raw() RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO blocks (height, chain_id, time, block_hash, proposer_address)
                VALUES (
                    NEW.height,
                    NEW.chain_id,
                    (NEW.block->'block'->'header'->'time')::TEXT::timestamp without time zone,
                    (NEW.block->'block_id'->'hash')::TEXT,
                    (NEW.block->'block'->'header'->'proposer_address')::TEXT
                );
                RETURN NEW;
            END
            $$ LANGUAGE plpgsql;
            CREATE OR REPLACE TRIGGER raw_insert
            BEFORE INSERT
            ON raw
            FOR EACH ROW EXECUTE PROCEDURE parse_raw();
            """
        )


async def upsert_block(pool, block: dict, txs: dict):
    async with pool.acquire() as conn:
        chain_id, height = block["block"]["header"]["chain_id"], int(
            block["block"]["header"]["height"]
        )
        await conn.execute(
            """
            INSERT INTO raw (chain_id, height, block, txs)
            VALUES ($1, $2, $3, $4)
            """,
            chain_id,
            height,
            json.dumps(block),
            json.dumps(txs),
        )
        # for some reason this doesn't support params
        await conn.execute(
            f"""
            NOTIFY raw, '{chain_id} {height}';
            """
        )
