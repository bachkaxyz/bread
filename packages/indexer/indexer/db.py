import json


async def drop_tables(pool):
    await pool.execute(
        """
        DROP TABLE IF EXISTS raw CASCADE;
        DROP TABLE IF EXISTS blocks CASCADE;
        DROP TABLE IF EXISTS txs CASCADE;
        """
    )


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
                proposer_address TEXT NOT NULL,
                
                PRIMARY KEY (chain_id, height)
            );
            CREATE TABLE IF NOT EXISTS txs (
                txhash TEXT NOT NULL PRIMARY KEY,
                chain_id TEXT NOT NULL,
                height BIGINT NOT NULL,
                tx JSONB,
                tx_response JSONB,
                tx_response_tx_type TEXT,
                code TEXT,
                data TEXT,
                info TEXT,
                logs JSONB,
                events JSONB,
                raw_log TEXT,
                gas_used BIGINT,
                gas_wanted BIGINT,
                codespace TEXT,
                timestamp TIMESTAMP,
                
                FOREIGN KEY (chain_id, height) REFERENCES blocks (chain_id, height)
            );
            """
        )

        await conn.execute(
            """ 
            CREATE OR REPLACE FUNCTION parse_raw() RETURNS TRIGGER AS $$
            DECLARE
                tx JSONB;
                tx_responses JSONB;
            BEGIN
                INSERT INTO blocks (height, chain_id, time, block_hash, proposer_address)
                VALUES (
                    NEW.height,
                    NEW.chain_id,
                    (NEW.block->'block'->'header'->'time')::TEXT::timestamp without time zone,
                    (NEW.block->'block_id'->>'hash')::TEXT,
                    (NEW.block->'block'->'header'->>'proposer_address')::TEXT
                );
                
                        
                FOR tx_responses IN SELECT * FROM jsonb_array_elements(NEW.txs->'tx_responses')
                LOOP
                    INSERT INTO txs (txhash, chain_id, height, tx_response, tx, tx_response_tx_type, code, data, info, logs, events, raw_log, gas_used, gas_wanted, codespace, timestamp)
                    VALUES (
                        tx_responses->>'txhash',
                        NEW.chain_id,
                        NEW.height,
                        tx_responses,
                        tx_responses->'tx',
                        tx_responses->'tx'->>'@type',
                        tx_responses->>'code',
                        tx_responses->>'data',
                        tx_responses->>'info',
                        tx_responses->'logs',
                        tx_responses->'events',
                        tx_responses->>'raw_log',
                        (tx_responses->>'gas_used')::BIGINT,
                        (tx_responses->>'gas_wanted')::BIGINT,
                        tx_responses->>'codespace',
                        (tx_responses->'timestamp')::TEXT::TIMESTAMP
                    );
                END LOOP;
                
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
