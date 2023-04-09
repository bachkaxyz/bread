import json
import os
import traceback
from typing import Tuple
from aiohttp import ClientError, ClientResponse, ClientSession
import asyncio
from base64 import b64decode
from asyncpg import create_pool, Connection
import datetime
from indexer.exceptions import APIResponseError

from indexer.parser import parse_logs
from indexer.data import Raw

base_api = "https://m-jackal.api.utsa.tech"
block_endpoint = "/cosmos/base/tendermint/v1beta1/blocks/{}"
tx_endpoint = "/cosmos/tx/v1beta1/txs?events=tx.height={}"


async def is_valid_response(resp: ClientResponse) -> bool:
    """Check if the response is in the correct format

    Args:
        resp (aiohttp.ClientResponse): raw response from the api call to check

    Returns:
        bool: True if the response is valid, False otherwise
    """
    # could we return specific error messages here to save to db?
    try:
        return (
            list((await get_json(resp)).keys()) != ["code", "message", "details"]
            and resp.status == 200
        )
    except Exception as e:
        return False


async def get_json(resp: ClientResponse) -> dict:
    return json.loads(await resp.read())


async def _get(
    base_api: str,
    endpoint: str,
    session: ClientSession,
    max_tries: int = 1,
) -> dict | None:
    """Get data from an endpoint with retries

    Args:
        endpoint (str): endpoint to
        session (aiohttp.ClientSession): _description_
        sem (asyncio.Semaphore): _description_
        max_retries (int): _description_

    Raises:
        APIResponseError: _description_

    Returns:
        Tuple[str, dict] | None: str is the api that was hit, dict is the response if valid, otherwise None
    """
    retries = 0

    while retries < max_tries:
        try:
            async with session.get(f"{base_api}{endpoint}") as resp:
                if await is_valid_response(resp):
                    return await get_json(resp)
                else:
                    raise APIResponseError("API Response Not Valid")
        except APIResponseError as e:
            print(f"failed to get {base_api}{endpoint}")
        except ConnectionRefusedError as e:
            print("connection refused error")
        except ClientError as e:
            print("aiohttp client error")
            traceback.print_exc()
        retries += 1
    return None


async def main():
    current_height = 0

    async with create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
    ) as pool:
        async with pool.acquire() as conn:
            conn: Connection
            # creating tables
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS raw (
                    chain_id TEXT NOT NULL,
                    height BIGINT NOT NULL,
                    block JSONB,
                    block_tx_count BIGINT NOT NULL,
                    tx_responses JSONB,
                    tx_tx_count BIGINT NOT NULL, 
                    created_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (chain_id, height)
                );
                CREATE TABLE IF NOT EXISTS blocks(
                    height BIGINT NOT NULL,
                    chain_id TEXT NOT NULL,
                    time TIMESTAMP NOT NULL,
                    block_hash TEXT NOT NULL,
                    proposer_address TEXT NOT NULL,
                    
                    PRIMARY KEY (chain_id, height),
                    FOREIGN KEY (chain_id, height) REFERENCES raw(chain_id, height) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS txs(
                    txhash TEXT NOT NULL PRIMARY KEY,
                    chain_id TEXT NOT NULL, 
                    height BIGINT NOT NULL,
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
                    FOREIGN KEY (chain_id, height) REFERENCES raw(chain_id, height) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS logs (
                    txhash TEXT NOT NULL,
                    msg_index TEXT NOT NULL, -- This should be an int
                    parsed JSONB,
                    failed BOOLEAN NOT NULL DEFAULT FALSE,
                    failed_msg TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    
                    PRIMARY KEY (txhash, msg_index),
                    FOREIGN KEY (txhash) REFERENCES txs(txhash) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS log_columns (
                    event TEXT NOT NULL,
                    attribute TEXT NOT NULL,
                    parse BOOLEAN NOT NULL DEFAULT FALSE,

                    PRIMARY KEY (event, attribute)
                );
                """
            )
        while True:
            async with ClientSession() as session:
                print("pulling new data")
                raw = await get_data_live(session, current_height)
                if raw and raw.height and raw.height > current_height:
                    current_height = raw.height
                    print(f"new height {raw.height=}")
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            await upsert_data(conn, raw)


async def upsert_data(conn: Connection, data: Raw):
    if not (data.height is None or data.chain_id is None or data.block is None):
        await conn.execute(
            f"""
            INSERT INTO raw(chain_id, height, block, block_tx_count, tx_responses, tx_tx_count)
            VALUES ($1, $2, $3, $4, $5, $6);
            """,
            *data.get_raw_db_params(),
        )

        await conn.execute(
            """
            INSERT INTO blocks(chain_id, height, time, block_hash, proposer_address)
            VALUES ($1, $2, $3, $4, $5);
            """,
            *data.block.get_db_params(),
        )
        await conn.executemany(
            """
            INSERT INTO txs(txhash, chain_id, height, code, data, info, logs, events, raw_log, gas_used, gas_wanted, codespace, timestamp)
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            """,
            data.get_txs_db_params(),
        )

        await conn.executemany(
            f"""
            INSERT INTO log_columns (event, attribute)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            """,
            data.get_log_columns_db_params(),
        )

        await conn.executemany(
            f"""
            INSERT INTO logs (txhash, msg_index, parsed, failed, failed_msg)
            VALUES (
                $1, $2, $3, $4, $5
            )
            """,
            data.get_logs_db_params(),
        )

        print(f"{data.height=} inserted")

    else:
        print(f"{data.height} {data.chain_id} {data.block}")


async def get_data_live(session: ClientSession, current_height: int) -> Raw | None:
    raw = Raw()
    block_res_json = await _get(base_api, block_endpoint.format("latest"), session)
    if block_res_json is not None:
        raw.parse_block(block_res_json)
        if raw.height and raw.height > current_height:
            print("new block")
            if raw.block and raw.block_tx_count > 0:
                tx_res_json = await _get(
                    base_api, tx_endpoint.format(raw.height), session
                )
                if tx_res_json is not None and "tx_responses" in tx_res_json:
                    tx_responses = tx_res_json["tx_responses"]
                    raw.parse_tx_responses(tx_responses)
                    if raw.block_tx_count != raw.tx_responses_tx_count:
                        print("tx count not right")
                        return Raw(
                            height=raw.height,
                            chain_id=raw.chain_id,
                            block_tx_count=raw.block_tx_count,
                            tx_responses_tx_count=0,
                            block=raw.block,
                            raw_block=raw.raw_block,
                        )
                else:
                    print("tx_response is not a key or tx_res_json is none")
                    return Raw(
                        height=raw.height,
                        chain_id=raw.chain_id,
                        block_tx_count=raw.block_tx_count,
                        tx_responses_tx_count=0,
                        block=raw.block,
                        raw_block=raw.raw_block,
                    )
            else:
                print("no txs")
                return Raw(
                    height=raw.height,
                    chain_id=raw.chain_id,
                    block_tx_count=raw.block_tx_count,
                    tx_responses_tx_count=0,
                    block=raw.block,
                    raw_block=raw.raw_block,
                )
        else:
            print("block already processed")
            return None
    else:
        print("block data is none")
        return None
    return raw


if __name__ == "__main__":
    asyncio.run(main())
