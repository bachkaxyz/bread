import json
import os
import traceback
from typing import Tuple
from aiohttp import ClientError, ClientResponse, ClientSession
import asyncio
from base64 import b64decode
from asyncpg import create_pool, Connection
import datetime
from indexer.db import create_tables
from indexer.exceptions import APIResponseError

from indexer.parser import Raw, parse_logs

base_api = "https://jackal.nodejumper.io:1317"
block_endpoint = os.getenv(
    "BLOCKS_ENDPOINT", "/cosmos/base/tendermint/v1beta1/blocks/{}"
)
tx_endpoint = os.getenv("TXS_ENDPOINT", "/cosmos/tx/v1beta1/txs?events=tx.height={}")


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
        base_api (str): base endpoint to query from
        endpoint (str): endpoint to query from base_api
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
            print(f"aiohttp client error with {endpoint=}")
            traceback.print_exc()
        retries += 1
    return None


async def main():
    current_height = 0
    schema_name = os.getenv("INDEXER_SCHEMA", "public")
    print(schema_name)
    async with create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        server_settings={"search_path": schema_name},
    ) as pool:
        async with pool.acquire() as conn:
            conn: Connection
            await create_tables(conn, schema_name)
        while True:
            async with ClientSession() as session:
                print("pulling new data")
                raw = await get_data_live(session, current_height)
                if raw and raw.height and raw.height > current_height:
                    current_height = raw.height
                    print(f"new height {raw.height=}")
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            await raw.upsert_data(conn)


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
