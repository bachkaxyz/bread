import asyncio
from dataclasses import Field, dataclass, field
import json
import os
import traceback
from typing import Coroutine, List, Set, Tuple
from aiohttp import ClientResponse, ClientSession
import aiohttp
from asyncpg import create_pool, Connection
from indexer.exceptions import APIResponseError, BlockNotParsedError
from indexer.parser import Log, parse_logs
from datetime import datetime

from indexer.round_2 import _get, block_endpoint, tx_endpoint, base_api, upsert_data
from indexer.round_2_types import Raw

min_block_height = 116001


async def main():
    async with create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
    ) as pool:
        async with ClientSession() as session:
            while True:
                async with pool.acquire() as conn:
                    conn: Connection

                    async with conn.transaction():
                        wrong_txs = await conn.execute(
                            """
                            select height
                            from raw
                            where tx_tx_count <> block_tx_count
                            """
                        )
                        if len(wrong_txs) == 0:
                            print("no wrong txs")
                        await asyncio.sleep(2)
                        async for record in conn.cursor(
                            """
                            select height, difference_per_block from (
                                select height, COALESCE(height - LAG(height) over (order by height), -1) as difference_per_block, chain_id
                                from raw
                                where chain_id = 'jackal-1'
                            ) as dif
                            where difference_per_block <> 1
                            order by height desc
                            """
                        ):

                            height, dif = record
                            print(f"{height=} {dif=}")
                            if dif == -1:
                                print("min block in db")

                                lowest_height = await get_lowest_height(
                                    session, base_api, block_endpoint
                                )

                                if height - 20 > lowest_height:
                                    dif = 20
                                else:
                                    dif = height - lowest_height
                            print(height, dif)
                            step_size = 10
                            max_height = height - 1
                            min_height = height - dif
                            current_height = max_height
                            while current_height > min_height:
                                print(f"{current_height}")
                                if current_height - step_size > min_height:
                                    query_lower_bound = current_height - step_size
                                else:
                                    query_lower_bound = min_height
                                print(
                                    f"querying range {current_height} - {query_lower_bound}"
                                )
                                results = await asyncio.gather(
                                    *[
                                        get_data_historical(session, h)
                                        for h in range(
                                            current_height, query_lower_bound, -1
                                        )
                                    ]
                                )
                                async with pool.acquire() as conn2:
                                    for res in results:
                                        if res:
                                            await upsert_data(conn2, res)
                                print("data upserted")
                                current_height = query_lower_bound


async def get_data_historical(session: ClientSession, height: int) -> Raw | None:
    raw = Raw()
    print(f"pulling new data {height}")
    block_res_json = await _get(base_api, block_endpoint.format(height), session)
    print(f"block returned {height}")
    if block_res_json is not None:
        raw.parse_block(block_res_json)
        if raw.block:
            if raw.block_tx_count > 0:
                print(f"getting tx {height}")
                tx_res_json = await _get(base_api, tx_endpoint.format(height), session)
                print(f"returned tx {height}")
                if tx_res_json is not None and "tx_responses" in tx_res_json:
                    tx_responses = tx_res_json["tx_responses"]
                    raw.parse_tx_responses(tx_responses)
                    if raw.block_tx_count != raw.tx_responses_tx_count:
                        return Raw(
                            block_tx_count=raw.block_tx_count,
                            tx_responses_tx_count=0,
                            block=raw.block,
                        )
                else:
                    print("tx_response is not a key or tx_res_json is none")
                    return Raw(
                        block_tx_count=raw.block_tx_count,
                        tx_responses_tx_count=0,
                        block=raw.block,
                    )
        else:
            print("block data is None")
            return None
    return raw


async def get_lowest_height(session: ClientSession, base_api: str, endpoint: str):
    block_res = await session.get(base_api + endpoint.format(1))
    block_res_json = await block_res.json()
    lowest_height = (
        block_res_json["message"].split("height is ")[-1].split(":")[0].lstrip()
    )
    return int(lowest_height)


if __name__ == "__main__":
    asyncio.run(main())
