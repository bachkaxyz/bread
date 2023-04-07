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

from indexer.round_2 import _get, block_endpoint, tx_endpoint, base_api
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
                        else:
                            print(
                                f"not min block we have a gap of {dif=} so we should query heights {height-1} - {height - dif + 1}"
                            )
                            results: List[Raw | None] = await asyncio.gather(
                                *[
                                    get_data_historical(session, h)
                                    for h in range(height - dif + 1, height)
                                ]
                            )

                            for res in results:
                                print(f"{res=}")


async def get_data_historical(session: ClientSession, height: int) -> Raw | None:
    raw = Raw()
    print("pulling new data")
    block_res_json = await _get(base_api, block_endpoint.format(height), session)
    if block_res_json is not None:
        raw.parse_block(block_res_json)
        if raw.block:
            if raw.block_tx_count > 0:
                tx_res_json = await _get(base_api, tx_endpoint.format(height), session)
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


if __name__ == "__main__":
    asyncio.run(main())
