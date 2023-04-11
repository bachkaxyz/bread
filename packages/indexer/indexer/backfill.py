import asyncio
from dataclasses import Field, dataclass, field
import json
import os
import traceback
from typing import Coroutine, List, Set, Tuple
from aiohttp import ClientResponse, ClientSession
import aiohttp
from asyncpg import Pool, create_pool, Connection
from indexer.chain import CosmosChain, get_chain_from_environment
from indexer.db import missing_blocks_cursor
from indexer.exceptions import APIResponseError, BlockNotParsedError
from indexer.parser import Log, Raw, parse_logs
from datetime import datetime

from indexer.db import upsert_data
from indexer.main import main

min_block_height = 116001


async def backfill(session: ClientSession, chain: CosmosChain, pool: Pool):
    print("starting backfill")
    async with pool.acquire() as cursor_conn:
        cursor_conn: Connection

        async with cursor_conn.transaction():
            wrong_txs = await cursor_conn.execute(
                """
                select height
                from raw
                where tx_tx_count <> block_tx_count
                """
            )
            if len(wrong_txs) == 0:
                print("no wrong txs")
            else:
                print(wrong_txs)
            await asyncio.sleep(2)

            async for record in missing_blocks_cursor(cursor_conn, chain):
                height, dif = record
                print(f"{height=} {dif=}")
                if dif == -1:
                    print("min block in db")

                    lowest_height = await chain.get_lowest_height(session)

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
                    print(f"querying range {current_height} - {query_lower_bound}")
                    results: List[Raw | None] = await asyncio.gather(
                        *[
                            get_data_historical(session, chain, h)
                            for h in range(current_height, query_lower_bound, -1)
                        ]
                    )
                    async with pool.acquire() as conn2:
                        for res in results:
                            if res:
                                await upsert_data(conn2, res)
                    print("data upserted")
                    current_height = query_lower_bound
    print("finish backfill task")


async def get_data_historical(
    session: ClientSession, chain: CosmosChain, height: int
) -> Raw | None:
    raw = Raw()
    print(f"pulling new data {height}")
    block_res_json = await chain.get_block(session, height=height)
    print(f"block returned {height}")
    if block_res_json is not None:
        raw.parse_block(block_res_json)
        if raw.block and raw.height:
            if raw.block_tx_count > 0:
                print(f"getting tx {height}")
                tx_res_json = await chain.get_block_txs(
                    session=session,
                    height=raw.height,
                )
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


if __name__ == "__main__":
    asyncio.run(main(backfill))
