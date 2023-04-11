import json
import os, asyncio
from aiohttp import ClientSession
from asyncpg import Pool, create_pool, Connection
from indexer.chain import LATEST, CosmosChain, get_chain_from_environment
from indexer.db import create_tables, upsert_data

from indexer.parser import Raw
from indexer.main import main

current_height = 0


async def live(session: ClientSession, chain: CosmosChain, pool: Pool):
    global current_height
    print("pulling live data")
    raw = await get_data_live(session, chain, current_height)
    if raw and raw.height and raw.height > current_height:
        current_height = raw.height
        print(f"new height {raw.height=}")
        async with pool.acquire() as conn:
            async with conn.transaction():
                await upsert_data(conn, raw)
    print("live finished")


async def get_data_live(
    session: ClientSession, chain: CosmosChain, current_height: int
) -> Raw | None:
    raw = Raw()
    block_res_json = await chain.get_block(session, LATEST)
    if block_res_json is not None:
        raw.parse_block(block_res_json)
        if raw.height and raw.height > current_height:
            print("new block")
            if raw.block and raw.block_tx_count > 0:
                tx_res_json = await chain.get_block_txs(
                    session=session,
                    height=raw.height,
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
            print(f"block already processed {raw.height=} {current_height=}")
            return None
    else:
        print("block data is none")
        return None
    return raw


if __name__ == "__main__":
    asyncio.run(main(live))
