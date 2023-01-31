import asyncio, aiohttp, json, asyncpg, os
from typing import List
from dotenv import load_dotenv
from indexer.chain_mapper import CosmosChain, chain_mapping
from indexer.data import get_block, get_txs
from indexer.db import create_tables, drop_tables, get_missing_blocks, upsert_block
import traceback

load_dotenv()


async def get_live_chain_data(
    chain: CosmosChain,
    pool,
    session: aiohttp.ClientSession,
):
    current_api_index = 0
    last_block = 0
    # async with aiohttp.ClientSession() as session:
    while True:
        try:
            block_data = await get_block(session, chain.apis[current_api_index])
            current_block = int(block_data["block"]["header"]["height"])
            if last_block >= current_block:
                # print(f"{chain_id} - block {current_block} already indexed")
                await asyncio.sleep(1)
            else:
                print(f"{chain.chain_id} - block {current_block} indexed")
                last_block = current_block
                # need to come back to this
                try:
                    txs_data = await get_txs(
                        session, chain.apis[current_api_index], current_block
                    )
                except:
                    print(f"failed to get txs for {chain.chain_id} - {current_block}")
                    txs_data = []
                try:
                    await upsert_block(pool, block_data, txs_data)
                except Exception as e:
                    print(
                        f"upsert_block error {repr(e)} - {chain.chain_id} - {current_block}"
                    )
                    traceback.print_exc()

                    # print(f"postgres error {repr(e)} - {chain_id} - {current_block}")

            # upsert block data to db
            # chain_id, current_height, json.dump(res)

        except Exception as e:
            print(
                f"live - {chain.chain_id} - Failed to get a block from {chain.apis[current_api_index].url} - {repr(e)}"
            )
            current_api_index = (current_api_index + 1) % len(chain.apis)


async def backfill_data(
    chain: CosmosChain, pool: asyncpg.pool, session: aiohttp.ClientSession
):
    current_api_index = 0
    async for height in get_missing_blocks(pool, session, chain):
        print(f"backfilling {chain.chain_id} {height}")
        block_data = await get_block(session, chain.apis[current_api_index], height)
        try:
            txs_data = await get_txs(session, chain.apis[current_api_index], height)
        except:
            print(f"failed to get txs for {chain.chain_id} - {height}")
            txs_data = []
        try:
            await upsert_block(pool, block_data, txs_data)
        except Exception as e:
            print(f"upsert_block error {repr(e)} - {chain.chain_id} - {height}")
            traceback.print_exc()


async def listen_raw_blocks(conn, pid, channel, payload):
    print(f"New block: {payload}")
    chain_id, height = payload.split(" ")
    height = int(height)
    raw_block = await conn.fetch(
        "SELECT * FROM raw WHERE chain_id = $1 AND height = $2", chain_id, height
    )
    chain_id, height, block, txs = raw_block


async def main():

    async with asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        command_timeout=60,
    ) as pool:
        # drop tables for testing purposes
        # await drop_tables(pool)

        await create_tables(pool)

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                *[get_live_chain_data(chain, pool, session) for chain in chain_mapping],
                *[backfill_data(chain, pool, session) for chain in chain_mapping]
                # conn.add_listener("raw_blocks", listen_raw_blocks),
            )


if __name__ == "__main__":
    asyncio.run(main())
