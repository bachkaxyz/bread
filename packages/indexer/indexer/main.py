import asyncio, aiohttp, json, asyncpg, os
from typing import List
from dotenv import load_dotenv
from indexer.chain_mapper import CosmosChain, chain_mapping
from indexer.data import get_block, get_txs
from indexer.db import create_tables, drop_tables, get_missing_blocks, upsert_block
import traceback

load_dotenv()


async def process_block(
    chain: CosmosChain,
    height: int,
    pool,
    session: aiohttp.ClientSession,
    block_data: dict = None,
) -> bool:
    if block_data is None:
        # this is because block data might be passed in from the live chain data (so removing a rerequest)
        block_data = await get_block(
            session, chain.apis[chain.current_api_index], height
        )

    # need to come back to this
    txs_data = await get_txs(session, chain.apis[chain.current_api_index], height)
    if txs_data is None or block_data is None:
        print(
            f"{'tx_data' if txs_data is None else 'block_data'} is None - {chain.chain_id}"
        )
        return False
    try:
        await upsert_block(pool, block_data, txs_data)
        return True
    except Exception as e:
        print(f"upsert_block error {repr(e)} - {chain.chain_id} - {height}")
        traceback.print_exc()
        return False


async def get_live_chain_data(
    chain: CosmosChain,
    pool,
    session: aiohttp.ClientSession,
):
    last_block = 0
    while True:
        try:
            block_data = await get_block(session, chain.apis[chain.current_api_index])
            if block_data is not None:
                current_block = int(block_data["block"]["header"]["height"])
                if last_block >= current_block:
                    # print(f"{chain_id} - block {current_block} already indexed")
                    await asyncio.sleep(1)
                else:
                    last_block = current_block
                    if await process_block(
                        chain=chain,
                        height=current_block,
                        pool=pool,
                        session=session,
                        block_data=block_data,
                    ):
                        print(
                            f"processed live block {current_block} - {chain.chain_id}"
                        )
                    else:
                        # should we save this to db?
                        print(
                            f"failed to process block {current_block} - {chain.chain_id}"
                        )

            else:
                print(f"block_data is None - {chain.chain_id}")
                # save error to db

            # upsert block data to db
            # chain_id, current_height, json.dump(res)

        except Exception as e:
            print(
                f"live - {chain.chain_id} - Failed to get a block from {chain.apis[chain.current_api_index].url} - {repr(e)}"
            )
            chain.current_api_index = (chain.current_api_index + 1) % len(chain.apis)


async def backfill_data(
    chain: CosmosChain, pool: asyncpg.pool, session: aiohttp.ClientSession
):
    async for height in get_missing_blocks(pool, session, chain):
        if await process_block(chain=chain, height=height, pool=pool, session=session):
            print(f"backfilled block {height} - {chain.chain_id}")
        else:
            print(f"failed to backfill block {height} - {chain.chain_id}")


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
        await drop_tables(pool)

        await create_tables(pool)

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                *[get_live_chain_data(chain, pool, session) for chain in chain_mapping],
                *[backfill_data(chain, pool, session) for chain in chain_mapping]
                # conn.add_listener("raw_blocks", listen_raw_blocks),
            )


if __name__ == "__main__":
    asyncio.run(main())
