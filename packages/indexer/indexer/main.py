import asyncio, aiohttp, json, asyncpg, os
from typing import List
from dotenv import load_dotenv
from indexer.chain_mapper import CosmosAPI, chain_mapping
from indexer.data import get_block
from indexer.db import create_tables, upsert_block

load_dotenv()


async def get_live_chain_data(
    chain_id: str,
    apis: List[CosmosAPI],
    pool,
    session: aiohttp.ClientSession,
):
    current_api_index = 0
    last_block = 0
    while True:
        try:
            res = await get_block(session, apis[current_api_index])
            current_block = int(res["block"]["header"]["height"])
            print(current_block)
            if last_block >= current_block:
                print(f"{chain_id} - block {current_block} already indexed")
                await asyncio.sleep(1)
            else:
                print(f"{chain_id} - block {current_block} indexed")
                last_block = current_block
                await upsert_block(pool, res)

            # upsert block data to db
            # chain_id, current_height, json.dump(res)

        except Exception as e:
            print(f"Failed to get block from {apis[current_api_index].url}")
            current_api_index = (current_api_index + 1) % len(apis)
            raise e


async def main():

    async with asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        command_timeout=60,
    ) as pool:

        await create_tables(pool)

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                *[
                    get_live_chain_data(chain_id, apis, pool, session)
                    for chain_id, apis in chain_mapping.items()
                ]
            )


if __name__ == "__main__":
    asyncio.run(main())
