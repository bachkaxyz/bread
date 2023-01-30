import asyncio, aiohttp, json, time, asyncpg, os
from typing import List
from dotenv import load_dotenv
from indexer.chain_mapper import CosmosAPI, chain_mapping

load_dotenv("./../../../")


async def create_tables(conn: asyncpg.Connection):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blocks (
            chain_id TEXT NOT NULL,
            height BIGINT NOT NULL,
            block JSONB,
            
            PRIMARY KEY (chain_id, height)
        );
    """
    )


async def get_block(
    session: aiohttp.ClientSession, api: CosmosAPI, height: str = "latest"
):
    async with session.get(
        f"{api.url}/cosmos/base/tendermint/v1beta1/blocks/{height}"
    ) as resp:
        return await resp.json()


async def upsert_block(conn: asyncpg.Connection, block: dict):
    print(block.keys())


async def get_live_chain_data(
    chain_id: str,
    apis: List[CosmosAPI],
    conn: asyncpg.Connection,
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
                await upsert_block(conn, res)

            # upsert block data to db
            # chain_id, current_height, json.dump(res)

        except Exception as e:
            print(f"Failed to get block from {apis[current_api_index].url}")
            current_api_index = (current_api_index + 1) % len(apis)
            raise e


async def main():
    conn = await asyncpg.connect(
        os.getenv("DATABASE_URI"),
    )
    await create_tables(conn)

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[
                get_live_chain_data(chain_id, apis, conn, session)
                for chain_id, apis in chain_mapping.items()
            ]
            + [
                # we can add other tasks (function calls) here to also run in parallel
            ]
        )


if __name__ == "__main__":
    asyncio.run(main())
