from aiohttp import ClientSession
from asyncpg import Pool
from indexer.chain import LATEST, CosmosChain
from indexer.db import upsert_data, get_max_height
from indexer.parser import Raw
from indexer.parser import process_block

current_height = 0


async def live(session: ClientSession, chain: CosmosChain, pool: Pool):
    global current_height
    print("pulling live data")
    print(f"{current_height=}")
    if current_height == 0:
        async with pool.acquire() as conn:
            current_height = await get_max_height(conn, chain)
    print(f"{current_height=}")
    raw = await get_data_live(session, chain, current_height)
    if raw and raw.height and raw.height > current_height:
        current_height = raw.height
        print(f"new height {raw.height=}")
        await upsert_data(pool, raw)
    print("live finished")


async def get_data_live(
    session: ClientSession, chain: CosmosChain, current_height: int
) -> Raw | None:
    block_res_json = await chain.get_block(session, LATEST)
    if block_res_json is not None:
        block_json_height = int(block_res_json["block"]["header"]["height"])
        if block_json_height > current_height:
            print("new block")
            return await process_block(block_res_json, session, chain)
        else:
            print(f"block already processed {block_json_height=} {current_height=}")
            return None
    else:
        print("block data is none")
        return None