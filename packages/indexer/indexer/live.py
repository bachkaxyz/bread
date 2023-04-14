from aiohttp import ClientSession
from asyncpg import Pool
from indexer.chain import LATEST, CosmosChain
from indexer.db import upsert_data, get_max_height
from indexer.parser import Raw
from indexer.parser import process_block

# this is outside of live so that it can be accessed by the live function on every iteration
current_height = 0


async def live(session: ClientSession, chain: CosmosChain, pool: Pool):
    """Pull live data from the chain and upsert it into the database.

    Args:
        session (ClientSession): ClientSession for making requests to the chain
        chain (CosmosChain): Chain object for making chain specific requests
        pool (Pool): Database connection pool
    """
    global current_height
    print("pulling live data")
    print(f"{current_height=}")

    # since we are using a global variable, we need to check if it is 0 and if so, get the max height from the database
    if current_height == 0:
        async with pool.acquire() as conn:
            current_height = await get_max_height(conn, chain)
    print(f"{current_height=}")

    # get the latest block from the chain
    raw = await get_data_live(session, chain, current_height)

    # if the latest block is defined and it is new, upsert it into the database
    if raw and raw.height and raw.height > current_height:
        current_height = raw.height
        print(f"new height {raw.height=}")
        await upsert_data(pool, raw)
    print("live finished")


async def get_data_live(
    session: ClientSession, chain: CosmosChain, current_height: int
) -> Raw | None:
    """Get the latest block from the chain and process it.

    Args:
        session (ClientSession): ClientSession for making requests to the chain
        chain (CosmosChain): Chain object for making chain specific requests
        current_height (int): The current height of our indexer

    Returns:
        Raw | None: _description_
    """
    block_res_json = await chain.get_block(session, LATEST)
    if block_res_json is not None:
        block_json_height = int(block_res_json["block"]["header"]["height"])
        # if new height process block and its txs
        if block_json_height > current_height:
            print("new block")
            return await process_block(block_res_json, session, chain)
        else:
            print(f"block already processed {block_json_height=} {current_height=}")
            return None
    else:
        print("block data is none")
        return None
