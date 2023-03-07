import asyncio
import asyncore
import traceback
import aiohttp
import asyncpg
from indexer.chain import CosmosChain
from indexer.db import upsert_raw_blocks, upsert_raw_txs


async def process_block(
    height: int,
    chain: CosmosChain,
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    sem: asyncore.Semaphore,
    block_data: dict = None,
) -> bool:
    if block_data is None:
        # this is because block data might be passed in from the live chain data (so removing a duplicated request)
        block_data = await chain.get_block(session, sem, height)
    try:
        await upsert_raw_blocks(pool, block_data)
        return True
    except Exception as e:
        print(f"upsert_block error {repr(e)} - {height}")
        traceback.print_exc()
        return False


async def process_tx(
    height: int,
    chain: CosmosChain,
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
):
    print(f"processing tx {height}")
    txs_data = await chain.get_block_txs(session, sem, height)
    try:
        if txs_data is None:
            print(f"txs_data is None")
            raise Exception("txs_data is None")
        else:
            txs_data = txs_data["tx_responses"]
            await upsert_raw_txs(pool, {height: txs_data}, chain.chain_id)

            print(f"upserting tx {height}")
    except Exception as e:
        print(f"upsert_txs error {repr(e)} - {height}")
        print(f"trying again... {txs_data}")
        traceback.print_exc()
