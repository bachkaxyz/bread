import asyncio
import aiohttp
import asyncpg
from indexer.chain import CosmosChain
from indexer.db import Database, upsert_raw_blocks, upsert_raw_txs


async def process_block(
    height: int,
    chain: CosmosChain,
    db: Database,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    block_data: dict = None,
) -> bool:
    if block_data is None:
        # this is because block data might be passed in from the live chain data (so removing a duplicated request)
        block_data = await chain.get_block(session, sem, height)
    try:
        if block_data is not None:
            await upsert_raw_blocks(db, block_data)
        else:
            raise Exception(f"block_data is None - {block_data}")
        return True
    except Exception as e:
        print(f"upsert_block error {repr(e)} - {height}")
        return False


async def process_tx(
    height: int,
    chain: CosmosChain,
    db: Database,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
) -> bool:
    txs_data = await chain.get_block_txs(session, sem, height)
    try:
        if txs_data is None:
            raise Exception("txs_data is None")
        else:
            txs_data = txs_data["tx_responses"]
            await upsert_raw_txs(db, {height: txs_data}, chain.chain_id)
            return True

    except Exception as e:
        print(f"upsert_txs error {repr(e)} - {height}")
        return False
