import asyncio
import aiohttp
import asyncpg
from indexer.chain import CosmosChain
from indexer.db import Database, upsert_raw_blocks, upsert_raw_txs
from indexer.exceptions import ChainDataIsNoneError, ChainIdMismatchError


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
            if chain.chain_id == block_data["header"]["chain_id"]:
                await upsert_raw_blocks(db, block_data)
            else:
                raise ChainIdMismatchError(
                    f"chain_id mismatch - {chain.chain_id} - {block_data['header']['chain_id']} - {chain.apis[chain.current_api_index]}"
                )
        else:
            raise ChainDataIsNoneError(f"block_data is None - {block_data}")
        return True
    except ChainDataIsNoneError as e:
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
            raise ChainDataIsNoneError("txs_data is None")
        else:
            txs_data = txs_data["tx_responses"]
            await upsert_raw_txs(db, {height: txs_data}, chain.chain_id)
            return True

    except ChainDataIsNoneError as e:
        print(f"upsert_txs error {repr(e)} - {height}")
        return False
    except KeyError as e:
        print("tx_responses doesn't exist")
        return False
