import asyncio, aiohttp, json, asyncpg, os
from collections import defaultdict
import traceback
from typing import List
from dotenv import load_dotenv
from indexer.chain import CosmosChain
from indexer.db import (
    add_columns,
    add_current_log_columns,
    add_logs,
    create_tables,
    drop_tables,
    get_missing_blocks,
    get_table_cols,
    insert_dict,
    upsert_raw_blocks,
    upsert_raw_txs,
)
from indexer.parser import Log, parse_logs, parse_messages
import asyncpg_listen

load_dotenv()


async def process_block(
    chain: CosmosChain,
    height: int,
    pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    block_data: dict = None,
) -> bool:
    if block_data is None:
        # this is because block data might be passed in from the live chain data (so removing a rerequest)
        block_data = await chain.get_block(session, sem, height)

    # # need to come back to this
    # num_txs = len(block_data['block']['data']['txs'])
    # if num_txs != 0:
    #     txs_data = await chain.get_txs(session, sem, height)
    # else: 
    #     txs_data = None
    # if (txs_data is None and num_txs != 0) or block_data is None:
    #     print(f"{'tx_data' if txs_data is None else 'block_data'} is None")
    #     return False
    try:
        await upsert_raw_blocks(pool, block_data)
        return True
    except Exception as e:
        print(f"upsert_block error {repr(e)} - {height}")
        traceback.print_exc()
        return False


# async def get_live_chain_data(
#     chain: CosmosChain,
#     pool,
#     session: aiohttp.ClientSession,
# ):
#     last_block = 0
#     while True:
#         try:
#             block_data = await chain.get_block(
#                 session,
#             )
#             if block_data is not None:
#                 current_block = int(block_data["block"]["header"]["height"])
#                 if last_block >= current_block:
#                     # print(f"{chain_id} - block {current_block} already indexed")
#                     await asyncio.sleep(1)
#                 else:
#                     last_block = current_block
#                     if await process_block(
#                         chain=chain,
#                         height=current_block,
#                         pool=pool,
#                         session=session,
#                         block_data=block_data,
#                     ):
#                         print(f"processed live block - {current_block}")
#                     else:
#                         # should we save this to db?
#                         print(f"failed to process block - {current_block}")
#             else:
#                 print(f"block_data is None")
#                 # save error to db

#             # upsert block data to db
#             # chain_id, current_height, json.dump(res)

#         except Exception as e:
#             print(
#                 f"live - Failed to get a block from {chain.apis[chain.current_api_index]} - {repr(e)}"
#             )
#             chain.current_api_index = (chain.current_api_index + 1) % len(chain.apis)
#     print("backfill complete")

async def backfill_block(height, chain, pool, session, sem):
    if not (await process_block(chain=chain, height=height, pool=pool, session=session, sem=sem)):
        # log error
        print(f"failed to backfill block - {height}")
    return height


async def backfill_blocks(
    chain: CosmosChain, pool: asyncpg.pool, session: aiohttp.ClientSession,
    sem: asyncio.Semaphore
):
    tasks = []
    async for height in get_missing_blocks(pool, session, sem, chain):
        tasks.append(asyncio.ensure_future(backfill_block(height, chain, pool, session, sem)))
        if len(tasks) >= 100:
            heights = await asyncio.gather(*tasks)
            print(f"blocks inserted: {min(heights)} - {max(heights)}")
            tasks = []

async def process_txs(min_height, max_height, chain, pool, session, sem):
    txs_data = await chain.get_batch_txs(session, sem, min_height, max_height)
    if txs_data is None:
        print(f"txs_data is None")
        return False
    try:
        txs = txs_data['tx_responses']
        txs_by_height = defaultdict(list)
        print(len(txs))
        [txs_by_height[int(tx['height'])].append(tx) for tx in txs]
        max_queried_height = max(txs_by_height.keys())
        every_height = {height: txs_by_height[height] if height in txs_by_height else [] for height in range(min_height, max_queried_height + 1)}
        await upsert_raw_txs(pool, every_height, 'secret-4')
        return max_queried_height
    except Exception as e:
        print(f"upsert_txs error {repr(e)} - {min_height} - {max_height}")
        traceback.print_exc()

async def backfill_tx_range(min_height: int, max_height: int, chain: CosmosChain, pool: asyncpg.pool, session: aiohttp.ClientSession, sem: asyncio.Semaphore):
    max_queried_height = await process_txs(min_height, max_height, chain, pool, session, sem)
    if max_queried_height is None:
        # log error
        print(f"failed to backfill txs from {min_height} to {max_height}")
        return min_height
    else:
        return max_queried_height

async def backfill_txs(
    chain: CosmosChain, pool: asyncpg.pool, session: aiohttp.ClientSession,
    sem: asyncio.Semaphore
):
    tasks = []
    tx_step = 1000
    # todo: implement proper backfilling of txs like in blocks
    while True:
        print("getting block txs")
        block_data = await chain.get_block(session, sem)
        current_height = int(block_data["block"]["header"]["height"])
        chain_id = block_data["block"]["header"]["chain_id"]
        async with pool.acquire() as conn:
            max_height_db = await conn.fetchrow(
                "SELECT MAX(height) FROM txs"
            )
            min_height = max_height_db[0] if max_height_db[0] is not None else chain.min_block_height
        print(f"{min_height}")
        next_height = min_height
        while next_height < current_height:
            # print(f"backfilling txs from {height} to {height + tx_step}")
            next_height = await backfill_tx_range(next_height, next_height + tx_step, chain, pool, session, sem)
            # if len(tasks) >= 100:
            #     await asyncio.gather(*tasks)
            #     tasks = []

msg_cols, log_cols = None, None

async def main():
    print(os.getenv("APIS").split(","))
    print(os.getenv("TXS_ENDPOINT"))

    chain = CosmosChain(
        # chain_isd=os.getenv("CHAIN_ID"),
        min_block_height=int(os.getenv("MIN_BLOCK_HEIGHT")),
        blocks_endpoint=os.getenv("BLOCKS_ENDPOINT"),
        txs_endpoint=os.getenv("TXS_ENDPOINT"),
        txs_batch_endpoint=os.getenv("TXS_BATCH_ENDPOINT"),
        apis=os.getenv("APIS").split(","),
    )

    async with asyncpg.create_pool(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
    ) as pool:
        # drop tables for testing purposes
        await drop_tables(pool)

        await create_tables(pool)

        async def handle_tx_notifications(
            notification: asyncpg_listen.NotificationOrTimeout,
        ) -> None:
            global msg_cols, log_cols
            if isinstance(notification, asyncpg_listen.Timeout):
                return
            
            if msg_cols is None or log_cols is None:
                print("cols are none")
                msg_cols = set(await get_table_cols(pool, "messages"))
                log_cols = set(await get_table_cols(pool, "log"))

            payload = notification.payload
            print(f"New tx: {payload}")
            txhash, chain_id = payload.split(" ")
            # print(f"New tx: {txhash} - {chain_id}")
            async with pool.acquire() as conn:
                data = await conn.fetchrow(
                    "SELECT raw_log, tx FROM txs WHERE chain_id = $1 AND txhash = $2",
                    chain_id,
                    txhash,
                )
            raw_logs, raw_tx = data
            logs: List[Log]
            # print(raw_logs)
            # messages = json.loads(raw_tx)["body"]["messages"]
            # msgs, cur_msg_cols = parse_messages(messages, txhash)
            # new_msg_cols = cur_msg_cols.difference(msg_cols)
            # msg_cols = msg_cols.union(new_msg_cols)
            
            logs = parse_logs(raw_logs, txhash)
            cur_log_cols = set()
            for log in logs:
                cur_log_cols = cur_log_cols.union(set(log.get_cols()))
            # print(f"{cur_log_cols}")
            await add_current_log_columns(pool, cur_log_cols)
            

            # if len(new_msg_cols) > 0:
            #     print(f"txhash: {txhash} new_msg_cols: {len(new_msg_cols)}")
            #     await add_columns(pool, "messages", list(new_msg_cols))

            # if not await insert_dict(pool, "messages", msgs):
            #     # log error
            #     pass
            
            await add_logs(pool, logs)
            # if not await insert_dict(pool, "logs", logs):
                ## log error
                # pass
            
            print(f"updated messages for {txhash}")
    
        listener = asyncpg_listen.NotificationListener(
            asyncpg_listen.connect_func(        
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=os.getenv("POSTGRES_DB")
            )
        )
        
        sem = asyncio.Semaphore(1000)
    
        async with aiohttp.ClientSession() as session:
            block_data = await chain.get_block(session, sem)
            chain_id = block_data["block"]["header"]["chain_id"]
            print(f"chain_id: {chain_id} {block_data['block']['header']['height']}")

            res = await asyncio.gather(
                listener.run(
                    {"txs_to_logs": handle_tx_notifications},
                    policy=asyncpg_listen.ListenPolicy.ALL,
                ),                
                # backfill_blocks(chain, pool, session, sem),
                backfill_txs(chain, pool, session, sem)
                # get_live_chain_data(chain, pool, session, sem),
            )
            for i in res:
                print(i)

if __name__ == "__main__":
    asyncio.run(main())
