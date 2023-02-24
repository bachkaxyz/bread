import asyncio, aiohttp, json, asyncpg, os
import logging
from collections import defaultdict
import traceback
from typing import List
from dotenv import load_dotenv
from indexer.chain import CosmosChain
from indexer.db import (
    add_columns,
    add_current_log_columns,
    add_logs,
    check_backfill,
    create_tables,
    drop_tables,
    get_missing_blocks,
    get_missing_txs,
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


async def get_live_chain_data(
    chain: CosmosChain,
    pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore
):
    last_block = 0
    while True:
        try:
            block_data = await chain.get_block(
                session,
                sem
            )
            if block_data is not None:
                current_block = int(block_data["block"]["header"]["height"])
                print(f"current block - {current_block}")
                if last_block >= current_block:
                    await asyncio.sleep(1)
                else:
                    last_block = current_block
                    if await process_block(
                        chain=chain,
                        height=current_block,
                        pool=pool,
                        session=session,
                        sem=sem,
                        block_data=block_data,
                    ):
                        print(f"processed live block - {current_block}")
                    else:
                        # should we save this to db?
                        print(f"failed to process block - {current_block}")
                    
                    txs_data = await chain.get_block_txs(session, sem, current_block)
                    
                
                    if txs_data:
                        
                        txs_data = txs_data['tx_responses']
                        await upsert_raw_txs(pool, {current_block: txs_data}, chain.chain_id)
                        print(f"processed live txs {current_block}")
                    else:
                        # should we save this to db?
                        print(f"failed to get txs for block - {current_block}")
            else:
                print(f"block_data is None")
                # save error to db


        except Exception as e:
            print(
                f"live - Failed to get a block from {chain.apis[chain.current_api_index]} - {repr(e)}"
            )
            chain.current_api_index = (chain.current_api_index + 1) % len(chain.apis)
            
    print("live complete")

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

async def process_batch_txs(min_height: int, max_height: int, chain: CosmosChain, pool: asyncpg.Pool, session: aiohttp.ClientSession, sem: asyncio.Semaphore): 
    """Insert the section of transactions between `min_height` and `max_height` into the database."""
    current_max = min_height
    print(f"processing txs {min_height} - {max_height}")
    while current_max < max_height:
        txs_data = await chain.get_batch_txs(session, sem, min_height, max_height)
        if txs_data is None:
            print(f"txs_data is None")
        try:
            txs = txs_data['tx_responses']
            txs_by_height = defaultdict(list)

            [txs_by_height[int(tx['height'])].append(tx) for tx in txs]
            max_queried_height = max(txs_by_height.keys())
            every_height = {height: txs_by_height[height] if height in txs_by_height else [] for height in range(min_height, max_queried_height + 1)}
            # todo: chain id
            await upsert_raw_txs(pool, every_height, chain.chain_id)
            print(f"upserted txs {min_height} - {max_queried_height}")
            current_max = max_queried_height
        except Exception as e:
            print(f"upsert_txs error {repr(e)} - {min_height} - {max_height}")
            print("trying again...")
            traceback.print_exc()
            
async def process_tx(height: int, chain: CosmosChain, pool: asyncpg.Pool, session: aiohttp.ClientSession, sem: asyncio.Semaphore): 
    print(f"processing tx {height}")
    txs_data = await chain.get_block_txs(session, sem, height)
    try:                
        if txs_data is None:
            print(f"txs_data is None")
            raise Exception("txs_data is None")
        else:
            txs_data = txs_data['tx_responses']
            await upsert_raw_txs(pool, {height: txs_data}, chain.chain_id)
                            
            # txs = txs_data['tx_responses']
            # txs_by_height = defaultdict(list)

            # [txs_by_height[int(tx['height'])].append(tx) for tx in txs]
            # max_queried_height = max(txs_by_height.keys())
            # every_height = {height: txs_by_height[height] if height in txs_by_height else [] for height in range(min_height, max_queried_height + 1)}

            # await upsert_raw_txs(pool, , chain.chain_id)
            print(f"upserted tx {height}")
            # current_max = max_queried_height
    except Exception as e:
        print(f"upsert_txs error {repr(e)} - {height}")
        print(f"trying again... {txs_data}")
        traceback.print_exc()


async def check_missing_txs(
    chain: CosmosChain, pool: asyncpg.pool, session: aiohttp.ClientSession,
    sem: asyncio.Semaphore
):
    tasks = []
    section_size = 1000
    batch_size = 20 # process 20 blocks at a time
    while True:
        # print("getting block txs")
        missing_txs_to_query = await get_missing_txs(pool, chain)
        print(f"# of missing txs: {len(missing_txs_to_query)}")
        for min_height_in_db, max_height_in_db in missing_txs_to_query:
            
            # if we are missing tons of txs, we should split it up into smaller sections
            # if max_height_in_db - min_height_in_db > section_size:
                # tasks = []
                # for new_min in range(min_height_in_db, max_height_in_db, section_size):
                #     print(f"adding task {new_min} to {new_min + section_size}")
                #     # await process_batch_txs(new_min, new_min + section_size, chain, pool, session, sem)
                #     tasks.append(asyncio.create_task(process_batch_txs(new_min, new_min + section_size, chain, pool, session, sem)))
                #     if len(tasks) >= 5:
                #         print("gather tasks")
                #         try: 
                #             await asyncio.gather(*tasks)
                #         except Exception as e:
                #             print("error in batch task gather")
                #             traceback.print_exc()
                #         tasks = []
            # else:
            #     await process_batch_txs(min_height_in_db, max_height_in_db, chain, pool, session, sem)
            tasks = []
            for new_min in range(min_height_in_db, max_height_in_db, batch_size):
                tasks = [
                    process_tx(h, chain, pool, session, sem)
                    for h in range(new_min, new_min + batch_size)
                ]
                await asyncio.gather(*tasks)
                with open(f"indexer/api_hit_miss_log.txt", "w") as f:
                    for i in range(len(chain.apis)):
                        f.write(f"{chain.apis[i]} - hit: {chain.apis_hit[i]} miss: {chain.apis_miss[i]}\n")
        await asyncio.sleep(1)
        

async def backfill_txs():
    pass

msg_cols, log_cols = None, None

async def on_request_start(session, context, params):
    logging.getLogger('aiohttp.client').debug(f'Starting request <{params}>')

async def main():

    
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
        
        sem = asyncio.Semaphore(100)
        
        logging.basicConfig(level=logging.DEBUG)
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
    
        async with aiohttp.ClientSession(trace_configs=[trace_config]) as session:

            chain = await session.get("https://raw.githubusercontent.com/cosmos/chain-registry/master/secretnetwork/chain.json")
            chain = json.loads(await chain.read())
            raw_apis = chain['apis']['rest']
            apis = [api['address'] for api in raw_apis] + os.getenv("APIS").split(",")

            chain = CosmosChain(
                chain_id='secret-4',
                min_block_height=int(os.getenv("MIN_BLOCK_HEIGHT")),
                blocks_endpoint=os.getenv("BLOCKS_ENDPOINT"),
                txs_endpoint=os.getenv("TXS_ENDPOINT"),
                txs_batch_endpoint=os.getenv("TXS_BATCH_ENDPOINT"),
                apis=apis,
                apis_hit=[0 for i in range(len(apis))],
                apis_miss=[0 for i in range(len(apis))]
            )
            print(f"chain: {chain.chain_id} min height: {chain.min_block_height}")            
            tasks = [
                listener.run(
                    {"txs_to_logs": handle_tx_notifications},
                    policy=asyncpg_listen.ListenPolicy.ALL,
                ),                
                # backfill_blocks(chain, pool, session, sem),
                check_missing_txs(chain, pool, session, sem),
                get_live_chain_data(chain, pool, session, sem),
            ]
            
            await asyncio.gather(
                *tasks
            )

if __name__ == "__main__":
    asyncio.run(main())
