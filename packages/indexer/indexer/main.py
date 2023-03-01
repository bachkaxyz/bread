import asyncio, aiohttp, json, asyncpg, os
import time
import logging
from collections import defaultdict
import traceback
from typing import List
from dotenv import load_dotenv
from indexer.chain import CosmosChain
from indexer.db import (
    add_current_log_columns,
    add_logs,
    create_tables,
    drop_tables,
    get_missing_blocks,
    get_missing_txs,
    get_table_cols,
    upsert_raw_blocks,
    upsert_raw_txs,
)
from indexer.parser import Log, parse_logs, parse_messages
import asyncpg_listen

load_dotenv()

batch_size = int(os.getenv("BATCH_SIZE", 100))


async def process_block(
    chain: CosmosChain,
    height: int,
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
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


async def get_live_chain_data(
    chain: CosmosChain,
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
):
    last_block = 0
    while True:
        try:
            block_data = await chain.get_block(session, sem)
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

                        txs_data = txs_data["tx_responses"]
                        await upsert_raw_txs(
                            pool, {current_block: txs_data}, chain.chain_id
                        )
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
    if not (
        await process_block(
            chain=chain, height=height, pool=pool, session=session, sem=sem
        )
    ):
        # log error
        print(f"failed to backfill block - {height}")
    return height


async def backfill_blocks(
    chain: CosmosChain,
    pool: asyncpg.pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
):
    tasks = []
    batch_size = 100
    async for height in get_missing_blocks(pool, session, sem, chain):
        tasks.append(
            asyncio.ensure_future(backfill_block(height, chain, pool, session, sem))
        )
        if len(tasks) >= batch_size:
            heights = await asyncio.gather(*tasks)
            print(f"blocks inserted: {min(heights)} - {max(heights)}")
            tasks = []


async def process_tx(
    height: int,
    chain: CosmosChain,
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
):
    print(f"processing tx {height}")
    txs_data = await chain.get_block_txs(session, sem, height)
    print(txs_data)
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


async def check_missing_txs(
    chain: CosmosChain,
    pool: asyncpg.pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
):
    tasks = []
    section_size = 1000
    start_time = time.time()
    while True:
        # print("getting block txs")
        missing_txs_to_query = await get_missing_txs(pool, chain)
        print(f"missing txs: {missing_txs_to_query}")
        for min_height_in_db, max_height_in_db in missing_txs_to_query:
            tasks = []
            for new_min in range(min_height_in_db, max_height_in_db, batch_size):
                tasks = [
                    process_tx(h, chain, pool, session, sem)
                    for h in range(new_min, new_min + batch_size)
                ]
                await asyncio.gather(*tasks)
                with open(f"indexer/api_hit_miss_log.txt", "w") as f:
                    f.write(
                        f"start time: {start_time} current time: {time.time()} elapsed: {time.time() - start_time}\n"
                    )
                    for i in range(len(chain.apis)):
                        f.write(
                            f"{chain.apis[i]} - hit: {chain.apis_hit[i]} miss: {chain.apis_miss[i]}\n"
                        )
        await asyncio.sleep(1)


async def backfill_txs():
    pass


msg_cols, log_cols = None, None


async def on_request_start(session, context, params):
    logging.getLogger("aiohttp.client").debug(f"Starting request <{params}>")


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
            async with pool.acquire() as conn:
                data = await conn.fetchrow(
                    "SELECT raw_log, tx FROM txs WHERE chain_id = $1 AND txhash = $2",
                    chain_id,
                    txhash,
                )
            raw_logs, raw_tx = data
            logs: List[Log]

            logs = parse_logs(raw_logs, txhash)
            cur_log_cols = set()
            for log in logs:
                cur_log_cols = cur_log_cols.union(set(log.get_cols()))

            await add_current_log_columns(pool, cur_log_cols)
            await add_logs(pool, logs)

            print(f"updated messages for {txhash}")

        listener = asyncpg_listen.NotificationListener(
            asyncpg_listen.connect_func(
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=os.getenv("POSTGRES_DB"),
            )
        )

        sem = asyncio.Semaphore(100)  # 100 concurrent request

        async with aiohttp.ClientSession(trace_configs=[]) as session:
            chain_name = os.getenv("CHAIN_NAME")
            if chain_name:
                raw_chain = await session.get(
                    f"https://raw.githubusercontent.com/cosmos/chain-registry/master/{chain_name}/chain.json"
                )
            raw_chain = json.loads(await raw_chain.read())
            raw_apis = raw_chain["apis"]["rest"]
            apis = [api["address"] for api in raw_apis] + os.getenv("APIS").split(",")

            chain = CosmosChain(
                chain_id=raw_chain["chain_id"],
                min_block_height=int(os.getenv("MIN_BLOCK_HEIGHT")),
                blocks_endpoint=os.getenv("BLOCKS_ENDPOINT"),
                txs_endpoint=os.getenv("TXS_ENDPOINT"),
                txs_batch_endpoint=os.getenv("TXS_BATCH_ENDPOINT"),
                apis=apis,
                apis_hit=[0 for i in range(len(apis))],
                apis_miss=[0 for i in range(len(apis))],
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

            await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
