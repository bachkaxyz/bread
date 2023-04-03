from ast import Tuple
import asyncio, aiohttp, json, asyncpg, os
import sys
import time
import logging
from typing import Any, Awaitable, Callable, Coroutine, List
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
    get_tx_raw_log_and_tx,
    upsert_raw_blocks,
    upsert_raw_txs,
    Database,
)
from indexer.exceptions import ChainDataIsNoneError, ProcessChainDataError
from indexer.parser import Log, parse_logs
import asyncpg_listen
from indexer.process import process_block, process_tx

load_dotenv()

batch_size = int(os.getenv("BATCH_SIZE", 20))

msg_cols, log_cols = None, None


async def get_live_chain_data(
    chain: CosmosChain,
    db: Database,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
):
    last_block = 0
    while True:
        try:
            block_data = await chain.get_block(session, sem)
            if block_data is None:
                raise ChainDataIsNoneError("block data is None")

            current_block = int(block_data["block"]["header"]["height"])
            # print(f"current block - {current_block}")
            if last_block >= current_block:
                await asyncio.sleep(chain.time_between_blocks)
            else:
                last_block = current_block

                is_block_processed = await process_block(
                    chain=chain,
                    height=current_block,
                    db=db,
                    session=session,
                    sem=sem,
                    block_data=block_data,
                )

                is_tx_processed = await process_tx(
                    current_block, chain, db, session, sem
                )

                if not is_block_processed or not is_tx_processed:
                    raise ProcessChainDataError(
                        f"block or tx not processed {is_block_processed=} {is_tx_processed=}"
                    )

        except ProcessChainDataError as e:
            print(f"live - failed to process block - {last_block} - {repr(e)}")
        except ChainDataIsNoneError as e:
            print(f"live - chain data is none - {last_block} - {repr(e)}")


async def backfill_data(
    get_missing_data: Callable[[Database, CosmosChain], Awaitable[List[tuple]]],
    process_missing_data: Callable[
        [int, CosmosChain, Database, aiohttp.ClientSession, asyncio.Semaphore],
        Coroutine[Any, Any, bool],
    ],
    chain: CosmosChain,
    db: Database,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    log_name: str = "",
):
    tasks = []
    start_time = time.time()
    while True:
        missing_txs_to_query = await get_missing_data(db, chain)
        # print(f"missing {log_name}: {missing_txs_to_query}")
        for min_height_in_db, max_height_in_db in missing_txs_to_query:
            # print(f"processing {log_name} {min_height_in_db} - {max_height_in_db}")
            tasks = []
            for new_min in range(min_height_in_db, max_height_in_db, batch_size):
                # print(
                #     f"processing subsection {log_name} {new_min} - {new_min + batch_size}"
                # )
                tasks = [
                    process_missing_data(h, chain, db, session, sem)
                    for h in range(new_min, new_min + batch_size)
                ]
                await asyncio.gather(*tasks)
                # print(
                #     f"processed subsection {log_name} {new_min} - {new_min + batch_size}"
                # )
                # with open(f"indexer/api_hit_miss_log.txt", "w") as f:
                #     f.write(
                #         f"start time: {start_time} current time: {time.time()} elapsed: {time.time() - start_time}\n"
                #     )
                #     for i in range(len(chain.apis)):
                #         f.write(
                #             f"{chain.apis[i]} - hit: {chain.apis_hit[i]} miss: {chain.apis_miss[i]}\n"
                #         )
            # print(f"processed {log_name} {min_height_in_db} - {max_height_in_db}")

        await asyncio.sleep(chain.time_between_blocks)


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

        schema = os.getenv("INDEXER_SCHEMA", "public")

        db = Database(pool=pool, schema=schema)

        # drop tables for testing purposes
        if os.getenv("DROP_TABLES_ON_STARTUP", False):
            await drop_tables(db)

        await create_tables(db)

        async def handle_tx_notifications(
            notification: asyncpg_listen.NotificationOrTimeout,
        ) -> None:
            if isinstance(notification, asyncpg_listen.Timeout):
                return

            payload = notification.payload

            if payload is None:
                return
            # print(f"New tx: {payload}")
            txhash, chain_id = payload.split(" ")

            raw_logs, raw_tx = await get_tx_raw_log_and_tx(db, chain_id, txhash)

            logs = parse_logs(raw_logs, txhash)
            cur_log_cols = set()
            for log in logs:
                cur_log_cols = cur_log_cols.union(log.get_cols())

            await add_current_log_columns(db, cur_log_cols)
            await add_logs(db, logs)

            # print(f"updated messages for {txhash}")

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
            apis = os.getenv("APIS").split(",") + [api["address"] for api in raw_apis]

            chain_apis = {api: {"hit": 0, "miss": 0} for api in apis}

            chain = CosmosChain(
                chain_id=raw_chain["chain_id"],
                min_block_height=int(os.getenv("MIN_BLOCK_HEIGHT")),
                blocks_endpoint=os.getenv("BLOCKS_ENDPOINT"),
                txs_endpoint=os.getenv("TXS_ENDPOINT"),
                txs_batch_endpoint=os.getenv("TXS_BATCH_ENDPOINT"),
                apis=chain_apis,
                time_between_blocks=int(os.getenv("TIME_BETWEEN_BLOCKS", 1)),
            )
            # print(f"chain: {chain.chain_id} min height: {chain.min_block_height}")
            tasks = [
                listener.run(
                    {"txs_to_logs": handle_tx_notifications},
                    policy=asyncpg_listen.ListenPolicy.ALL,
                ),
                backfill_data(
                    get_missing_data=get_missing_blocks,
                    process_missing_data=process_block,
                    chain=chain,
                    db=db,
                    session=session,
                    sem=sem,
                    log_name="blocks",
                ),
                backfill_data(
                    get_missing_data=get_missing_txs,
                    process_missing_data=process_tx,
                    chain=chain,
                    db=db,
                    session=session,
                    sem=sem,
                    log_name="txs",
                ),
                get_live_chain_data(chain, db, session, sem),
            ]

            await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
