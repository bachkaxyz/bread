import asyncio, aiohttp, json, asyncpg, os
import traceback
from typing import List
from dotenv import load_dotenv
from indexer.chain import CosmosChain
from indexer.db import (
    add_columns,
    create_tables,
    drop_tables,
    get_missing_blocks,
    get_table_cols,
    upsert_block,
)
from indexer.parser import parse_logs, parse_messages
import asyncpg_listen

load_dotenv()


async def process_block(
    chain: CosmosChain,
    height: int,
    pool,
    session: aiohttp.ClientSession,
    block_data: dict = None,
) -> bool:
    if block_data is None:
        # this is because block data might be passed in from the live chain data (so removing a rerequest)
        block_data = await chain.get_block(session, height)

    # need to come back to this
    txs_data = await chain.get_txs(session, height)
    if txs_data is None or block_data is None:
        print(f"{'tx_data' if txs_data is None else 'block_data'} is None")
        return False
    try:
        await upsert_block(pool, block_data, txs_data)
        return True
    except Exception as e:
        print(f"upsert_block error {repr(e)} - {height}")
        traceback.print_exc()
        return False


async def get_live_chain_data(
    chain: CosmosChain,
    pool,
    session: aiohttp.ClientSession,
):
    last_block = 0
    while True:
        try:
            block_data = await chain.get_block(
                session,
            )
            if block_data is not None:
                current_block = int(block_data["block"]["header"]["height"])
                if last_block >= current_block:
                    # print(f"{chain_id} - block {current_block} already indexed")
                    await asyncio.sleep(1)
                else:
                    last_block = current_block
                    if await process_block(
                        chain=chain,
                        height=current_block,
                        pool=pool,
                        session=session,
                        block_data=block_data,
                    ):
                        print(f"processed live block - {current_block}")
                    else:
                        # should we save this to db?
                        print(f"failed to process block - {current_block}")
            else:
                print(f"block_data is None")
                # save error to db

            # upsert block data to db
            # chain_id, current_height, json.dump(res)

        except Exception as e:
            print(
                f"live - Failed to get a block from {chain.apis[chain.current_api_index]} - {repr(e)}"
            )
            chain.current_api_index = (chain.current_api_index + 1) % len(chain.apis)
    print("backfill complete")


async def backfill_data(
    chain: CosmosChain, pool: asyncpg.pool, session: aiohttp.ClientSession
):
    async for height in get_missing_blocks(pool, session, chain):
        if await process_block(chain=chain, height=height, pool=pool, session=session):
            print(f"backfilled block - {height}")
        else:
            print(f"failed to backfill block - {height}")
    print("backfill complete")


msg_cols, log_cols = None, None

async def main():
    print(os.getenv("APIS").split(","))
    print(os.getenv("TXS_ENDPOINT"))

    chain = CosmosChain(
        # chain_isd=os.getenv("CHAIN_ID"),
        min_block_height=int(os.getenv("MIN_BLOCK_HEIGHT")),
        blocks_endpoint=os.getenv("BLOCKS_ENDPOINT"),
        txs_endpoint=os.getenv("TXS_ENDPOINT"),
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

        async def handle_notifications(
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
                logs, tx = data
                messages = json.loads(tx)["body"]["messages"]
                msgs, cur_msg_cols = parse_messages(messages, txhash)
                logs, cur_log_cols = parse_logs(logs, txhash)

                # print(f"{msg_cols=} {cur_msg_cols=} {log_cols=} {cur_log_cols=}")
                new_msg_cols = cur_msg_cols.difference(msg_cols)
                new_log_cols = cur_log_cols.difference(log_cols)
                msg_cols = msg_cols.union(new_msg_cols)
                log_cols = log_cols.union(new_log_cols)

                if len(new_msg_cols) > 0:
                    print(f"txhash: {txhash} new_msg_cols: {len(new_msg_cols)}")
                    await add_columns(pool, "messages", list(new_msg_cols))

                if len(new_log_cols) > 0:
                    print(f"txhash: {txhash} new_log_cols: {len(new_log_cols)}")
                    await add_columns(pool, "logs", list(new_log_cols))

                async with conn.transaction():
                    for msg in msgs:
                        keys = ",".join(msg.keys())
                        values = "'" + "','".join(str(i) for i in msg.values()) + "'"
                        # print(f"INSERT INTO messages ({keys}) VALUES ({values})")
                        try:
                            await conn.execute(
                                f"""
                                INSERT INTO messages ({keys})
                                VALUES ({values})
                                """
                            )
                        except asyncpg.PostgresSyntaxError as e:
                            print("messages", txhash, traceback.format_exc())
                    for log in logs:
                        keys = ",".join(log.keys())
                        values = "'" + "','".join(str(i) for i in log.values()) + "'"
                        # print(f"INSERT INTO messages ({keys}) VALUES ({values})")
                        try:
                            await conn.execute(
                                f"""
                                INSERT INTO logs ({keys})
                                VALUES ({values})
                                """
                            )
                        except asyncpg.PostgresSyntaxError as e:
                            print("logs", txhash, traceback.format_exc())

                print(f"updated messages for {txhash}")
                # print("updated", (await get_table_cols(pool, "messages")))
                # cur_columns_raw = await conn.fetch(
                #     "SELECT column_name FROM information_schema.columns WHERE table_name = 'messages'"
                # )
                # cur_columns = [col["column_name"] for col in cur_columns_raw]

        listener = asyncpg_listen.NotificationListener(
            asyncpg_listen.connect_func(        
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=os.getenv("POSTGRES_DB")
            )
        )
    
        async with aiohttp.ClientSession() as session:
            block_data = await chain.get_block(session)
            chain_id = block_data["block"]["header"]["chain_id"]
            print(f"chain_id: {chain_id}")

            await asyncio.gather(
                asyncio.create_task(
                    listener.run(
                        {"txs_to_messages_logs": handle_notifications},
                        policy=asyncpg_listen.ListenPolicy.ALL,
                    )
                ),
                asyncio.create_task(backfill_data(chain, pool, session)),
                asyncio.create_task(get_live_chain_data(chain, pool, session)),
                return_exceptions=True,
            )

if __name__ == "__main__":
    asyncio.run(main())
