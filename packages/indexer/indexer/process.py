import asyncio
import contextlib
from typing import List
import aiohttp
import asyncpg
from asyncpg_listen import NotificationListener, NotificationOrTimeout, Timeout
import asyncpg_listen
from indexer import db
from indexer.chain import CosmosChain
from indexer.db import (
    Database,
    add_current_log_columns,
    add_logs,
    get_tx_raw_log_and_tx,
    upsert_raw_blocks,
    upsert_raw_txs,
)
from indexer.parser import parse_logs


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


class DbNotificationHandler:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.notifications: List[asyncpg_listen.NotificationOrTimeout] = []

    async def process_tx_notifications(
        self,
        notification: NotificationOrTimeout,
    ) -> None:
        if isinstance(notification, Timeout):
            return
        self.notifications.append(notification)
        payload = notification.payload
        # print(f"New tx: {payload}")
        txhash, chain_id = payload.split(" ")

        raw_logs, raw_tx = await get_tx_raw_log_and_tx(self.db, chain_id, txhash)

        logs = parse_logs(raw_logs, txhash)
        cur_log_cols = set()
        for log in logs:
            cur_log_cols = cur_log_cols.union(log.get_cols())

        await add_current_log_columns(self.db, cur_log_cols)
        await add_logs(self.db, logs)

        # print(f"updated messages for {txhash}")

    async def cancel_and_wait(future: asyncio.Future[None]) -> None:
        future.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await future
