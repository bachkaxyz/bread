import asyncio
import io
import json
import os
import time
import traceback
from typing import Any, Coroutine, List
from aiohttp import ClientSession
from asyncpg import Connection, Pool
from indexer.chain import CosmosChain
from indexer.exceptions import ChainDataIsNoneError
from indexer.parser import Raw
import logging
from google.cloud.storage import Blob, Client, Bucket

# timing
blob_upload_times = []
upsert_times = []


async def missing_blocks_cursor(conn: Connection, chain: CosmosChain):
    """
    Generator that yields missing blocks from the database

    limit of 100 is to prevent the generator from yielding too many results to keep live data more up to date
    """
    async for record in conn.cursor(
        """
        select height, difference_per_block from (
            select height, COALESCE(height - LAG(height) over (order by height), -1) as difference_per_block, chain_id
            from raw
            where chain_id = $1
        ) as dif
        where difference_per_block <> 1
        order by height desc
        limit 100
        """,
        chain.chain_id,
    ):
        yield record


async def wrong_tx_count_cursor(conn: Connection, chain: CosmosChain):
    """
    Generator that yields blocks with wrong tx counts from the database
    """
    async for record in conn.cursor(
        """
         select height, block_tx_count, chain_id
        from raw
        where (tx_tx_count <> block_tx_count or tx_tx_count is null or block_tx_count is null) and chain_id = $1
        """,
        chain.chain_id,
    ):
        yield record


async def drop_tables(conn: Connection, schema: str):
    await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    await conn.execute(f"CREATE SCHEMA {schema}")


async def create_tables(conn: Connection, schema: str):
    # we use the path of your current directory to get the absolute path of the sql files depending on where the script is run from
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur_dir = os.path.dirname(__file__)
    file_path = os.path.join(cur_dir, "sql/create_tables.sql")
    with open(file_path, "r") as f:
        # our schema in .sql files is defined as $schema so we replace it with the actual schema name
        await conn.execute(f.read().replace("$schema", schema))

    file_path = os.path.join(cur_dir, "sql/log_triggers.sql")
    with open(file_path, "r") as f:
        await conn.execute(f.read().replace("$schema", schema))


async def upsert_data(pool: Pool, raw: Raw, bucket: Bucket, chain: CosmosChain):
    # loop = asyncio.get_event_loop()
    tasks: List[Coroutine[Any, Any, bool]] = [upsert_data_to_db(pool, raw)]
    if raw.height and raw.raw_block:
        # tasks.append(
        await run_insert_into_gcs(
            bucket,
            f"{chain.chain_registry_name}/{raw.chain_id}/blocks/{raw.height}.json",
            raw.raw_block,
        )
        # )
    if raw.height and raw.raw_tx:
        tasks.append(
            run_insert_into_gcs(
                bucket,
                f"{chain.chain_registry_name}/{raw.chain_id}/txs/{raw.height}.json",
                raw.raw_tx,
            )
        )
    results = await asyncio.gather(*tasks)
    return all(results)


async def upsert_data_to_db(pool: Pool, raw: Raw) -> bool:
    global upsert_times
    """Upsert a blocks data into the database

    Args:
        pool (Pool): The database connection pool
        raw (Raw): The raw data to upsert

    Returns:
        bool: True if the data was upserted, False if the data was not upserted
    """
    upsert_start_time = time.time()
    logger = logging.getLogger("indexer")
    # we check if the data is valid before upserting it
    if raw.height is not None and raw.chain_id is not None:
        async with pool.acquire() as conn:
            async with pool.acquire() as conn2:
                conn: Connection
                conn2: Connection
                # we do all the upserts in a transaction so that if one fails, all of them fail
                await insert_raw(conn, raw)

                tasks = []
                # we are checking if the block is not None because we might only have the tx data and not the block data
                if raw.block is not None:
                    tasks.append(insert_block(conn2, raw))
                if raw.txs:
                    tasks.append(insert_many_txs(conn, raw))
                await asyncio.gather(*tasks)

                await asyncio.gather(
                    insert_many_log_columns(conn2, raw), insert_many_logs(conn, raw)
                )

            logger.info(f"{raw.height=} inserted")
            upsert_end_time = time.time()
            upsert_times.append(upsert_end_time - upsert_start_time)
            return True

    else:
        logger.info(f"{raw.height} {raw.chain_id} is None")
        return False


async def insert_raw(conn: Connection, raw: Raw):
    await conn.execute(
        f"""
                        INSERT INTO raw(chain_id, height, block_tx_count, tx_tx_count)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT ON CONSTRAINT raw_pkey
                        DO UPDATE SET tx_tx_count = EXCLUDED.tx_tx_count;
                        """,
        *raw.get_raw_db_params(),
    )


async def run_insert_into_gcs(
    bucket: Bucket, blob_url: str, data: dict | list, max_retries: int = 5
) -> bool:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, insert_json_into_gcs, bucket.blob(blob_url), data, max_retries
    )


def insert_json_into_gcs(blob: Blob, data: dict | list, max_retries: int = 5) -> bool:
    global blob_upload_times
    start_time = time.time()
    retries = 0
    while retries < max_retries:
        try:
            blob.upload_from_string(json.dumps(data))
            finish_time = time.time()
            blob_upload_times.append(finish_time - start_time)
            return True
        except Exception as e:
            logger = logging.getLogger("indexer")
            logger.error(
                f"blob upload failed, retrying in 1 second {traceback.format_exc()}"
            )
            time.sleep(1)
        retries += 1
    return False


async def insert_block(conn: Connection, raw: Raw):
    if raw.block:
        await conn.execute(
            """
                    INSERT INTO blocks(chain_id, height, time, block_hash, proposer_address)
                    VALUES ($1, $2, $3, $4, $5);
                    """,
            *raw.block.get_db_params(),
        )
    else:
        raise ChainDataIsNoneError("Block does not exist")


async def insert_many_txs(conn: Connection, raw: Raw):
    await conn.executemany(
        """
                INSERT INTO txs(txhash, chain_id, height, code, data, info, logs, events, raw_log, gas_used, gas_wanted, codespace, timestamp)
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                )
                """,
        raw.get_txs_db_params(),
    )


async def insert_many_log_columns(conn: Connection, raw: Raw):
    await conn.executemany(
        f"""
                INSERT INTO log_columns (event, attribute)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
        raw.get_log_columns_db_params(),
    )


async def insert_many_logs(conn: Connection, raw: Raw):
    await conn.executemany(
        f"""
                INSERT INTO logs (txhash, msg_index, parsed, failed, failed_msg)
                VALUES (
                    $1, $2, $3, $4, $5
                )
                """,
        raw.get_logs_db_params(),
    )


async def get_max_height(conn: Connection, chain: CosmosChain) -> int:
    """Get the max height of the chain from the database"""
    res = await conn.fetchval(
        """
        select max(height)
        from raw
        where chain_id = $1
        """,
        chain.chain_id,
        column=0,
    )
    logger = logging.getLogger("indexer")
    logger.info(res)
    if res:
        return res
    else:
        # if max height doesn't exist
        return 0
