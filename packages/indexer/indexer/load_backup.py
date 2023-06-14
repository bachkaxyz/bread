import asyncio
import json
from aiohttp import ClientSession
from gcloud.aio.storage import Storage, Bucket
from indexer.db import create_tables, drop_tables, upsert_data_to_db
from indexer.manager import Manager
from indexer.config import Config
from parse import Raw
import os


async def main():
    config = Config()
    await config.configure()

    prefix = (
        f"{config.chain.chain_registry_name.lower()}/{config.chain.chain_id.lower()}"
    )
    async with Manager(
        db_kwargs=config.db_kwargs, session_kwargs=config.session_kwargs
    ) as manager:
        async with ClientSession() as session:
            async with (await manager.getPool()).acquire() as conn:
                if config.DROP_TABLES_ON_STARTUP:
                    await drop_tables(conn, config.schema_name)
                if config.CREATE_TABLES_ON_STARTUP:
                    await create_tables(conn, config.schema_name)
            storage = Storage(session=session)
            bucket = storage.get_bucket("sn-mono-indexer")
            next_page_token = ""
            while True:
                blobs = await storage.list_objects(
                    bucket.name,
                    params={
                        "maxResults": "10",
                        "pageToken": next_page_token,
                        "prefix": f"{prefix}/block",
                    },
                )

                await asyncio.gather(
                    *[
                        parse_and_upsert(manager, storage, bucket, blob["name"], prefix)
                        for blob in blobs["items"]
                    ]
                )
                next_page_token = blobs["nextPageToken"]


async def parse_and_upsert(
    manager: Manager, storage: Storage, bucket: Bucket, blob_name: str, prefix: str
):
    block_data = await storage.download(bucket.name, blob_name)
    raw = Raw()
    raw.parse_block(json.loads(block_data))
    if raw.height and raw.block_tx_count and raw.block_tx_count > 0:
        height = raw.height

        # we will in theory only get here if there are txs
        try:
            tx_data = await storage.download(bucket.name, f"{prefix}/txs/{height}.json")
            txs = json.loads(tx_data)
            raw.parse_tx_responses(txs)
        except Exception as e:
            print(f"error pulling tx for {height} with {raw.block_tx_count} txs", e)

    else:
        raw.tx_responses_tx_count = 0
    return await upsert_data_to_db(manager, raw)


if __name__ == "__main__":
    asyncio.run(main())
