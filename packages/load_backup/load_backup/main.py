import asyncio
from aiohttp import ClientSession
from gcloud.aio.storage import Storage


async def main():
    async with ClientSession() as session:
        storage = Storage(session=session)
        bucket = storage.get_bucket("sn-mono-indexer")
        next_page_token = ""
        while True:
            blobs = await storage.list_objects(
                bucket.name, params={"maxResults": "1000", "pageToken": next_page_token}
            )
            next_page_token = blobs["nextPageToken"]
            print(len(blobs["items"]))


if __name__ == "__main__":
    asyncio.run(main())
