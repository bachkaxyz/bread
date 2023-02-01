import asyncio
import uuid


async def consume(item):
    print(f"Consuming {item}")
    await asyncio.sleep(1)
    print(f"{item} consumed")


async def hello(n):
    print("Hello")
    await asyncio.sleep(2)
    print(f"Finishing hello {n}")


async def check_events(interval=1):
    while True:
        item = uuid.uuid4().hex
        print(f"Producing item")
        asyncio.create_task(consume(item))
        print("Tasks count: ", len(asyncio.all_tasks()))
        await asyncio.sleep(interval)


async def main():
    await check_events()


if __name__ == "__main__":
    asyncio.run(main())
