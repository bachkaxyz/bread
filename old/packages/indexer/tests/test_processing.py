import asyncio
import re
from typing import List, Tuple
from indexer.chain import CosmosChain
from indexer.manager import Manager
from pytest_mock import MockerFixture, mocker
from parse import Raw
from indexer.process import process_tx, process_block
from indexer.live import live, get_data_live
from indexer.db import (
    create_tables,
    drop_tables,
    upsert_data,
    wrong_tx_count_cursor,
    missing_blocks_cursor,
)
from indexer.backfill import (
    get_data_historical,
    run_and_upsert_tasks,
    backfill_historical,
    backfill_wrong_count,
)

from parse.fixtures import *
from asyncpg import Connection, Pool
from aiohttp import ClientSession
from gcloud.aio.storage import Bucket, Storage
from deepdiff import DeepDiff
from aioresponses import aioresponses


async def test_live(
    raws: List[Raw],
    schema: str,
    chain: CosmosChain,
    storage_config: Tuple[ClientSession, Storage, Bucket],
    mocker: MockerFixture,
    manager: Manager,
):
    storage_session, storage, bucket = storage_config
    raw = raws[0]
    if raw and raw.chain_id:
        async with (await manager.getPool()).acquire() as conn:
            await drop_tables(conn, schema)
            await create_tables(conn, schema)
            assert True == await upsert_data(manager, raw, bucket, chain)

        chain.chain_id = raw.chain_id
        mocker.patch("indexer.live.get_data_live", return_value=raws[1])
        await live(manager, chain, bucket)

        async with (await manager.getPool()).acquire() as conn:
            await drop_tables(conn, schema)


async def test_get_data_live_correct(
    raws: List[Raw],
    unparsed_raw_data,
    mocker: MockerFixture,
    chain: CosmosChain,
    manager: Manager,
):
    chain.chain_id = "jackal-1"
    current_height = 0
    with aioresponses() as m:
        for i, raw in enumerate(raws):
            if raw.height:
                m.get(
                    re.compile(".*"),
                    status=200,
                    body=json.dumps(unparsed_raw_data[i]["block"]),
                )
                m.get(
                    re.compile(".*"),
                    status=200,
                    body=json.dumps({"tx_responses": unparsed_raw_data[i]["txs"]}),
                )

                raw_res_live = await get_data_live(manager, chain, current_height)
                m.get(
                    re.compile(".*"),
                    status=200,
                    body=json.dumps(unparsed_raw_data[i]["block"]),
                )
                m.get(
                    re.compile(".*"),
                    status=200,
                    body=json.dumps({"tx_responses": unparsed_raw_data[i]["txs"]}),
                )
                raw_res_backfill = await get_data_historical(
                    manager,
                    chain,
                    int(unparsed_raw_data[i]["block"]["block"]["header"]["height"]),
                )

                assert DeepDiff(raw_res_backfill, raw) == {}
                assert DeepDiff(raw_res_live, raw) == {}
                if raw:
                    current_height = raw.height


async def test_tx_parsing_errors(
    raws: List[Raw],
    unparsed_raw_data: List[dict],
    chain: CosmosChain,
    manager: Manager,
):
    chain.chain_id = "jackal-1"
    raw = raws[0]
    unparsed = unparsed_raw_data[0]
    if not raw.height:
        return

    # 5 incorrect cases:

    # tx count not correct
    current_height = 0
    with aioresponses() as m:
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(unparsed["block"]),
        )
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps({"tx_responses": unparsed_raw_data[1]["txs"]}),
        )

        raw_res = await get_data_live(manager, chain, current_height)
        assert raw_res == Raw(
            height=raw.height,
            chain_id=raw.chain_id,
            block_tx_count=raw.block_tx_count,
            tx_responses_tx_count=None,
            block=raw.block,
            raw_block=raw.raw_block,
        )

        # tx_response doesn't exist
        current_height = 0
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(unparsed["block"]),
        )
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps({}),
        )

        raw_res = await get_data_live(manager, chain, current_height)
        assert raw_res == Raw(
            height=raw.height,
            chain_id=raw.chain_id,
            block_tx_count=raw.block_tx_count,
            tx_responses_tx_count=None,
            block=raw.block,
            raw_block=raw.raw_block,
        )

        # no txs
        no_txs_unparsed = unparsed
        no_txs_unparsed["block"]["block"]["data"]["txs"] = []
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(no_txs_unparsed["block"]),
        )

        raw_res = await get_data_live(manager, chain, current_height)

        assert raw_res == Raw(
            height=raw.height,
            chain_id=raw.chain_id,
            block_tx_count=0,
            tx_responses_tx_count=0,
            block=raw.block,
            raw_block=raw.raw_block,
        )

        # block already processed
        current_height = raw.height
        m.get(
            re.compile(".*"),
            status=200,
            body=json.dumps(unparsed["block"]),
        )

        raw_res = await get_data_live(manager, chain, current_height)
        assert raw_res == None

        # block data is none
        m.get(re.compile(".*"), status=400, body=json.dumps({}))
        raw_res = await get_data_live(manager, chain, current_height)
        assert raw_res == None


async def test_backfill(
    raws: List[Raw],
    chain: CosmosChain,
    schema: str,
    storage_config: Tuple[ClientSession, Storage, Bucket],
    mocker: MockerFixture,
    manager: Manager,
):
    storage_session, storage, bucket = storage_config
    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)
        await create_tables(conn, schema)

    await asyncio.gather(*[upsert_data(manager, raw, bucket, chain) for raw in raws])

    min_height = min([raw.height if raw.height else float("inf") for raw in raws])
    mocker.patch(
        "indexer.chain.CosmosChain.get_lowest_height",
        return_value=min_height,
    )
    await backfill_historical(manager, chain, bucket)
    await backfill_wrong_count(manager, chain, bucket)

    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)

    mocker.resetall()


async def test_backfill_run_and_upsert_batch(
    chain: CosmosChain,
    schema: str,
    mocker,
    raws: List[Raw],
    unparsed_raw_data: List[dict],
    storage_config: Tuple[ClientSession, Storage, Bucket],
    manager: Manager,
):
    storage_session, storage, bucket = storage_config
    async with (await manager.getPool()).acquire() as conn:
        await create_tables(conn, schema)
    current_height = 0
    chain.chain_id = "jackal-1"
    raw = raws[0]
    unparsed = unparsed_raw_data[0]
    if not raw.height:
        return
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=unparsed["block"],
    )
    mocker.patch(
        "indexer.chain.CosmosChain.get_block_txs",
        return_value={"tx_responses": unparsed_raw_data[1]["txs"]},
    )
    raw_res = await get_data_live(manager, chain, current_height)
    if raw_res:
        await upsert_data(manager, raw_res, bucket, chain)

    async with (await manager.getPool()).acquire() as conn:
        async with conn.transaction():
            async for record in wrong_tx_count_cursor(conn, chain):
                assert record["height"] == 2316140
                raw = Raw(
                    height=record["height"],
                    block_tx_count=record["block_tx_count"],
                    chain_id=record["chain_id"],
                )
                mocker.patch(
                    "indexer.chain.CosmosChain.get_block_txs",
                    return_value={"tx_responses": unparsed["txs"]},
                )
                tasks: List[asyncio.Task[Raw | None]] = [
                    asyncio.create_task(process_tx(raw, manager, chain))
                ]
                await run_and_upsert_tasks(tasks, manager, bucket, chain)

    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)

    mocker.resetall()


async def test_wrong_tx_count_cursor_under_than_20(
    chain: CosmosChain,
    schema: str,
    mocker,
    raws: List[Raw],
    unparsed_raw_data: List[dict],
    storage_config: Tuple[ClientSession, Storage, Bucket],
    manager: Manager,
):
    storage_session, storage, bucket = storage_config
    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)
        await create_tables(conn, schema)

    current_height = 0
    chain.chain_id = "jackal-1"
    raw = raws[0]
    unparsed = unparsed_raw_data[0]
    if not raw.height:
        return
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=unparsed["block"],
    )
    mocker.patch(
        "indexer.chain.CosmosChain.get_block_txs",
        return_value={"tx_responses": unparsed_raw_data[1]["txs"]},
    )
    raw_res = await get_data_live(manager, chain, current_height)
    if raw_res:
        print(raw_res.block_tx_count)
        await upsert_data(manager, raw_res, bucket, chain)

    async with (await manager.getPool()).acquire() as conn:
        async with conn.transaction():
            async for record in wrong_tx_count_cursor(conn, chain):
                assert record["height"] == 2316140
    mocker.resetall()
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=unparsed["block"],
    )
    mocker.patch(
        "indexer.chain.CosmosChain.get_block_txs",
        return_value={"tx_responses": unparsed["txs"]},
    )
    test_tx = await chain.get_block_txs(manager, 2316140)
    if test_tx:
        print("test_tx", len(test_tx["tx_responses"]))

    await backfill_wrong_count(manager, chain, bucket)

    async with (await manager.getPool()).acquire() as conn:
        async with conn.transaction():
            wrong_tx_heights = [
                record["height"] async for record in wrong_tx_count_cursor(conn, chain)
            ]
            assert wrong_tx_heights == []

    async with (await manager.getPool()).acquire() as conn:
        print(await conn.fetch("select * from raw"))
        rec = await conn.fetch(
            "select block_tx_count, tx_tx_count from raw where height = 2316140"
        )
        print(rec)

    assert rec[0]["block_tx_count"] == rec[0]["tx_tx_count"]

    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)

    mocker.resetall()


async def test_wrong_tx_count_cursor_more_than_step_size(
    chain: CosmosChain,
    schema: str,
    mocker,
    raws: List[Raw],
    unparsed_raw_data: List[dict],
    storage_config: Tuple[ClientSession, Storage, Bucket],
    manager: Manager,
):
    storage_session, storage, bucket = storage_config
    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)
        await create_tables(conn, schema)

    current_height = 0
    chain.chain_id = "jackal-1"
    raw = raws[0]
    chain.step_size = 2
    unparsed = unparsed_raw_data[0]
    if not raw.height:
        return
    for unparse in unparsed_raw_data[0 : chain.step_size + 1]:
        mocker.patch(
            "indexer.chain.CosmosChain.get_block", return_value=unparse["block"]
        )
        mocker.patch(
            "indexer.chain.CosmosChain.get_block_txs", return_value={"tx_responses": []}
        )
        raw_res = await get_data_live(manager, chain, current_height)
        if raw_res:
            print(raw_res.block_tx_count)
            await upsert_data(manager, raw_res, bucket, chain)

    async with (await manager.getPool()).acquire() as conn:
        async with conn.transaction():
            wrong_counts = [
                record["height"] async for record in wrong_tx_count_cursor(conn, chain)
            ]

            assert wrong_counts == [2316140, 2316141, 2316142]

    mocker.patch("indexer.backfill.run_and_upsert_tasks", return_value=None)

    await backfill_wrong_count(manager, chain, bucket)
    mocker.resetall()


async def test_get_data_historical_block_is_none(
    mocker,
    chain: CosmosChain,
    manager: Manager,
):
    mocker.patch("indexer.chain.CosmosChain.get_block", return_value=None)

    assert None == await get_data_historical(manager, chain, 1)


async def test_missing_blocks_cursor_more_than_20(
    chain: CosmosChain,
    schema: str,
    mocker,
    raws: List[Raw],
    unparsed_raw_data: List[dict],
    storage_config: Tuple[ClientSession, Storage, Bucket],
    manager: Manager,
):
    storage_session, storage, bucket = storage_config
    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)
        await create_tables(conn, schema)

    current_height = 0
    chain.chain_id = "jackal-1"
    raw = raws[0]
    unparsed = unparsed_raw_data[0]
    if not raw.height:
        return
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=unparsed["block"],
    )
    mocker.patch(
        "indexer.chain.CosmosChain.get_block_txs",
        return_value={"tx_responses": unparsed["txs"]},
    )
    raw_res = await get_data_live(manager, chain, current_height)
    if raw_res:
        await upsert_data(manager, raw_res, bucket, chain)

    async with (await manager.getPool()).acquire() as cursor_conn:
        async with cursor_conn.transaction():
            raw_tasks = []
            wrong_tx_heights = [
                (height, dif)
                async for (height, dif) in missing_blocks_cursor(cursor_conn, chain)
            ]

            assert [(2316140, -1)] == wrong_tx_heights

    mocker.resetall()
    mocker.patch(
        "indexer.chain.CosmosChain.get_lowest_height",
        return_value=2316140 - chain.batch_size - chain.step_size,
    )
    mocker.patch("indexer.backfill.run_and_upsert_tasks", return_value=None)
    await backfill_historical(manager, chain, bucket)

    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)

    mocker.resetall()


async def test_missing_blocks_cursor_less_than_20(
    chain: CosmosChain,
    schema: str,
    mocker,
    raws: List[Raw],
    unparsed_raw_data: List[dict],
    storage_config: Tuple[ClientSession, Storage, Bucket],
    manager: Manager,
):
    storage_session, storage, bucket = storage_config
    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)
        await create_tables(conn, schema)

    current_height = 0
    chain.chain_id = "jackal-1"
    raw = raws[0]
    unparsed = unparsed_raw_data[0]
    if not raw.height:
        return
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=unparsed["block"],
    )
    mocker.patch(
        "indexer.chain.CosmosChain.get_block_txs",
        return_value={"tx_responses": unparsed["txs"]},
    )
    raw_res = await get_data_live(manager, chain, current_height)
    if raw_res:
        await upsert_data(manager, raw_res, bucket, chain)

    async with (await manager.getPool()).acquire() as cursor_conn:
        async with cursor_conn.transaction():
            raw_tasks = []
            wrong_tx_heights = [
                (height, dif)
                async for (height, dif) in missing_blocks_cursor(cursor_conn, chain)
            ]

            assert [(2316140, -1)] == wrong_tx_heights

    mocker.resetall()
    mocker.patch(
        "indexer.chain.CosmosChain.get_lowest_height",
        return_value=2316140 - 40,
    )
    mocker.patch("indexer.backfill.run_and_upsert_tasks", return_value=None)
    await backfill_wrong_count(manager, chain, bucket)

    async with (await manager.getPool()).acquire() as conn:
        await drop_tables(conn, schema)

    mocker.resetall()


async def test_process_tx_incorrect_wrong_input(chain: CosmosChain, manager: Manager):
    raw = Raw()
    assert await process_tx(raw, manager, chain) == None


async def test_parse_block_unsuccessfully_parse(
    chain: CosmosChain, mocker: MockerFixture, manager: Manager
):
    mocker.patch("parse.Raw.parse_block", return_value=None)

    assert await process_block({}, manager, chain) == None

    mocker.resetall()
