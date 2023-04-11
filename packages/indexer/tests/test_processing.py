from typing import List
from indexer.chain import CosmosChain
from indexer.parser import Raw
from indexer.live import live, get_data_live
from indexer.db import create_tables, drop_tables
from tests.db_test import raws, mock_schema, mock_client, mock_pool, unparsed_raw_data
from tests.chain_test import mock_client, mock_chain

from asyncpg import Pool
from aiohttp import ClientSession


async def test_live(
    raws: List[Raw],
    mock_schema: str,
    mock_client: ClientSession,
    mock_chain: CosmosChain,
    mock_pool: Pool,
    mocker,
):
    async with mock_pool.acquire() as conn:
        await create_tables(conn, mock_schema)
    mocker.patch("indexer.live.get_data_live", return_value=raws[0])
    await live(mock_client, mock_chain, mock_pool)

    async with mock_pool.acquire() as conn:
        await drop_tables(conn, mock_schema)

    mocker.resetall()


async def test_get_data_live_correct(
    raws: List[Raw],
    unparsed_raw_data,
    mocker,
    mock_chain: CosmosChain,
    mock_client: ClientSession,
):
    mock_chain.chain_id = "jackal-1"
    current_height = 0
    for i, raw in enumerate(raws):
        if raw.height:
            mocker.patch(
                "indexer.chain.CosmosChain.get_block",
                return_value=unparsed_raw_data[i]["block"],
            )
            mocker.patch(
                "indexer.chain.CosmosChain.get_block_txs",
                return_value={"tx_responses": unparsed_raw_data[i]["txs"]},
            )
            raw_res = await get_data_live(mock_client, mock_chain, current_height)

            assert raw_res == raw
            if raw:
                current_height = raw.height


async def test_wrong_tx_count_live(
    raws: List[Raw],
    unparsed_raw_data,
    mocker,
    mock_chain: CosmosChain,
    mock_client: ClientSession,
):
    mock_chain.chain_id = "jackal-1"
    raw = raws[0]
    unparsed = unparsed_raw_data[0]
    if not raw.height:
        return

    # 5 incorrect cases:

    # tx count not correct
    current_height = 0
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=unparsed["block"],
    )
    mocker.patch(
        "indexer.chain.CosmosChain.get_block_txs",
        return_value={"tx_responses": unparsed_raw_data[1]["txs"]},
    )
    raw_res = await get_data_live(mock_client, mock_chain, current_height)
    assert raw_res == Raw(
        height=raw.height,
        chain_id=raw.chain_id,
        block_tx_count=raw.block_tx_count,
        tx_responses_tx_count=0,
        block=raw.block,
        raw_block=raw.raw_block,
    )

    # tx_response doesn't exist
    current_height = 0
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=unparsed["block"],
    )
    mocker.patch(
        "indexer.chain.CosmosChain.get_block_txs",
        return_value=unparsed["txs"],
    )
    raw_res = await get_data_live(mock_client, mock_chain, current_height)
    assert raw_res == Raw(
        height=raw.height,
        chain_id=raw.chain_id,
        block_tx_count=raw.block_tx_count,
        tx_responses_tx_count=0,
        block=raw.block,
        raw_block=raw.raw_block,
    )

    # no txs
    no_txs_unparsed = unparsed
    no_txs_unparsed["block"]["block"]["data"]["txs"] = []
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=no_txs_unparsed["block"],
    )
    raw_res = await get_data_live(mock_client, mock_chain, current_height)

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
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=unparsed["block"],
    )
    raw_res = await get_data_live(mock_client, mock_chain, current_height)
    assert raw_res == None

    # block data is none
    mocker.patch(
        "indexer.chain.CosmosChain.get_block",
        return_value=None
    )
    raw_res = await get_data_live(mock_client, mock_chain, current_height)
    assert raw_res == None