from datetime import datetime
import json
from typing import List
from indexer.parser import Log, Raw, parse_log_event, parse_logs
import pytest

from indexer.exceptions import BlockNotParsedError


@pytest.fixture(scope="module")
def log_data():
    with open("tests/test_data/log_data.json", "r") as f:
        return json.load(f)


@pytest.fixture
def unparsed_raw_data() -> dict:
    with open("tests/test_data/test_data.json", "r") as f:
        return json.load(f)


@pytest.fixture
def raw_tx(unparsed_raw_data) -> dict:
    return unparsed_raw_data[0]["txs"]


@pytest.fixture
def raws(unparsed_raw_data) -> List[Raw]:
    raws = []
    for data in unparsed_raw_data:
        txs, block = data["txs"], data["block"]
        raw = Raw()
        raw.parse_block(block)
        if txs:
            raw.parse_tx_responses(txs)
        raws.append(raw)
    return raws


def test_parse_individual_log_event(log_data):
    # test parsing an individual log event
    event = log_data[0]["events"][0]
    parsed_log = parse_log_event(event)
    assert parsed_log == {
        ("message", "action"): ["/secret.compute.v1beta1.MsgExecuteContract"],
        ("message", "module"): ["compute"],
        ("message", "sender"): ["secret1rjml9f6ma7rwgqq9ud8e0aw2arhhj4v4hhzc4k"],
        ("message", "contract_address"): [
            "secret1266jqzsyw98g3v8cz5cyhw2s9kwhtmtdnr0898"
        ],
    }


def test_parse_tx_logs(log_data):
    # multiple events in one log
    logs = parse_logs(json.dumps(log_data), "test_tx_hash")
    assert [log.get_cols() for log in logs] == [
        {
            ("message", "action"),
            ("message", "contract_address"),
            ("wasm", "contract_address"),
            ("message", "sender"),
            ("message", "module"),
        }
    ]

    assert [log.dump() for log in logs] == [
        '{"message_action": ["_secret_compute_v1beta1_MsgExecuteContract"], "message_module": ["compute"], "message_sender": ["secret1rjml9f6ma7rwgqq9ud8e0aw2arhhj4v4hhzc4k"], "message_contract_address": ["secret1266jqzsyw98g3v8cz5cyhw2s9kwhtmtdnr0898"], "wasm_contract_address": ["secret1266jqzsyw98g3v8cz5cyhw2s9kwhtmtdnr0898", "secret1rgm2m5t530tdzyd99775n6vzumxa5luxcllml4"]}'
    ]


def test_parse_tx_log_error():
    logs = parse_logs("invalid json", "test_txhash")
    assert logs == [
        Log(txhash="test_txhash", failed_msg="invalid json", failed=True, msg_index=0)
    ]


def test_parsing(raws: List[Raw], unparsed_raw_data):
    for i, (raw, raw_data) in enumerate(zip(raws, unparsed_raw_data)):
        # raw data
        raw_block, raw_txs = raw_data["block"], raw_data["txs"]

        assert raw.raw_block == raw_block
        assert raw.raw_tx == raw_txs

        # tx count
        assert raw.block_tx_count == len(raw_block["block"]["data"]["txs"])
        assert raw.tx_responses_tx_count == len(raw_txs)
        assert raw.block_tx_count == raw.tx_responses_tx_count

        # block
        if raw.block:
            assert raw.block.height == int(raw_block["block"]["header"]["height"])
            assert raw.block.chain_id == raw_block["block"]["header"]["chain_id"]
            assert raw.block.time == datetime.strptime(
                raw_block["block"]["header"]["time"][:-4] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            assert (
                raw.block.proposer_address
                == raw_block["block"]["header"]["proposer_address"]
            )
            assert raw.block.block_hash == raw_block["block_id"]["hash"]

        assert len(raw_txs) == len(raw.txs)
        for tx, raw_tx in zip(raw.txs, raw_txs):
            assert tx.txhash == raw_tx["txhash"]
            assert tx.height == int(raw_tx["height"])
            assert tx.chain_id == raw_block["block"]["header"]["chain_id"]
            assert tx.code == str(raw_tx["code"])
            assert tx.data == raw_tx["data"]
            assert tx.info == raw_tx["info"]
            assert tx.logs == raw_tx["logs"]
            assert tx.events == raw_tx["events"]
            assert tx.raw_log == raw_tx["raw_log"]
            assert tx.gas_used == int(raw_tx["gas_used"])
            assert tx.gas_wanted == int(raw_tx["gas_wanted"])
            assert tx.codespace == raw_tx["codespace"]
            assert tx.timestamp == datetime.strptime(
                raw_tx["timestamp"], "%Y-%m-%dT%H:%M:%SZ"
            )


def test_parse_tx_before_block_error(raw_tx):
    raw = Raw()
    with pytest.raises(BlockNotParsedError):
        raw.parse_tx_responses(raw_tx)
