import json
from typing import List
from indexer.parser import Log, parse_log_event, parse_logs
import pytest


@pytest.fixture(scope="module")
def log_data():
    with open("tests/test_data/log_data.json", "r") as f:
        return json.load(f)


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
