from typing import List
import pytest
import json

from parse import Raw


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
