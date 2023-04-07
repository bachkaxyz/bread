from dataclasses import dataclass, field
from datetime import datetime
import json
from typing import List

from indexer.parser import Log, parse_logs
from indexer.exceptions import BlockNotParsedError


@dataclass
class Block:
    height: int
    chain_id: str
    time: datetime
    block_hash: str
    proposer_address: str

    def get_db_params(self):
        return (
            self.chain_id,
            self.height,
            self.time,
            self.block_hash,
            self.proposer_address,
        )


@dataclass
class Tx:
    txhash: str
    chain_id: str
    height: int
    code: str
    data: str
    info: str
    logs: dict
    events: dict
    raw_log: str
    gas_used: int
    gas_wanted: int
    codespace: str
    timestamp: datetime


@dataclass
class Raw:
    height: int | None = None
    chain_id: str | None = None

    raw_block = dict
    raw_tx = List[dict]

    block_tx_count: int = 0
    tx_responses_tx_count: int = 0

    block: Block | None = None
    txs: List[Tx] = field(default_factory=list)
    logs: List[Log] = field(default_factory=list)
    log_columns: set = field(default_factory=set)

    def parse_block(self, raw_block: dict):
        self.raw_block = raw_block

        block = raw_block["block"]
        header = block["header"]
        height, chain_id, time, proposer_address = (
            int(header["height"]),
            header["chain_id"],
            header["time"],
            header["proposer_address"],
        )
        block_hash = raw_block["block_id"]["hash"]
        txs = block["data"]["txs"]
        self.block_tx_count = len(txs)

        self.chain_id = chain_id
        self.height = height
        self.block = Block(
            height=height,
            chain_id=chain_id,
            time=datetime.now(),
            block_hash=block_hash,
            proposer_address=proposer_address,
        )

    def parse_tx_responses(self, raw_tx_responses: dict):
        self.raw_tx = raw_tx_responses
        self.tx_responses_tx_count = len(raw_tx_responses)
        if self.block and self.chain_id:
            for tx_response in raw_tx_responses:
                self.txs.append(
                    Tx(
                        txhash=tx_response["txhash"],
                        height=int(tx_response["height"]),
                        chain_id=self.chain_id,
                        code=str(tx_response["code"]),
                        data=tx_response["data"],
                        info=tx_response["info"],
                        logs=tx_response["logs"],
                        events=tx_response["events"],
                        raw_log=tx_response["raw_log"],
                        gas_used=int(tx_response["gas_used"]),
                        gas_wanted=int(tx_response["gas_wanted"]),
                        codespace=tx_response["codespace"],
                        timestamp=datetime.now(),  # should be tx_response["timestamp"] but did .now for speed
                    )
                )
                logs = parse_logs(
                    tx_response["raw_log"],
                    tx_response["txhash"],
                )
                self.logs.extend(logs)
                for log in logs:
                    self.log_columns = self.log_columns.union(log.get_cols())
        else:
            raise BlockNotParsedError(
                "Block needs to be parsed before a transaction in that block can be parsed"
            )

    def get_db_params(self):
        return (
            self.chain_id,
            self.height,
            json.dumps(self.raw_block),
            self.block_tx_count,
            json.dumps(self.raw_tx) if self.tx_responses_tx_count > 0 else None,
            self.tx_responses_tx_count,
        )
