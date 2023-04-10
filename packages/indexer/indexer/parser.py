from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
import json
import time
from typing import List
from asyncpg import Connection

from indexer.exceptions import BlockNotParsedError

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass
class Log:
    txhash: str
    failed_msg: str | None = None
    failed: bool = False
    msg_index: int = 0
    event_attributes = defaultdict(list)

    def get_cols(self):
        return set(self.event_attributes.keys())

    def fix_entries(self):
        self.event_attributes = {
            (fix_entry(k[0]), fix_entry(k[1])): [fix_entry(v) for v in vs]
            for k, vs in self.event_attributes.items()
        }

    def dump(self):
        final = defaultdict(list)
        for k, v in self.event_attributes.items():
            event, attr = k
            final[f"{event}_{attr}"].extend(v)
        return json.dumps(final)

    def get_log_db_params(self):
        return (
            self.txhash,
            str(self.msg_index),
            self.dump(),
            self.failed,
            str(self.failed_msg) if self.failed_msg else None,
        )


def parse_logs(raw_logs: str, txhash: str) -> List[Log]:
    logs: List[Log] = []
    json_raw_logs: dict = {}
    try:  # try to parse the logs as json, if it fails, it's a string error message (i dont like this but....)
        json_raw_logs = json.loads(raw_logs)
    except:
        return [Log(txhash, failed=True, failed_msg=raw_logs)]

    for msg_index, raw_log in enumerate(json_raw_logs):  # for each message
        log = Log(txhash=txhash, msg_index=msg_index)
        log.event_attributes = defaultdict(list)
        # for each event in the message
        for i, event in enumerate(raw_log["events"]):
            updated_log_dic = parse_log_event(event)
            log.event_attributes.update(updated_log_dic)
        log.fix_entries()
        logs.append(log)
    return logs


def parse_log_event(event: dict):
    log_dic = defaultdict(list)
    event_type = event["type"]
    if event_type == "wasm":
        for a in event["attributes"]:
            key = a["key"]
            if key == "contract_address":
                value = a["value"] if "value" in a.keys() else None
                log_dic[(event_type, key)].append(value)
            else:
                pass
    else:
        for attr in event["attributes"]:
            if "key" in attr.keys():
                log_dic[(event_type, attr["key"])].append(
                    (attr["value"] if "value" in attr.keys() else "")
                )
    return log_dic


def fix_entry(s) -> str:
    return str(s).replace(".", "_").replace("/", "_").replace("-", "_").replace("@", "")


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

    def get_db_params(self):
        return (
            self.txhash,
            self.chain_id,
            self.height,
            self.code,
            self.data,
            self.info,
            json.dumps(self.logs),
            json.dumps(self.events),
            self.raw_log,
            self.gas_used,
            self.gas_wanted,
            self.codespace,
            self.timestamp,
        )


@dataclass
class Raw:
    height: int | None = None
    chain_id: str | None = None

    raw_block: dict | None = None
    raw_tx: List[dict] | None = None

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
            datetime.strptime(
                header["time"][:-4] + "Z", "%Y-%m-%dT%H:%M:%S.%fZ"
            ),  # the %f accepts 6 digits of a decimal not 9, so strip last 4 (last character is a "Z") and add back the removed "Z"
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
            time=time,
            block_hash=block_hash,
            proposer_address=proposer_address,
        )

    def parse_tx_responses(self, raw_tx_responses: List[dict]):
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
                        timestamp=datetime.strptime(
                            tx_response["timestamp"], "%Y-%m-%dT%H:%M:%SZ"
                        ),
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

    def get_raw_db_params(self):
        return (
            self.chain_id,
            self.height,
            json.dumps(self.raw_block),
            self.block_tx_count,
            json.dumps(self.raw_tx) if self.tx_responses_tx_count > 0 else None,
            self.tx_responses_tx_count,
        )

    def get_txs_db_params(self):
        return [tx.get_db_params() for tx in self.txs]

    def get_log_columns_db_params(self):
        return [[e, a] for e, a in self.log_columns]

    def get_logs_db_params(self):
        return [log.get_log_db_params() for log in self.logs]


# def flatten_msg(msg: dict):
#     updated_msg = {}
#     for k, v in msg.items():
#         if k == "commit":  # this is a reserved word in postgres
#             k = "_commit"
#         if isinstance(v, dict):
#             updated_sub_msg = flatten_msg(v)
#             for k1, v1 in updated_sub_msg.items():
#                 updated_msg[f"{k}_{k1}"] = v1
#         elif isinstance(v, list):
#             updated_msg[k] = json.dumps(v)
#         else:
#             updated_msg[k] = str(v)
#     return updated_msg


# def parse_messages(messages: dict, txhash: str):
#     msgs = []
#     msg_cols = set()
#     for msg_index, msg in enumerate(messages):
#         msg_dic = flatten_msg(msg)

#         msg_dic = {fix_entry(k): fix_entry(v) for k, v in msg_dic.items()}
#         msg_dic["txhash"] = txhash
#         msg_dic["msg_index"] = msg_index
#         msg_cols.update(msg_dic.keys())
#         msgs.append(msg_dic)
#     return msgs, msg_cols
