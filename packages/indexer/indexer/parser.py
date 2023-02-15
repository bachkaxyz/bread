from dataclasses import dataclass
import json
import time
from typing import List, Tuple
from base64 import b64decode, b64encode
@dataclass
class Log:
    txhash: str
    failed_msg: str = None
    failed: bool = False
    msg_index: int = 0
    event_attributes = {}
    
    def get_cols(self):
        return set(self.event_attributes.keys())
    
        
    def fix_entries(self):
        self.event_attributes = {(fix_entry(k[0]), fix_entry(k[1])): fix_entry(v) for k, v in self.event_attributes.items()}
    
    def dump(self):
        final = {}
        for k, v in self.event_attributes.items():
            event, attr = k
            final[f'{event}_{attr}'] = v
        return json.dumps(final)

    

def parse_logs(raw_logs: str, txhash: str) -> List[Log]:
    logs: List[Log] = []
    try:  # try to parse the logs as json, if it fails, it's a string error message (i dont like this but....)
        raw_logs = json.loads(raw_logs)
    except:
        return [Log(txhash, failed=True, failed_msg=raw_logs)]

    for msg_index, raw_log in enumerate(raw_logs): # for each message
        log = Log(txhash=txhash, msg_index=msg_index)
        # for each event in the message
        for i, event in enumerate(raw_log["events"]):
            updated_log_dic = parse_log_event(event)
            log.event_attributes.update(updated_log_dic)
        log.fix_entries()
        logs.append(log)
    return logs


types = set()
packet_payloads = []


def parse_log_event(event: dict):
    log_dic = {}
    type = event["type"]
    if type == "wasm":
        for a in event["attributes"]:
            key = a["key"]
            if key == "contract_address":
                value = a["value"] if "value" in a.keys() else None
                wasm_dict = {("wasm", key): value}
                log_dic.update(wasm_dict)
            else:
                pass
    else:
        for attr in event["attributes"]:
            # try:
            #     print(b64decode(attr["key"]))
            # except:
            #     print(attr["key"])
            log_dic[(type, attr['key'])] = (
                attr["value"] if "value" in attr.keys() else ""
            )
            # packet_payloads.append(str({type + "|" + attr["key"]: attr["value"]}))
    # types.add(json.dumps({type: list(log_cols)}))
    return log_dic


def fix_entry(s: any) -> str:
    return str(s).replace(".", "_").replace("/", "_").replace("-", "_").replace("@", "")


def flatten_msg(msg: dict):
    updated_msg = {}
    for k, v in msg.items():
        if k == "commit":  # this is a reserved word in postgres
            k = "_commit"
        if isinstance(v, dict):
            updated_sub_msg = flatten_msg(v)
            for k1, v1 in updated_sub_msg.items():
                updated_msg[f"{k}_{k1}"] = v1
        elif isinstance(v, list):
            updated_msg[k] = json.dumps(v)
        else:
            updated_msg[k] = str(v)
    return updated_msg


def parse_messages(messages: dict, txhash: str):
    msgs = []
    msg_cols = set()
    for msg_index, msg in enumerate(messages):
        msg_dic = flatten_msg(msg)

        msg_dic = {fix_entry(k): fix_entry(v) for k, v in msg_dic.items()}
        msg_dic["txhash"] = txhash
        msg_dic["msg_index"] = msg_index
        msg_cols.update(msg_dic.keys())
        msgs.append(msg_dic)
    return msgs, msg_cols


# with open("indexer/test_data.csv", "r") as f:
#     df = pd.read_csv(f)

# l_msg = []
# l_log = []
# for i, row in df.iterrows():
#     # print(row[0])
#     tx = json.loads(row["tx"])
#     logs = row["raw_log"]
#     msgs, msg_cols = parse_messages(tx["body"]["messages"], "txhash")
#     logs, log_cols = parse_logs(logs, "txhash")
#     l_msg.extend(msgs)
#     l_log.extend(logs)
# msg_df = pd.DataFrame(l_msg)
# msg_df.to_csv("indexer/msg_data_parsed.csv")
# log_df = pd.DataFrame(l_log)
# log_df.to_csv("indexer/log_data_parsed.csv")


# types = list(types)
# types.sort()
# with open("indexer/types.txt", "w") as f:
#     f.write("\n".join(types))
# packet_payloads.sort()
# with open("indexer/packet_payloads.txt", "w") as f:
#     f.write("\n".join(packet_payloads))
