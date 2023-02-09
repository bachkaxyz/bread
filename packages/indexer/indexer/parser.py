import json
import time
import pandas as pd


def parse_logs(raw_logs: str, txhash: str):
    logs = []
    log_cols = set()
    try:  # try to parse the logs as json, if it fails, it's a string error message (i dont like this but....)
        raw_logs = json.loads(raw_logs)
    except:
        return [
            {
                "failed": True,
                "txhash": txhash,
                "msg_index": 0,
                "error_msg": raw_logs,
            }
        ], set(["failed", "txhash", "msg_index", "error_msg"])

    for msg_index, log in enumerate(raw_logs):
        log_dic = {}
        for i, event in enumerate(log["events"]):
            log_dic.update(flatten_logs(event))
        log_dic["txhash"] = txhash
        log_dic["msg_index"] = msg_index
        log_dic = {fix_entry(k): fix_entry(v) for k, v in log_dic.items()}
        log_dic["failed"] = False
        log_cols.update(log_dic.keys())
        logs.append(log_dic)
    return logs, log_cols


types = set()
packet_payloads = []


def flatten_logs(event):
    log_dic = {}
    type = event["type"]
    log_cols = set()
    valid_packet_payloads = [
        "packet_connection",
        "packet_src_channel",
        "packet_dst_channel",
        "packet_src_port",
        "packet_timeout_timestamp",
        "packet_timeout_timestamp",
        "packet_data",
    ]
    if type == "wasm":
        for a in event["attributes"]:
            key = a["key"]
            if key == "contract_address":
                value = a["value"] if "value" in a.keys() else None
                wasm_dict = {"wasm_key": key, "wasm_value": value}
                log_dic.update(wasm_dict)
                log_cols.add("wasm_key")
                log_cols.add("wasm_value")
            else:
                pass
    else:
        for attr in event["attributes"]:
            if (
                not attr["key"].startswith("packet")
                or attr["key"] in valid_packet_payloads
            ):
                log_cols.add(attr["key"])
                log_dic[f"{type}_{attr['key']}"] = (
                    attr["value"] if "value" in attr.keys() else ""
                )
            else:
                packet_payloads.append(str({attr["key"]: attr["value"]}))
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


with open("indexer/test_data.csv", "r") as f:
    df = pd.read_csv(f)

l_msg = []
l_log = []
for i, row in df.iterrows():
    # print(row[0])
    tx = json.loads(row["tx"])
    logs = row["raw_log"]
    msgs, msg_cols = parse_messages(tx["body"]["messages"], "txhash")
    logs, log_cols = parse_logs(logs, "txhash")
    l_msg.extend(msgs)
    l_log.extend(logs)
msg_df = pd.DataFrame(l_msg)
msg_df.to_csv("indexer/msg_data_parsed.csv")
log_df = pd.DataFrame(l_log)
log_df.to_csv("indexer/log_data_parsed.csv")


types = list(types)
types.sort()
with open("indexer/types.txt", "w") as f:
    f.write("\n".join(types))
packet_payloads.sort()
with open("indexer/packet_payloads.txt", "w") as f:
    f.write("\n".join(packet_payloads))
