import json


def parse_logs(raw_logs: dict, txhash: str):
    logs = []
    log_cols = set()
    for msg_index, log in enumerate(json.loads(raw_logs)):
        log_dic = {}
        for i, event in enumerate(log["events"]):
            log_dic.update(flatten_logs(event))
        log_dic["txhash"] = txhash
        log_dic["msg_index"] = msg_index
        log_dic = {fix_entry(k): fix_entry(v) for k, v in log_dic.items()}
        log_cols.update(log_dic.keys())
        logs.append(log_dic)
    return logs, log_cols


def flatten_logs(event):
    log_dic = {}
    type = event["type"]
    if type == "wasm":
        for a in event["attributes"]:
            key = a["key"]
            if key == "contract_address":
                value = a["value"] if "value" in a.keys() else None
                wasm_dict = {"wasm_key": key, "wasm_value": value}
                log_dic.update(wasm_dict)
            else:
                pass
    else:
        for attr in event["attributes"]:
            log_dic[f"{type}_{attr['key']}"] = attr["value"]
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
