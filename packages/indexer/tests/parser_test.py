import json
from indexer.parser import Log, parse_log_event, parse_logs


raw_log = [{"events":[{"type":"message","attributes":[{"key":"action","value":"/secret.compute.v1beta1.MsgExecuteContract"},{"key":"module","value":"compute"},{"key":"sender","value":"secret1rjml9f6ma7rwgqq9ud8e0aw2arhhj4v4hhzc4k"},{"key":"contract_address","value":"secret1266jqzsyw98g3v8cz5cyhw2s9kwhtmtdnr0898"}]},{"type":"wasm","attributes":[{"key":"contract_address","value":"secret1266jqzsyw98g3v8cz5cyhw2s9kwhtmtdnr0898"},{"key":"CjABfNUNmW/ZmkxacGL/yqDywwk+ew==","value":"Odng8iw3EBn0t4v+EwnnQFcBICMlbaebFvusZxxENqz6PUY5"},{"key":"Yxlzf/42hUhOklVo87eSe1O5FyaVnXi+Hw==","value":"/X5CRFEbnjNX1RMf69r19XiYryGJLh/cV4swLLorrKJIJnyB4qrkd5aPrlAJEHzpSK1bjpgdKBKAtG68Ag=="},{"key":"contract_address","value":"secret1rgm2m5t530tdzyd99775n6vzumxa5luxcllml4"}]}]}]
def test_parse_log_event():
    event = raw_log[0]["events"][0]
    parsed_log = parse_log_event(event)
    # todo: log event attributes are not being parsed correctly
    # account for duplicate keys (in the following the first contract address is being )
    assert parsed_log ==  'fail_on_purpose'
    

def test_parse_tx_log():
    logs = parse_logs(json.dumps(raw_log), "test_txhash")
    assert [log.event_attributes for log in logs] == [{('message', 'action'): '_secret_compute_v1beta1_MsgExecuteContract', ('message', 'module'): 'compute', ('message', 'sender'): 'secret1rjml9f6ma7rwgqq9ud8e0aw2arhhj4v4hhzc4k', ('message', 'contract_address'): 'secret1266jqzsyw98g3v8cz5cyhw2s9kwhtmtdnr0898', ('wasm', 'contract_address'): 'secret1rgm2m5t530tdzyd99775n6vzumxa5luxcllml4'}]

def test_parse_tx_cols():
    logs = parse_logs(json.dumps(raw_log), "test_txhash")
    assert [log.get_cols() for log in logs] == [{('message', 'action'), ('message', 'contract_address'), ('wasm', 'contract_address'), ('message', 'sender'), ('message', 'module')}]