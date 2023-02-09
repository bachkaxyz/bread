CREATE TABLE IF NOT EXISTS raw (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    block JSONB,      
    txs JSONB,
    blocks_txs_parsed_at TIMESTAMP DEFAULT NULL,
    messages_parsed_at TIMESTAMP DEFAULT NULL,
    logs_parsed_at TIMESTAMP DEFAULT NULL,
                
    PRIMARY KEY (chain_id, height)
);
CREATE TABLE IF NOT EXISTS blocks (
    height BIGINT NOT NULL,
    chain_id TEXT NOT NULL,
    time TIMESTAMP NOT NULL,
    block_hash TEXT NOT NULL,
    proposer_address TEXT NOT NULL,
    
    PRIMARY KEY (chain_id, height)
);
CREATE TABLE IF NOT EXISTS txs (
    txhash TEXT NOT NULL PRIMARY KEY,
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    tx JSONB,
    tx_response JSONB,
    tx_response_tx_type TEXT,
    code TEXT,
    data TEXT,
    info TEXT,
    logs JSONB,
    events JSONB,
    raw_log TEXT,
    gas_used BIGINT,
    gas_wanted BIGINT,
    codespace TEXT,
    timestamp TIMESTAMP,
    
    FOREIGN KEY (chain_id, height) REFERENCES blocks (chain_id, height)
);
CREATE TABLE IF NOT EXISTS messages (
    txhash TEXT NOT NULL,
    msg_index TEXT NOT NULL,
    
    PRIMARY KEY (txhash, msg_index),
    FOREIGN KEY (txhash) REFERENCES txs (txhash)
);
CREATE TABLE IF NOT EXISTS logs (
    txhash TEXT NOT NULL,
    msg_index TEXT NOT NULL,
    unparsed_columns JSONB,
    -- other columns added by the indexer
    
    PRIMARY KEY (txhash, msg_index),
    FOREIGN KEY (txhash, msg_index) REFERENCES messages (txhash, msg_index)
);

CREATE TABLE IF NOT EXISTS log_columns (
    event TEXT NOT NULL,
    attribute TEXT NOT NULL,
    parse BOOLEAN NOT NULL,

    PRIMARY KEY (event, attribute)
);