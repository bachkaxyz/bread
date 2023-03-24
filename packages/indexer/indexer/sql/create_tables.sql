CREATE TABLE IF NOT EXISTS raw_blocks (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    block JSONB,
    parsed_at TIMESTAMP DEFAULT NULL,
    
    PRIMARY KEY (chain_id, height)
);
CREATE TABLE IF NOT EXISTS blocks (
    height BIGINT NOT NULL,
    chain_id TEXT NOT NULL,
    time TIMESTAMP NOT NULL DEFAULT NOW(),
    block_hash TEXT NOT NULL,
    proposer_address TEXT NOT NULL,
    
    PRIMARY KEY (chain_id, height)
);
CREATE TABLE IF NOT EXISTS raw_txs (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    txs JSONB,
    parsed_at TIMESTAMP DEFAULT NULL,

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
    logs_parsed_at TIMESTAMP DEFAULT NULL,    
    
    FOREIGN KEY (chain_id, height) REFERENCES raw_txs(chain_id, height) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS logs (
    txhash TEXT NOT NULL,
    msg_index TEXT NOT NULL, -- This should be an int
    parsed JSONB,
    failed BOOLEAN NOT NULL DEFAULT FALSE,
    failed_msg TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (txhash, msg_index),
    FOREIGN KEY (txhash) REFERENCES txs(txhash) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS log_columns (
    event TEXT NOT NULL,
    attribute TEXT NOT NULL,
    parse BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (event, attribute)
);