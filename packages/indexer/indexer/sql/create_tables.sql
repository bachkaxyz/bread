CREATE TABLE IF NOT EXISTS raw (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    block JSONB,      
    txs JSONB,
                
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