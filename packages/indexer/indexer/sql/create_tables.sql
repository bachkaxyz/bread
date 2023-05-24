CREATE SCHEMA IF NOT EXISTS $schema;

CREATE TABLE IF NOT EXISTS $schema.raw (
    chain_id TEXT NOT NULL,
    height BIGINT NOT NULL,
    block_tx_count BIGINT,
    tx_tx_count BIGINT, 
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (chain_id, height)
);
CREATE TABLE IF NOT EXISTS $schema.blocks(
    height BIGINT NOT NULL,
    chain_id TEXT NOT NULL,
    time TIMESTAMP NOT NULL,
    block_hash TEXT NOT NULL,
    proposer_address TEXT NOT NULL,

    PRIMARY KEY (chain_id, height),
    FOREIGN KEY (chain_id, height) REFERENCES $schema.raw(chain_id, height) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS $schema.txs(
    txhash TEXT NOT NULL PRIMARY KEY,
    chain_id TEXT NOT NULL, 
    height BIGINT NOT NULL,
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
    tx JSONB,
    FOREIGN KEY (chain_id, height) REFERENCES $schema.raw(chain_id, height) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS $schema.logs (
    txhash TEXT NOT NULL,
    msg_index TEXT NOT NULL,  -- This should be an int
    parsed JSONB,
    failed BOOLEAN NOT NULL DEFAULT FALSE,
    failed_msg TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (txhash, msg_index),
    FOREIGN KEY (txhash) REFERENCES $schema.txs(txhash) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS $schema.log_columns (
    event TEXT NOT NULL,
    attribute TEXT NOT NULL,
    parse BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (event, attribute)
);

CREATE TABLE IF NOT EXISTS $schema.messages (
    txhash TEXT NOT NULL,
    msg_index TEXT NOT NULL, 
    type TEXT,
    parsed JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (txhash, msg_index),
    FOREIGN KEY (txhash) REFERENCES $schema.txs(txhash) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS $schema.msg_columns (
    attribute TEXT NOT NULL PRIMARY KEY,
    parse BOOLEAN NOT NULL DEFAULT FALSE
);
