CREATE OR REPLACE FUNCTION parse_raw_block() RETURNS TRIGGER AS
$$ 
    BEGIN
        INSERT INTO blocks (height, chain_id, time, block_hash, proposer_address)
        VALUES (
            NEW.height,
            NEW.chain_id,
            (NEW.block->'block'->'header'->'time')::TEXT::timestamp without time zone,
            (NEW.block->'block_id'->>'hash')::TEXT,
            (NEW.block->'block'->'header'->>'proposer_address')::TEXT
        )
        ON CONFLICT DO NOTHING;
        NEW.parsed_at := NOW();
        RETURN NEW;
    END
$$
LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER raw_block_insert
AFTER INSERT
ON raw_blocks
FOR EACH ROW EXECUTE PROCEDURE parse_raw_block();

CREATE OR REPLACE FUNCTION parse_raw_txs() RETURNS TRIGGER AS 
$$
    DECLARE
        tx JSONB;
        tx_responses JSONB;
        notify_channel TEXT;
    BEGIN            
        FOR tx_responses IN SELECT * FROM jsonb_array_elements(NEW.txs) where jsonb_array_length(NEW.txs) > 0
        LOOP
            INSERT INTO txs (txhash, chain_id, height, tx_response, tx, tx_response_tx_type, code, data, info, logs, events, raw_log, gas_used, gas_wanted, codespace, timestamp)
            VALUES (
                tx_responses->>'txhash',
                NEW.chain_id,
                NEW.height,
                tx_responses,
                tx_responses->'tx',
                tx_responses->'tx'->>'@type',
                tx_responses->>'code',
                tx_responses->>'data',
                tx_responses->>'info',
                tx_responses->'logs',
                tx_responses->'events',
                tx_responses->>'raw_log',
                (tx_responses->>'gas_used')::BIGINT,
                (tx_responses->>'gas_wanted')::BIGINT,
                tx_responses->>'codespace',
                (tx_responses->'timestamp')::TEXT::TIMESTAMP
            )
            ON CONFLICT DO NOTHING;
            
            
            PERFORM pg_notify('txs_to_logs', tx_responses->>'txhash' || ' ' || NEW.chain_id);
        END LOOP;
        NEW.parsed_at := NOW();
        RETURN NEW;
    END
$$ 
LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER raw_tx_insert
AFTER INSERT
ON raw_txs
FOR EACH ROW EXECUTE PROCEDURE parse_raw_txs();

