CREATE OR REPLACE FUNCTION parse_raw() RETURNS TRIGGER AS 
$$
    DECLARE
        tx JSONB;
        tx_responses JSONB;
        notify_channel TEXT;
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
        
                
        FOR tx_responses IN SELECT * FROM jsonb_array_elements(NEW.txs->'tx_responses')
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
            
            
            PERFORM pg_notify('txs_to_messages_logs', tx_responses->>'txhash' || ' ' || NEW.chain_id);
        END LOOP;
        NEW.blocks_txs_parsed_at := NOW();
        RETURN NEW;
    END
$$ 
LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER raw_insert
BEFORE INSERT
ON raw
FOR EACH ROW EXECUTE PROCEDURE parse_raw();

CREATE OR REPLACE FUNCTION on_log_column_change() RETURNS TRIGGER AS
$$
    DECLARE
        column_name TEXT;
        unparsed_columns JSONB;
    BEGIN
        column_name := NEW.event || NEW.attribute;
        IF NEW.parse = TRUE THEN
            EXECUTE format(
                'ALTER TABLE logs ADD COLUMN IF NOT EXISTS %I TEXT',
                column_name
            );
            -- Look for the key in the parsed data
            -- insert the value into the new column
            
        ELSE
            EXECUTE format(
                'ALTER TABLE logs DROP COLUMN IF EXISTS %I',
                column_name
            );
        END IF;
        RETURN NEW;
    END
$$
LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER log_column_change
AFTER UPDATE
ON log_columns
FOR EACH ROW EXECUTE PROCEDURE on_log_column_change();