CREATE OR REPLACE FUNCTION on_log_column_change() RETURNS TRIGGER AS
$$
    DECLARE
        column_name TEXT;
        unparsed_columns JSONB;
        kv_logs cursor(column_name TEXT) for 
            select key, value, txhash, msg_index
            from $schema.logs, jsonb_each_text(parsed)
            where key = column_name;
    BEGIN
        column_name := NEW.event || '_' || NEW.attribute;
        IF NEW.parse = TRUE THEN
            EXECUTE format(
                'ALTER TABLE $schema.logs ADD COLUMN IF NOT EXISTS %I JSONB DEFAULT NULL',
                column_name
            );
            FOR row IN kv_logs(column_name)
            LOOP
                EXECUTE format(
                    'UPDATE $schema.logs SET %I = %L, updated_at=NOW() WHERE txhash = %L AND msg_index = %L',
                    column_name,
                    row.value,
                    row.txhash,
                    row.msg_index
                );
            END LOOP;
            
        ELSE
            EXECUTE format(
                'ALTER TABLE $schema.logs DROP COLUMN IF EXISTS %I',
                column_name
            );
        END IF;
        RETURN NEW;
    END
$$
LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER log_column_change
AFTER UPDATE
ON $schema.log_columns
FOR EACH ROW EXECUTE PROCEDURE on_log_column_change();

CREATE OR REPLACE FUNCTION log_insert() RETURNS TRIGGER AS
$$
    DECLARE
        cur_log_columns cursor for 
            select event || '_' || attribute as column_name
            from $schema.log_columns
            where parse = TRUE;
    BEGIN
        FOR row IN cur_log_columns
        LOOP
            EXECUTE 'UPDATE $schema.logs SET ' || row.column_name ||' = parsed->' || quote_literal(row.column_name) || ', updated_at=NOW() WHERE txhash =' || quote_literal(NEW.txhash) || ' AND msg_index =' || quote_literal(NEW.msg_index::TEXt) || ';';
        END LOOP;
        UPDATE $schema.txs SET logs_parsed_at = NOW() WHERE txhash = NEW.txhash;
        RETURN NEW;
    END
$$
LANGUAGE plpgsql;
CREATE OR REPLACE TRIGGER log_insert
AFTER INSERT
on $schema.logs
FOR EACH ROW EXECUTE PROCEDURE log_insert();