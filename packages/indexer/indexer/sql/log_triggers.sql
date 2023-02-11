CREATE OR REPLACE FUNCTION on_log_column_change() RETURNS TRIGGER AS
$$
    DECLARE
        column_name TEXT;
        unparsed_columns JSONB;
        kv_logs cursor(column_name TEXT) for 
            select key, value, txhash, msg_index
            from logs, jsonb_each_text(parsed)
            where key = column_name;
    BEGIN
        column_name := NEW.event || '_' || NEW.attribute;
        IF NEW.parse = TRUE THEN
            EXECUTE format(
                'ALTER TABLE logs ADD COLUMN IF NOT EXISTS %I TEXT',
                column_name
            );
            -- Look for the key in the parsed data
            -- insert the value into the new column
            FOR row IN kv_logs(column_name)
            LOOP
                EXECUTE format(
                    'UPDATE logs SET %I = %L WHERE txhash = %L AND msg_index = %L',
                    column_name,
                    row.value,
                    row.txhash,
                    row.msg_index
                );
            END LOOP;
            
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