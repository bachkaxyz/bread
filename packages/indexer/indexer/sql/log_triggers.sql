create or replace function on_log_column_change()
returns trigger
as
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
                'ALTER TABLE $schema.logs ADD COLUMN IF NOT EXISTS %I JSONB GENERATED ALWAYS AS (parsed->%L) STORED',
                column_name,
                column_name
            );
        ELSE
            EXECUTE format(
                'ALTER TABLE $schema.logs DROP COLUMN IF EXISTS %I',
                column_name
            );
        END IF;
        RETURN NEW;
    END
$$
language plpgsql
;
CREATE OR REPLACE TRIGGER log_column_change
AFTER UPDATE
ON $schema.log_columns
FOR EACH ROW EXECUTE PROCEDURE on_log_column_change();
