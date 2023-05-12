with
    type_attr as (
        select txhash, timestamp, e ->> 'type' as type, e -> 'attributes' as attributes
        from indexer.txs, jsonb_array_elements(events) as e
    ),
    t_attr as (
        select
            txhash,
            timestamp,
            type,
            convert_from(decode(attrs ->> 'key', 'base64'), 'UTF8') as key,
            convert_from(decode(attrs ->> 'value', 'base64'), 'UTF8') as value,
            attrs ->> 'index' = 'true' as index
        from type_attr, jsonb_array_elements(attributes) as attrs
    ),
    dup_events as (
        select txhash, type, key, array_agg(value) as
        values
        from t_attr
        group by txhash, type, key
    )
select txhash, json_object_agg(type || '_' || key, values)
from dup_events
group by txhash
