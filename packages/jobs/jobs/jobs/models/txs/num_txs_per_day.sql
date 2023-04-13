{{ config(materialized='view') }}

with num_txs_per_day as (
    select count(1) as tx_count, date_trunc('day', timestamp) as "day"
    from {{ source('indexer', 'txs') }}

    group by "day"
    order by "day"
) 

select *
from num_txs_per_day