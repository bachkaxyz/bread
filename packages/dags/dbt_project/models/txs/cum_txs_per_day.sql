{{ config(materialized='view') }}

with cum_txs_per_day as (
    select 
        "day", 
        sum(tx_count) over (order by "day" asc rows between unbounded preceding and current row) as "tx_count"
    from {{ ref('num_txs_per_day')}}
) 

select *
from cum_txs_per_day