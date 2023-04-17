{{ config(materialized="view") }}

select sum(gas_used), date_trunc('day', timestamp) as "day"
from {{ source("indexer", "txs") }}

group by "day"
order by "day"
