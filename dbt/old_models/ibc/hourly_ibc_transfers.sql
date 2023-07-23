with
    daily_ibc_transfers as (
        select
            sum(amount_over_direction) as total_amount_over_direction,
            ibct.hour,
            transfer_denom
        from
            (
                select
                    amount_over_direction,
                    date_trunc('hour'::text, ibc_transfers.timestamp) as "hour",
                    transfer_denom
                from {{ ref("ibc_transfers") }}
            ) ibct
        group by ibct.hour, transfer_denom
    )
select *
from daily_ibc_transfers
order by hour desc
