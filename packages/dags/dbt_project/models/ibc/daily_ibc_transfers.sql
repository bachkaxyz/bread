with
    daily_ibc_transfers as (
        select
            sum(amount_over_direction) as total_amount_over_direction,
            ibct.day,
            transfer_denom
        from
            (
                select
                    amount_over_direction,
                    date_trunc('day'::text, ibc_transfers.timestamp) as day,
                    transfer_denom
                from {{ ref("ibc_transfer") }}
            ) ibct
        group by ibct.day, transfer_denom
    )
select * 
from daily_ibc_transfers