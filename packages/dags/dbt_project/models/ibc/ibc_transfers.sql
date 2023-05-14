with
    combined_transfers as (
        select *, (transfer_amount) as amount_over_direction
        from {{ ref("ibc_transfers_in") }}
        union
        select *, (transfer_amount::numeric * -1) as amount_over_direction
        from {{ ref("ibc_transfers_out") }}
    )
select *
from combined_transfers