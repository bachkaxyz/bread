select
    "hour",
    transfer_denom,
    sum(total_amount_over_direction) over (
        order by "hour" asc rows between unbounded preceding and current row
    ) as "cum_amount_over_direction"
from {{ ref("hourly_ibc_transfers") }}
order by "hour" desc
