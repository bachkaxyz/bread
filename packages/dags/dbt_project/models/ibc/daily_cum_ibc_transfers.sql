select
    "day",
    transfer_denom,
    sum(total_amount_over_direction) over (
        order by "day" asc rows between unbounded preceding and current row
    ) as "cum_amount_over_direction"
from {{ ref("daily_ibc_transfers") }}
