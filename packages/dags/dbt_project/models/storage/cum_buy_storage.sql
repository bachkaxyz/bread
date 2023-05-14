select
    timestamp,
    sum(transfer_amount) over (
        order by timestamp asc rows between unbounded preceding and current row
    ) as "transfer_amount",
    count(message_sender) over (
        order by timestamp asc rows between unbounded preceding and current row
    ) as "message_sender"
from {{ ref("buy_storage") }}
where transfer_denom = 'ujkl'
order by timestamp desc