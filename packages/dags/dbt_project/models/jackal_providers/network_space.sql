select
    timestamp,
    sum(totalspace) as totalspace,
    sum(freespace) as freespace,
    sum(totalspace - freespace) as usedspace
from {{ source("jackal_providers", "providers") }}
group by timestamp
order by timestamp desc
