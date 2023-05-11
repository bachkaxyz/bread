select
    timestamp,
    json_object_agg(address, freespace) as freespace,
    json_object_agg(address, totalspace - freespace) as usedspace,
    json_object_agg(address, totalspace) as totalspace
from {{ source("jackal_providers", "providers") }}
group by timestamp
