select count(distinct address), timestamp
from {{ source("jackal_providers", "providers") }}
group by timestamp
