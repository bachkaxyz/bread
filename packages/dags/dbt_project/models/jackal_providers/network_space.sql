select timestamp, sum(totalspace) as totalspace, sum(freespace) as freespace, sum(used_space) as usedspace
from {{ source('jackal_providers', 'providers')}}
group by timestamp