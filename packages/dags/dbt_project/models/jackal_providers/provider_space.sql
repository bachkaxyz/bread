select timestamp, json_object_agg(address, freespace) as freespace, json_object_agg(address, used_space) as usedspace, json_object_agg(address, totalspace) as totalspace
from {{ source('jackal_providers', 'providers')}}
group by timestamp

