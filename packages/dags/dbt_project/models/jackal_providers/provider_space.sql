select timestamp, json_object_agg(address, freespace)
from {{ source('jackal_providers', 'providers')}}
group by timestamp

