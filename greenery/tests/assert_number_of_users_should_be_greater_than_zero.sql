select
    value
from {{ref('user_count')}}
where value <= 0