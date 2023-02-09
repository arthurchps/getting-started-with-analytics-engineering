select
  status,
  count(status)
from {{ source('greenery', 'orders') }}
group by status