select nationality, count(nationality) as nat_count
from {{ source('football_data', 'players') }}
group by nationality
order by nat_count desc
limit 10