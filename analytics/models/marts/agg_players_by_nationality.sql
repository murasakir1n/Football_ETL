select nationality, count(nationality) as nat_count
from {{ ref('stg_players') }} p
group by nationality
order by nat_count desc
limit 10