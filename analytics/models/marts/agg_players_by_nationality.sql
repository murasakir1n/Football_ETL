select nationality, count(nationality) as nat_count
from players p
group by nationality
order by nat_count desc