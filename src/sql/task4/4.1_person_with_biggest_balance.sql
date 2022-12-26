with rich as (
    Select id,
           Round(SUM(amount), 2) as total_amount
    From transactions
    Group by id
    Order by total_amount desc
    LIMIT 1
)
select /*+ Broadcast(a,c) */
    a.first_name,
    a.last_name,
    c.country_full_name,
    r.total_amount
from rich r
         JOIN accounts a
              on a.id == r.id
         JOIN country_abbreviation c
              on a.country == c.abbreviation





