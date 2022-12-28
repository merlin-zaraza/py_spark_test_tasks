with total as (
    Select id,
           account_type,
           Round(SUM(amount), 2) as total_amount
    From transactions
    Group by id, account_type
)
select /*+ Broadcast(a,c) */
    t.id,
    a.first_name,
    a.last_name,
    a.age,
    c.country_full_name,
    t.account_type,
    t.total_amount
from
     total t
JOIN accounts a
  on a.id == t.id
JOIN country_abbreviation c
  on a.country == c.abbreviation





