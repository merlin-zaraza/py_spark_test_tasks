with total as (
    Select id,
           concat_ws(",", collect_list(account_type)) as account_types,
           Round(SUM(amount), 2) as total_amount
    From transactions
    Group by id
)
select /*+ Broadcast(a,c) */
    t.id,
    a.first_name,
    a.last_name,
    c.country_full_name,
    t.total_amount,
    t.account_types
from
     total t
JOIN accounts a
  on a.id == t.id
JOIN country_abbreviation c
  on a.country == c.abbreviation





