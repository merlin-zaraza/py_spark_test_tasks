with t as (
    select id
    from transactions
    where account_type = 'Professional'
),
a as (
     select a.*
     from accounts a
     where a.age < 26
)
select /*+ Broadcast(a) */
    a.*
from
     t
JOIN a
  on a.id == t.id






