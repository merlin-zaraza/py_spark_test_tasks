with t as (
    Select id,
           count(*) as cnt
    From transactions
    Where account_type != 'Professional'
    Group by id
), a as (
     Select id,
            first_name,
            last_name
     from accounts
)
select /*+ broadcast(a) */
    t.id,
    a.first_name,
    a.last_name,
    t.cnt
from t
Inner join a
    on a.id = t.id