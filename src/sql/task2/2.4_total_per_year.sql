with t as (
    Select id,
           sum(case when amount < 0 then abs( amount) else 0 end) as total_expenses,
           sum(case when amount > 0 then amount       else 0 end) as total_earnings
    From transactions
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
    t.total_expenses,
    t.total_earnings
from t
Inner join a
    on a.id = t.id