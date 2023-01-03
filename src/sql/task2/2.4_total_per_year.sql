with t as (
    Select id,
           Round( sum(case when amount < 0 then abs( amount) else 0 end), 2) as expenses,
           Round( sum(case when amount > 0 then amount       else 0 end), 2) as earnings
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
    t.expenses ,
    t.earnings
from t
Inner join a
    on a.id = t.id