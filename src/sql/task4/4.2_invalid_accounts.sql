with t as (
    select t.id,
           t.amount,
           t.account_type,
           t.transaction_date
    from transactions t
    where account_type = 'Professional'
),
 a as (
     select a.*
     from accounts a
     where a.age < 26
 )
select /*+ Broadcast(a) */
    a.id,
    a.first_name,
    a.last_name,
    a.age,
    a.country,
    t.id as account_id,
    t.amount,
    t.account_type,
    t.transaction_date
from t
JOIN a
     on a.id == t.id






