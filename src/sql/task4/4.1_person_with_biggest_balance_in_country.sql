with
t as (
    Select id,
           Round(SUM(amount), 2) as total_amount
    From
        transactions
    Group by
        id
) ,
rich as (
    SELECT /*+ Broadcast(a) */
        t.id,
        t.total_amount,
        a.first_name,
        a.last_name,
        a.country,
        row_number() over (partition by a.country order by total_amount desc)  as rn
    From
         t
    JOIN accounts a
        on a.id == t.id
)
select /*+ Broadcast(c) */
    r.first_name,
    r.last_name,
    r.total_amount,
    c.country_full_name
from
    rich r
JOIN country_abbreviation c
     on r.country == c.abbreviation
where
    r.rn == 1





