select
    t.id,
    a.first_name,
    a.last_name,
    a.age,
    c.country_full_name,
    t.account_type,
    t.total_amount
from
     total t, a, c