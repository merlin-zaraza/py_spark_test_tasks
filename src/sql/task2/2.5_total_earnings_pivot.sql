with tr as (
    Select id,
           sum(amount)                            as amount,
           int(substring(transaction_date, 1, 4)) as tr_year
    From transactions
    where amount >= 0
    Group BY id, tr_year
)
Select *
from tr Pivot (
    ifnull(  Round( Sum(amount), 2), 0 ) as total_amount
    for tr_year in ( 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021 )
)