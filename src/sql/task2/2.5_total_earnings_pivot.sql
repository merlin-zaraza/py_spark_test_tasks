with tr as (
    Select id,
           sum(amount)                            as amount,
           int(substring(transaction_date, 1, 4)) as tr_year
    From transactions
    where amount >= 0
    Group BY id, tr_year
)
Select id,
       ifnull(`2011`, 0) as `2011`,
       ifnull(`2012`, 0) as `2012`,
       ifnull(`2013`, 0) as `2013`,
       ifnull(`2014`, 0) as `2014`,
       ifnull(`2015`, 0) as `2015`,
       ifnull(`2016`, 0) as `2016`,
       ifnull(`2017`, 0) as `2017`,
       ifnull(`2018`, 0) as `2018`,
       ifnull(`2019`, 0) as `2019`,
       ifnull(`2020`, 0) as `2020`,
       ifnull(`2021`, 0) as `2021`
from tr Pivot (
    ifnull(  Round( Sum(amount), 2), 0 ) as total_amount
    for tr_year in ( 2011,
                     2012,
                     2013,
                     2014,
                     2015,
                     2016,
                     2017,
                     2018,
                     2019,
                     2020,
                     2021 )
)