with tr as (
    Select id,
           sum(amount)                            as amount,
           int(substring(transaction_date, 1, 4)) as tr_year
    From transactions
    where amount >= 0
    Group BY id, tr_year
)
Select id,
       ifnull( y_2011 , 0) as y_2011 ,
       ifnull( y_2012 , 0) as y_2012 ,
       ifnull( y_2013 , 0) as y_2013 ,
       ifnull( y_2014 , 0) as y_2014 ,
       ifnull( y_2015 , 0) as y_2015 ,
       ifnull( y_2016 , 0) as y_2016 ,
       ifnull( y_2017 , 0) as y_2017 ,
       ifnull( y_2018 , 0) as y_2018 ,
       ifnull( y_2019 , 0) as y_2019 ,
       ifnull( y_2020 , 0) as y_2020 ,
       ifnull( y_2021 , 0) as y_2021
from tr Pivot (
    ifnull(  Round( Sum(amount), 2), 0 ) as total_amount
    for tr_year in ( 2011 as y_2011,
                     2012 as y_2012,
                     2013 as y_2013,
                     2014 as y_2014,
                     2015 as y_2015,
                     2016 as y_2016,
                     2017 as y_2017,
                     2018 as y_2018,
                     2019 as y_2019,
                     2020 as y_2020,
                     2021 as y_2021 )
)