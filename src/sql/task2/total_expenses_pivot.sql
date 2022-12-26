with tr as (
    Select id,
           amount,
           case
               when amount <= 0 then
                   "expenses"
               else
                   "earnings"
               end type
    From transactions
)
Select id,
       ifnull(earnings, 0) as earnings,
       ifnull(expenses, 0) as expenses
from tr Pivot (
    Sum(amount) as amount
    for type in ( "earnings" as earnings , "expenses" as expenses )
)