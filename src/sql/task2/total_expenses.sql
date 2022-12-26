Select id,
       sum(case when amount < 0 then amount else 0 end) as expenses,
       sum(case when amount > 0 then amount else 0 end) as earnings
From transactions
Group by id