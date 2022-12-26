Select id,
       Round(SUM(amount), 2) as total_amount
From transactions
where amount > 0
Group by id
Order by total_amount desc
LIMIT 10


