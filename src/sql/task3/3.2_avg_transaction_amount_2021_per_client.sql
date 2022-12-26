Select id,
       Round(AVG(amount),2) as avg_amount
From transactions
Group by id

