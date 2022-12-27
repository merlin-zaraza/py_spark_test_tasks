Select id,
       Round(AVG(amount),2) as avg_amount
From transactions
Where transaction_date like '2021%'
Group by id

