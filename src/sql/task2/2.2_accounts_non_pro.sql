Select id,
       count(*) as cnt
From transactions
Where account_type != 'Professional'
Group by id