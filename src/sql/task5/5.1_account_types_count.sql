Select account_type,
       count(id) as cnt
From transactions
Group by account_type