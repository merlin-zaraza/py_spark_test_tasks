Select account_type,
       count(*) as cnt
From transactions
Group by account_type