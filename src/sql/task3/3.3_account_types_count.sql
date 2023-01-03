Select account_type,
       count( distinct id ) as cnt
From transactions
Group by account_type