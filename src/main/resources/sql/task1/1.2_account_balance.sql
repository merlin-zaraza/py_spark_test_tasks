Select
    id,
    max(transaction_date) as latest_date,
    Round( sum(amount) , 2 ) as balance
From
    transactions
Group by
    id