Select
    id,
    sum(amount) as balance,
    max(transaction_date) as latest_date
From
    transactions
Group by
    id