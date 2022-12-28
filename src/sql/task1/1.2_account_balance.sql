Select
    id,
    max(transaction_date) as latest_date,
    sum(amount) as balance
From
    transactions
Group by
    id