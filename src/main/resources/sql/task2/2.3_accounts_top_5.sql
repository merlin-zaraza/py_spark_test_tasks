Select first_name,
       count(*) as cnt
from accounts
Group by first_name
Order by cnt desc
limit 5