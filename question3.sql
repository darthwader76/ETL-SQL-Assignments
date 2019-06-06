select CONCAT(YEAR(payment_date), MONTH(payment_date)) year_month, sum(amount) total_revenue, count(*) total_transactions
from payment
group by MONTH(payment_date), YEAR(payment_date)
order by MONTH(payment_date), YEAR(payment_date)