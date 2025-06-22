-- Change over time
select
Year(order_date) as order_year,
Month(order_date) as order_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date), month(order_date);

--- group date by month and year hence date is always 1, can change the datetrunc to year
select
format(order_date, 'yyyy-MMM') as order_date,
--datetrunc(month,order_date) as order_date, >> update the group by and order by based on the returned field, wont sort correctly 
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by format(order_date, 'yyyy-MMM')
order by format(order_date, 'yyyy-MMM');

