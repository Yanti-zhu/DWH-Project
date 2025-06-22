-- Data Segmentation

/* Segment products into cost ranges and
count how many products fall into each segment */

-- creating a new dimension using CTE
WITH product_segments AS(
select
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
	 WHEN cost between 100 and 500 THEN '100-500'
	 WHEN cost between 500 and 1000 THEN '500-1000'
	 ELSE 'Above 1000'
END cost_range
from gold.dim_products)

SELECT
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by total_products DESC

/* Group customers into 3 segments based on their spending behaviour.
- VIP: at least 12 months of history and spending more than $5000
- Regular: at least 12 months of history but spending $5K or less
- New: lifespan less than 12 months
And find the total number of customers by each group */

-- This is the basic/ main statement, put it as a CTE (subquery is another option). this is used to build the final script
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) first_order,
max(order_date) last_order,
datediff(month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key

-- build from main/ basic script
-- This is to segment the customer, next is to find the total number of customers for each group
WITH customer_spending AS (
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) first_order,
max(order_date) last_order,
datediff(month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key) 

SELECT
customer_key,
total_spending,
lifespan,
CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
	 WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
	 ELSE 'New'
END customer_segment
From customer_spending

--  To find the total number of customer for each customer_segment, use CTE and subquery
WITH customer_spending AS (
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) first_order,
max(order_date) last_order,
datediff(month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key) 

SELECT
customer_segment,
COUNT(customer_key) as total_customers
FROM(
	SELECT -- this is now become the subquery
	customer_key,
	CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
		 WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
		 ELSE 'New'
	END customer_segment
	From customer_spending) t
Group by customer_segment
order by total_customers DESC