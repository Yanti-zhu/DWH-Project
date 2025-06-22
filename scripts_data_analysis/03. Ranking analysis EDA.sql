-- Ranking Analysis (Top and Bottom performer)

-- which 5 products generate the highest revenue
select top 5
p.product_name,
sum(s.sales_amount) total_revenue
from gold.fact_sales s
left join gold.dim_products p
on p.product_key = s.product_key
group by p.product_name
order by total_revenue DESC

-- with rank function, and subquery
select * 
from(
	select
	p.product_name,
	sum(s.sales_amount) total_revenue,
	row_number() over(order by sum(s.sales_amount) DESC) AS rank_products
	from gold.fact_sales s
	left join gold.dim_products p
	on p.product_key = s.product_key
	group by p.product_name)t
Where rank_products <= 5

-- what are the 5 worst-performing products in terms of sales
select top 5
p.product_name,
sum(s.sales_amount) total_revenue
from gold.fact_sales s
left join gold.dim_products p
on p.product_key = s.product_key
group by p.product_name
order by total_revenue ASC

-- Find the top 10 customers who have generated the highest revenue
select top 10
c.customer_key,
c.first_name,
c.last_name,
sum(s.sales_amount) as total_revenue
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
group by 
c.customer_key,
c.first_name,
c.last_name
Order by total_revenue DESC

-- the 3 customers with the lowest orders placed
select top 3
c.customer_key,
c.first_name,
c.last_name,
count(distinct order_number) as total_orders
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
group by 
c.customer_key,
c.first_name,
c.last_name
Order by total_orders ASC
