-- Part to whole analysis

-- WHICH CATEGORIES CONTRIBUTE THE MOST TO OVERALL SALES?
-- we can use group by or CTE, will be using CTE in this case

-- Basic/ main statement to be added to CTE
Select
category,
sum(sales_amount) total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category

-- We use CTE for this example
WITH category_sales AS (
Select
category,
sum(sales_amount) total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category)

Select
category,
total_sales,
sum(total_sales) over()overall_sales,-- don't need to partition anything because we want to high level data
Concat(Round((cast(total_sales as FLOAT)/ sum(total_sales) over ()) *100, 2), '%') as pct_of_total -- we cast to Float 
from category_sales
order by total_sales DESC